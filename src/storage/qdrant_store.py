import gc
from typing import List, Dict, Any, Optional, Tuple, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
from tqdm import tqdm
from qdrant_client import QdrantClient
from qdrant_client.http.models import (
    Distance, VectorParams, Filter, FieldCondition, KeywordIndexParams,
    MatchValue, PayloadSchemaType, PointStruct, SearchParams
)

from .core import BaseVectorStore, VectorSearchResult
from .model_manager import embedding_model_manager
from src.utils.logger import get_logger


class QdrantVectorStore(BaseVectorStore):
    """
    Qdrant-бэкенд с поддержкой:
    - Prompt-based encoding (Harrier, E5)
    - Scalar quantization для экономии памяти
    - Payload indexing для быстрой фильтрации
    - Batch processing для эффективности
    """
    
    def __init__(
        self,
        collection_name: str,
        embedding_model: str = "BAAI/bge-small-en-v1.5",
        device: str = "cpu",
        quantization: bool = False,
        dtype: str = "auto",
        path: str = None,
        location: str = None,
        log_path: str = None
    ):
        self.path = path
        self.location = location
        self._collection_name = collection_name
        self.model_name = embedding_model
        self.device = device
        self.quantization = quantization
        self.dtype = dtype
        
        # Параметры из конфига модели
        model_cfg = embedding_model_manager.MODEL_CONFIGS[embedding_model]
        self.vector_size = model_cfg["dim"]
        self.query_prompt_name = embedding_model_manager.get_query_prompt(embedding_model)
        
        self._client: Optional[QdrantClient] = None
        self._is_index_built = False
        
        # complete_log_path = log_path if log_path.endswith('.log') else os.path.join(log_path, "qdrant_vector_store.log")
        self.logger = get_logger(
            "qdrant_vector_store", 
            # complete_log_path, 
            # mode='w' if not os.path.exists(complete_log_path) else 'a'
            file=False
        ) if log_path else None
        if self.logger:
            self.logger.info(
                f"QdrantVectorStore initialized: model={embedding_model}, "
                f"dim={self.vector_size}, path={path}, location={location}, device={device}"
            )
            # complete_log_path = log_path if log_path.endswith('.log') else os.path.join(log_path, "embedding_model.log")
            embedding_model_manager.logger = get_logger(
                "emdedding_model",
                # complete_log_path, 
                # mode='w' if not os.path.exists(complete_log_path) else 'a'
                file=False
            )
    
    @property
    def collection_name(self) -> str:
        return self._collection_name
    
    @property
    def client(self) -> QdrantClient:
        if self._client is None:
            if self.logger: self.logger.info(f"Connecting to Qdrant at {self.path}")
            self._client = QdrantClient(path=self.path)
        return self._client
    
    @property
    def is_loaded(self) -> bool:
        return self._client is not None and self._is_index_built
    
    def _encode_texts(
        self,
        texts: List[str],
        is_query: bool = False,
        batch_size: int = 64
    ) -> np.ndarray:
        """Кодирует тексты с учётом prompt_name."""
        prompt_name = self.query_prompt_name if is_query else None
        return embedding_model_manager.encode(
            model_name=self.model_name,
            texts=texts,
            device=self.device,
            dtype=self.dtype,
            prompt_name=prompt_name,
            normalize=True,
            batch_size=batch_size,
            is_query=is_query
        )
    
    def _get_collection_config(self) -> Dict[str, Any]:
        config = {
            "vectors_config": VectorParams(
                size=self.vector_size,
                distance=Distance.COSINE, 
                hnsw_config={
                    "m": 16,
                    "ef_construct": 200,
                    "full_scan_threshold": 10000
                }
            ),
            "optimizers_config": {
                "default_segment_number": 2,
                "memmap_threshold": 10000
            }
        }
        
        if self.quantization:
            config["quantization_config"] = {
                "scalar": {
                    "type": "int8",
                    "quantile": 0.99,
                    "always_ram": True
                }
            }
        
        return config
    
    def build_index(
        self,
        documents: List[Dict[str, Any]],
        collection_name: Optional[str] = None,
        batch_size: int = 256,
        max_workers: int = 2,
        force_rebuild: bool = False
    ) -> None:
        """
        Создает векторный индекс из списка документов с параллельной обработкой батчей.
        
        Args:
            documents: Список dict с ключами 'text' и 'metadata'.
            collection_name: Имя коллекции (по умолчанию self._collection_name).
            batch_size: Размер одного батча для кодирования и upsert.
            max_workers: Количество потоков. Рекомендуется 2-4 для CPU, 1 для CUDA 
                         (из-за внутренней многопоточности PyTorch).
            force_rebuild: Если True, удаляет коллекцию и создаёт её заново,
        """
        if not documents:
            if self.logger: self.logger.warning("Empty document list provided. Skipping index build.")
            return

        coll_name = collection_name or self._collection_name
        if self.logger: self.logger.info(f"Building index '{coll_name}' with {len(documents)} documents...")
        
        if force_rebuild and self.client.collection_exists(coll_name):
            self.client.delete_collection(coll_name)
            if self.logger: self.logger.info(f"Dropped existing collection '{coll_name}' for rebuild")

        if not self.client.collection_exists(coll_name):
            if self.logger: self.logger.info(f"Creating collection '{coll_name}'")
            self.client.create_collection(
                collection_name=coll_name,
                **self._get_collection_config()
            )
            # Индекс для фильтрации по db_id
            self.client.create_payload_index(
                collection_name=coll_name,
                field_name="db_id",
                field_schema=KeywordIndexParams(
                    type=PayloadSchemaType.KEYWORD,
                    is_tenant=True 
                )
            )

        embedding_model_manager.get_model(self.model_name)
        _model_name = list(embedding_model_manager._locks.keys())[0]
        _lock = embedding_model_manager._locks[_model_name]
        embedding_model_manager._locks[_model_name] = None

        batches = [documents[i:i + batch_size] for i in range(0, len(documents), batch_size)]
        completed_batches = 0
        
        def _process_batch(batch_data: Tuple[int, List[Dict]]) -> Tuple[int, List[PointStruct]]:
            start_idx, batch_docs = batch_data
            texts = [doc["text"] for doc in batch_docs]
            
            vectors = embedding_model_manager.encode(
                model_name=self.model_name,
                texts=texts,
                device=self.device,
                dtype=self.dtype,
                prompt_name=None,
                normalize=True,
                batch_size=len(texts)
            )

            points = []
            for i, (doc, vec) in enumerate(zip(batch_docs, vectors)):
                points.append(PointStruct(
                    id=doc.get("id", start_idx + i),
                    vector=vec.tolist(),
                    payload={"text": doc["text"], **doc["metadata"]}
                ))
            return start_idx, points

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Отправляем батчи на кодирование параллельно, на случай отключения блокировки потоков
            futures = {
                executor.submit(_process_batch, (i * batch_size, batch)): i 
                for i, batch in enumerate(batches)
            }

            # По мере завершения потока сразу отправляем в Qdrant
            with tqdm(total=len(list(futures.keys())), desc="Adding batches", ncols=160, leave=True) as pbar:
                for future in as_completed(futures):
                    batch_idx = futures[future]
                    try:
                        _, points = future.result()
                        self.client.upsert(collection_name=coll_name, points=points)
                        completed_batches += 1
                        pbar.update(1)
                    except Exception as e:
                        if self.logger: self.logger.error(f"Failed to upsert batch {batch_idx}: {e}")
                        executor.shutdown(wait=False)
                        raise
        
        embedding_model_manager._locks[_model_name] = _lock
        self._is_index_built = True
        if self.logger: self.logger.info(f"Index '{coll_name}' built successfully.")
    
    def search(
        self,
        queries: List[str],
        top_k: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 32
    ) -> Dict[str, List[VectorSearchResult]]:
        """Поиск. Запросы кодируются С query-промптом (если задан)."""
        if not self.is_loaded:
            raise RuntimeError("Index not loaded. Call build_index() first.")
        
        results: Union[Dict[str, List[VectorSearchResult]], List[VectorSearchResult]] = {}
        
        for i in range(0, len(queries), batch_size):
            batch_queries = queries[i:i + batch_size]
            vectors = self._encode_texts(batch_queries, is_query=True)

            # Формируем фильтр Qdrant
            qdrant_filter = None
            if filters:
                must_conditions = [
                    FieldCondition(key=key, match=MatchValue(value=value))
                    for key, value in filters.items()
                ]
                if must_conditions:
                    qdrant_filter = Filter(must=must_conditions)

            for query_text, vector in zip(batch_queries, vectors):
                hits = self.client.query_points(
                    collection_name=self._collection_name,
                    query=vector.tolist(),
                    query_filter=qdrant_filter,
                    limit=top_k,
                    with_payload=True,
                    search_params=SearchParams(hnsw_ef=128)
                ).points
                results[query_text] = [
                    VectorSearchResult(
                        id=hit.id,
                        text=hit.payload.get("text", ""),
                        metadata={k: v for k, v in hit.payload.items() if k not in ["text", "id"]},
                        score=hit.score,
                        rank=idx
                    )
                    for idx, hit in enumerate(hits)
                ]
        
        return results

    def get_indexed_columns(self, collection_name: str, target_db_id: str, id_only=False) -> set:
        """Возвращает множество уникальных колонок (table.column), уже проиндексированных для db_id."""
        indexed = set()
        limit = 5000
        
        # Фильтр только по нужной БД
        db_filter = Filter(must=[FieldCondition(key="db_id", match=MatchValue(value=target_db_id))])
        next_offset = 0

        while True:
            points, next_offset = self.client.scroll(
                collection_name=collection_name,
                scroll_filter=db_filter,
                limit=limit,
                offset=next_offset,
                with_payload=["table_name", "column_name"] if not id_only else False,
                with_vectors=False
            )
            
            if id_only:
                for p in points:
                    indexed.add(p.id)
            else:
                for p in points:
                    payload = p.payload if hasattr(p, 'payload') else (p[2] if isinstance(p, tuple) else {})
                    if payload:
                        indexed.add((p.id, payload.get('table_name'), payload.get('columns_name')))
        
            if next_offset is None:
                break
                
        return indexed
    
    def close(self) -> None:
        """Закрывает клиент Qdrant (модель управляется глобальным менеджером)."""
        if self._client:
            self._client = None
            gc.collect()
            if self.logger: self.logger.info(f"Qdrant client closed for collection '{self._collection_name}'")
