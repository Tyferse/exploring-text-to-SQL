import logging
from typing import List, Dict, Any, Optional

from qdrant_client import QdrantClient
from qdrant_client.http.models import (
    Distance, VectorParams, Filter, FieldCondition, 
    MatchValue, PointStruct, SearchParams
)
from sentence_transformers import SentenceTransformer

from src.storage.core import BaseVectorStore, VectorSearchResult


logger = logging.getLogger("vector_store")

class QdrantVectorStore(BaseVectorStore):
    """
    Реализация BaseVectorStore для Qdrant в local/embedded режиме.
    Оптимизирована для работы с ограниченной памятью (8 Гб ОЗУ).
    """
    
    def __init__(
        self,
        location: str,
        collection_name: str,
        embedding_model: str = "BAAI/bge-small-en-v1.5",
        vector_size: int = 384,
        device: str = "cpu",
        quantization: bool = True
    ):
        self.location = location
        self.collection_name = collection_name
        self.vector_size = vector_size
        self.device = device
        self.quantization = quantization
        
        # Инициализация модели эмбеддингов (ленивая загрузка)
        self._model_name = embedding_model
        self._embedding_model: Optional[SentenceTransformer] = None
        self._client: Optional[QdrantClient] = None
        self._is_index_built = False
        
        logger.info(f"Initialized QdrantVectorStore: location={location}, collection={collection_name}")
    
    @property
    def embedding_model(self) -> SentenceTransformer:
        """Ленивая загрузка модели эмбеддингов."""
        if self._embedding_model is None:
            logger.info(f"Loading embedding model: {self._model_name}")
            self._embedding_model = SentenceTransformer(
                self._model_name,
                device=self.device
            )
        return self._embedding_model
    
    @property
    def client(self) -> QdrantClient:
        """Ленивая инициализация клиента Qdrant."""
        if self._client is None:
            logger.info(f"Connecting to Qdrant at {self.location}")
            self._client = QdrantClient(location=self.location)
        return self._client
    
    @property
    def is_loaded(self) -> bool:
        return self._client is not None and self._is_index_built
    
    def _get_collection_config(self) -> Dict[str, Any]:
        """Конфигурация коллекции с оптимизацией под память."""
        config = {
            "vectors": VectorParams(
                size=self.vector_size,
                distance=Distance.COSINE,
                hnsw_config={
                    "m": 16,
                    "ef_construct": 200,
                    "full_scan_threshold": 10000
                }
            ),
            "optimizers_config": {
                "default_segment_number": 2,  # Меньше сегментов = меньше фрагментации памяти
                "memmap_threshold": 10000     # Включаем mmap для больших индексов
            }
        }
        
        # Квантование для экономии памяти (в 4 раза)
        if self.quantization:
            config["quantization_config"] = {
                "scalar": {
                    "type": "int8",
                    "quantile": 0.99,
                    "always_ram": True  # Держим квантованный индекс в RAM для скорости
                }
            }
        
        return config
    
    def build_index(self, documents: List[Dict[str, Any]], collection_name: Optional[str] = None) -> None:
        """
        Создает индекс из документов. Поддерживает батчинг для экономии памяти.
        
        Args:
            documents: Список {'text': str, 'metadata': dict}
            collection_name: Переопределение имени коллекции (опционально)
        """
        coll_name = collection_name or self.collection_name
        logger.info(f"Building index '{coll_name}' with {len(documents)} documents...")
        
        # Создаем коллекцию если не существует
        if not self.client.collection_exists(coll_name):
            self.client.create_collection(
                collection_name=coll_name,
                **self._get_collection_config()
            )
            # Создаем индекс для фильтрации по db_id (критично для schema linking)
            self.client.create_payload_index(
                collection_name=coll_name,
                field_name="db_id",
                field_schema="keyword"
            )
        
        # Батчинг для экономии памяти (не грузим все векторы сразу)
        batch_size = 256
        points = []
        
        for i, doc in enumerate(documents):
            # Генерируем эмбеддинг
            vector = self.embedding_model.encode(
                doc["text"],
                show_progress_bar=False
            ).tolist()
            
            # Создаем точку для Qdrant
            point = PointStruct(
                id=i,  # Простой числовой ID
                vector=vector,
                payload={
                    "text": doc["text"],
                    **doc["metadata"]  # db_id, table_name, column_name, etc.
                }
            )
            points.append(point)
            
            # Отправляем батч
            if len(points) >= batch_size:
                self.client.upsert(collection_name=coll_name, points=points)
                points = []
                logger.info(f"Indexed {i+1}/{len(documents)} documents...")
        
        # Отправляем остаток
        if points:
            self.client.upsert(collection_name=coll_name, points=points)
        
        self._is_index_built = True
        logger.info(f"Index '{coll_name}' built successfully.")
    
    def search(
        self,
        queries: List[str],
        top_k: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 32
    ) -> Dict[str, List[VectorSearchResult]]:
        """
        Поиск с поддержкой батчинга и фильтрации.
        """
        if not self.is_loaded:
            raise RuntimeError("Index not loaded. Call build_index() first.")
        
        results = {}
        
        # Батчинг запросов для эффективного использования GPU/CPU
        for i in range(0, len(queries), batch_size):
            batch_queries = queries[i:i+batch_size]
            
            # Генерируем эмбеддинги для батча
            vectors = self.embedding_model.encode(
                batch_queries,
                show_progress_bar=False,
                batch_size=batch_size
            )
            
            # Формируем фильтр Qdrant
            qdrant_filter = None
            if filters:
                must_conditions = []
                for key, value in filters.items():
                    must_conditions.append(
                        FieldCondition(
                            key=key,
                            match=MatchValue(value=value)
                        )
                    )
                if must_conditions:
                    qdrant_filter = Filter(must=must_conditions)
            
            # Поиск для каждого запроса в батче
            for query_text, vector in zip(batch_queries, vectors):
                hits = self.client.search(
                    collection_name=self.collection_name,
                    query_vector=vector,
                    query_filter=qdrant_filter,
                    limit=top_k,
                    with_payload=True,
                    params=SearchParams(hnsw_ef=128)  # Баланс скорость/точность
                )
                
                results[query_text] = [
                    VectorSearchResult(
                        text=hit.payload.get("text", ""),
                        metadata={k: v for k, v in hit.payload.items() if k != "text"},
                        score=hit.score,
                        rank=idx
                    )
                    for idx, hit in enumerate(hits)
                ]
        
        return results
    
    def close(self) -> None:
        """Закрывает соединения и освобождает память."""
        if self._client:
            self._client = None
        if self._embedding_model:
            # SentenceTransformer не имеет явного close(), но можно удалить ссылку
            self._embedding_model = None
        logger.info("QdrantVectorStore resources released.")