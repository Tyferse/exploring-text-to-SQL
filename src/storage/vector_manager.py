import gc
import json
import os
import pickle
import threading
from typing import Dict, List, Optional, Any
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed

from .core import BaseVectorStore, VectorSearchResult
from .qdrant_store import QdrantVectorStore
from src.utils.logger import get_logger, attach_shared_file_handler
from src.utils.preprocessing import get_column_hash


class VectorStoreManager:
    """
    Управляет пулом сессий векторных хранилищ.
    
    Особенности:
    - LRU-кэш сессий для экономии памяти
    - Автоматическое создание индексов из результатов предобработки
    - Групповой поиск по db_id
    - Потокобезопасные операции
    """
    
    def __init__(
        self,
        storage_root: str = "storage",
        location: str = None,
        max_cached_sessions: int = 3,
        embedding_model: str = "BAAI/bge-small-en-v1.5",
        backend: str = "qdrant",
        device: str = "cpu",
        quantization: bool = True,
        dtype: str = "auto",
        log_path: str = None
    ):
        self.storage_root = storage_root
        self.location = location
        self.max_cached_sessions = max_cached_sessions
        self.embedding_model = embedding_model
        self.backend = backend
        self.device = device
        self.quantization = quantization
        self.dtype = dtype
        self.log_path = log_path
        self._is_log_file_set = False
        
        # LRU-кэш: {context_id: VectorStore}
        self._session_cache: OrderedDict[str, BaseVectorStore] = OrderedDict()
        self._cache_lock = threading.Lock() if backend == "qdrant" else None
        self._coll_name = "schema_columns"

        os.makedirs(self.log_path, exist_ok=True)
        if not os.path.exists(os.path.join(self.log_path, "vector_store.log")):
            open(os.path.join(self.log_path, "vector_store.log"), 'w', encoding='utf-8').close()

        # attach_shared_file_handler(
        #     log_file=os.path.join(self.log_path, "vector_store.log"),
        #     logger_names=["vector_manager", "qdrant_vector_store", "embedding_model_manager"],
        #     level="INFO",
        #     mode='a'
        # )

        self.logger = get_logger(
            "vector_manager", 
            # os.path.join(self.log_path, "vector_store.log"), 
            # mode='w'
            file=False
        ) if self.log_path else None
        if self.logger:
            self.logger.info(
                f"VectorStoreManager initialized: backend={backend}, "
                f"model={embedding_model}, max_sessions={max_cached_sessions}, device={device}"
            )
    
    def _get_context_path(self, context_id: str) -> str:
        """Возвращает путь к хранилищу для контекста."""
        return os.path.join(self.storage_root, context_id, "column_vdb")
    
    def _create_session(self, context_id: str) -> BaseVectorStore:
        """Создаёт новую сессию для контекста."""
        if self.backend == "qdrant":
            session = QdrantVectorStore(
                location=self.location,
                path=self._get_context_path(context_id) if self.location is None else None,
                collection_name=self._coll_name,
                embedding_model=self.embedding_model,
                device=self.device,
                quantization=self.quantization,
                dtype=self.dtype,
                log_path=self.log_path,
            )
        else:
            raise ValueError(f"Unsupported backend: {self.backend}")
        
        if not self._is_log_file_set:
            attach_shared_file_handler(
                log_file=os.path.join(self.log_path, "vector_store.log"),
                logger_names=["vector_manager", "qdrant_vector_store", "embedding_model_manager"],
                level="INFO",
                mode='a'
            )
            self._is_log_file_set = True

        return session
    
    def _get_or_create_session(self, context_id: str) -> BaseVectorStore:
        """Получает или создаёт сессию."""
        if self._cache_lock:
            with self._cache_lock:
                return self._get_or_create_session_unlocked(context_id)
            
        return self._get_or_create_session_unlocked(context_id)
    
    def _get_or_create_session_unlocked(self, context_id: str) -> BaseVectorStore:
        """Внутренний метод без блокировки."""
        # Если уже в кэше — перемещаем в конец (LRU)
        if context_id in self._session_cache:
            self._session_cache.move_to_end(context_id)
            return self._session_cache[context_id]
        
        # Если кэш полон — вытесняем наименее используемую
        if len(self._session_cache) >= self.max_cached_sessions:
            oldest_id, oldest_session = self._session_cache.popitem(last=False)
            oldest_session.close()
            if self.logger: self.logger.info(f"Evicted session for context '{oldest_id}' from cache")
        
        # Создаём новую сессию
        session = self._create_session(context_id)
        self._session_cache[context_id] = session
        if self.logger: self.logger.info(f"Created new session for context '{context_id}'")
        return session
    
    def build_from_preprocessing_results(
        self,
        preprocessing_results: Dict[str, str],
        context_id: Optional[str] = None,
        batch_size: int = 256,
        max_workers: int = 2,
        force_rebuild: bool = False
    ) -> None:
        """
        Создаёт векторные индексы из результатов предобработки.
        
        Args:
            preprocessing_results: Dict {db_id: path_to_meta.json}
            context_id: Уникальный ID для изоляции индексов
            max_workers: число процессов для проверки на существующие данные
            force_rebuild: Если True, пересоздаёт хранилище заново вне зависимости от существующих данных
        """
        if not context_id:
            context_id = list(preprocessing_results.keys())[0].rsplit("_", 1)[0]
        
        if self.logger: self.logger.info(f"Building vector indexes for context '{context_id}' with {batch_size} batch size...")
        
        # Собираем все документы
        all_documents: List[Dict] = []
        
        for db_id, meta_path in preprocessing_results.items():
            docs_path = meta_path[:meta_path.rfind('_meta')] + '_docs.json'
            
            if not os.path.exists(docs_path):
                if self.logger: self.logger.warning(f"Docs file not found: {docs_path}")
                continue
            
            with open(docs_path, 'r', encoding='utf-8') as f:
                docs_data = json.load(f)
            
            # docs_data может быть списком документов или dict с ключом "documents"
            documents = docs_data if isinstance(docs_data, list) else docs_data.get("documents", [])
            for doc in documents:
                if 'id' not in doc:
                    meta = doc['metadata']
                    doc['id'] = get_column_hash(meta)

                all_documents.append(doc)
        
        if not all_documents:
            if self.logger: self.logger.warning(f"No documents found for indexing '{db_id}'")
            return
        
        # Создаём индекс через сессию
        session = self._get_or_create_session(context_id)

        existing_ids = []
        cached_id_path = os.path.join(self.storage_root, context_id, "added_ids.pkl")
        if session.client.collection_exists(self._coll_name) and session.client.count(collection_name=self._coll_name).count > 0:
            if not force_rebuild:
                if self.logger: self.logger.info("Index exists. Checking data.")

                # Загружаем существующие id из кэша
                if os.path.exists(cached_id_path):
                    with open(cached_id_path, 'rb') as f:
                        existing_ids = pickle.load(f)
                else:
                    with ThreadPoolExecutor(max_workers) as executor:                    
                        futures = {
                            executor.submit(session.get_indexed_columns, self._coll_name, db_id, True): db_id
                            for db_id in preprocessing_results.keys()
                        }

                        for future in as_completed(futures):
                            try:
                                db_id = futures[future]
                                existing_ids.extend(future.result())
                            except Exception as e:
                                if self.logger: self.logger.error(f"Failed to find items for {db_id}: {e}")
                                executor.shutdown(wait=False)
                                raise

                    with open(cached_id_path, 'wb') as f:
                        pickle.dump(existing_ids, f)

                all_documents = [doc for doc in all_documents if doc['id'] not in existing_ids]
                if self.logger: self.logger.info(f"Found {len(existing_ids)} items, adding {len(all_documents)}")

            if all_documents:
                added_ids = session.build_index(all_documents, collection_name=self._coll_name, batch_size=batch_size, max_workers=max_workers, cache_path=cached_id_path, force_rebuild=force_rebuild)
                if added_ids is not None:
                    with open(cached_id_path, 'wb') as f:
                        pickle.dump(existing_ids + added_ids, f)
                    
                    raise
            else:
                session._is_index_built = True
        else:
            added_ids = session.build_index(all_documents, collection_name=self._coll_name, batch_size=batch_size, max_workers=max_workers, force_rebuild=True)
            if added_ids is not None:
                with open(cached_id_path, 'wb') as f:
                    pickle.dump(existing_ids + added_ids, f)
                
                raise
        
        if self.logger: self.logger.info(f"Built index with {len(all_documents)} column documents for context '{context_id}'")
    
    def search_batch(
        self,
        context_id: str,
        queries_by_db: Dict[str, List[str]],
        top_k: int = 10,
        batch_size: int = 32,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Dict[str, List[VectorSearchResult]]]:
        """
        Выполняет поиск для групп вопросов, сгруппированных по db_id.
        
        Args:
            queries_by_db: {db_id: [question1, question2, ...]}
            top_k: Число результатов на запрос
            batch_size: Размер батча для эмбеддингов
            filters: Дополнительные фильтры (объединяются с db_id)
            
        Returns:
            {db_id: {query: [results]}}
        """
        results: Dict[str, Dict[str, List[VectorSearchResult]]] = {}
        
        for db_id, queries in queries_by_db.items():
            if not queries:
                continue
            
            # Получаем сессию
            session = self._get_or_create_session(context_id)
            
            # Объединяем фильтры: обязательный db_id + дополнительные
            search_filters = {"db_id": db_id}
            if filters:
                search_filters.update(filters)
            
            search_results = session.search(
                queries=queries,
                top_k=top_k,
                filters=search_filters,
                batch_size=batch_size
            )
            
            results[db_id] = search_results
            if self.logger: self.logger.info(f"Searched {len(queries)} queries for db '{db_id}'")
        
        return results
    
    def close_all(self) -> None:
        """Закрывает все сессии и освобождает память."""
        if self.logger: self.logger.info(f"Closing {len(self._session_cache)} vector store sessions...")
        for session in self._session_cache.values():
            session.close()
        self._session_cache.clear()
        gc.collect()
        if self.logger: self.logger.info("All vector store sessions closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_all()
        return False
