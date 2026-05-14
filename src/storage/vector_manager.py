import os
import json
import logging
from typing import List, Dict, Any, Optional
from collections import OrderedDict

from src.storage.core import BaseVectorStore, VectorSearchResult
from qdrant_store import QdrantVectorStore

logger = logging.getLogger("vector_manager")

class VectorStoreManager:
    """
    Управляет пулом сессий векторных хранилищ с LRU-эвикцией.
    Оптимизирован для работы с ограниченной памятью.
    """
    
    def __init__(
        self,
        storage_root: str = "storage",
        max_cached_sessions: int = 3,
        embedding_model: str = "BAAI/bge-small-en-v1.5",
        backend: str = "qdrant"
    ):
        self.storage_root = storage_root
        self.max_cached_sessions = max_cached_sessions
        self.embedding_model = embedding_model
        self.backend = backend
        
        # LRU-кэш сессий: {context_id: VectorStore}
        self._session_cache: OrderedDict[str, BaseVectorStore] = OrderedDict()
        
        logger.info(f"Initialized VectorStoreManager: max_sessions={max_cached_sessions}")
    
    def _get_context_path(self, context_id: str) -> str:
        """Возвращает путь к хранилищу для данного контекста."""
        return os.path.join(self.storage_root, "vector_db", context_id)
    
    def _get_or_create_session(self, context_id: str) -> BaseVectorStore:
        """
        Получает или создает сессию для контекста с LRU-эвикцией.
        """
        # Если уже в кэше — перемещаем в конец (LRU)
        if context_id in self._session_cache:
            self._session_cache.move_to_end(context_id)
            return self._session_cache[context_id]
        
        # Если кэш полон — вытесняем наименее используемую
        if len(self._session_cache) >= self.max_cached_sessions:
            oldest_id, oldest_session = self._session_cache.popitem(last=False)
            oldest_session.close()
            logger.info(f"Evicted session for context '{oldest_id}' from cache")
        
        # Создаем новую сессию
        location = self._get_context_path(context_id)
        
        if self.backend == "qdrant":
            session = QdrantVectorStore(
                location=location,
                collection_name="schema_columns",
                embedding_model=self.embedding_model
            )
        else:
            raise ValueError(f"Unsupported backend: {self.backend}")
        
        self._session_cache[context_id] = session
        logger.info(f"Created new session for context '{context_id}'")
        return session
    
    def build_from_preprocessing_results(
        self,
        preprocessing_results: Dict[str, str],
        context_id: Optional[str] = None
    ) -> None:
        """
        Создает векторные индексы из результатов предобработки.
        
        Args:
            preprocessing_results: Dict {db_id: path_to_meta.json} от spider2preprocess
            context_id: Уникальный ID для изоляции индексов (если None, генерируется)
        """
        if not context_id:
            # Генерируем context_id на основе первого db_id
            context_id = list(preprocessing_results.keys())[0].split("_")[0]
        
        logger.info(f"Building vector indexes for context '{context_id}'...")
        
        # Группируем документы по db_id для эффективной загрузки
        documents_by_db: Dict[str, List[Dict]] = {}
        
        for db_id, meta_path in preprocessing_results.items():
            docs_path = meta_path.replace("_meta.json", "_docs.json")
            
            if not os.path.exists(docs_path):
                logger.warning(f"Docs file not found: {docs_path}")
                continue
            
            with open(docs_path, 'r', encoding='utf-8') as f:
                docs_data = json.load(f)
            
            documents_by_db[db_id] = docs_data  # Список документов для этой БД
        
        # Создаем индекс для каждой БД
        session = self._get_or_create_session(context_id)
        
        # Объединяем все документы в одну коллекцию (фильтрация по db_id)
        all_documents = []
        for db_id, docs in documents_by_db.items():
            for doc in docs:
                # Убеждаемся, что db_id есть в метаданных
                doc["metadata"]["db_id"] = db_id
                all_documents.append(doc)
        
        if all_documents:
            session.build_index(all_documents, collection_name="schema_columns")
            logger.info(f"Built index with {len(all_documents)} column documents")
        else:
            logger.warning("No documents found for indexing")
    
    def search_batch(
        self,
        queries_by_db: Dict[str, List[str]],
        top_k: int = 10,
        batch_size: int = 32
    ) -> Dict[str, Dict[str, List[VectorSearchResult]]]:
        """
        Выполняет поиск для групп вопросов, сгруппированных по db_id.
        
        Args:
            queries_by_db: {db_id: [question1, question2, ...]}
            top_k: Число результатов на запрос
            batch_size: Размер батча для эмбеддингов
            
        Returns:
            {db_id: {query: [results]}}
        """
        results = {}
        
        for db_id, queries in queries_by_db.items():
            # Извлекаем context_id из db_id (например, "sqlite_orders" -> "sqlite")
            context_id = db_id.split("_")[0]
            
            # Получаем сессию для этого контекста
            session = self._get_or_create_session(context_id)
            
            # Поиск с фильтрацией по db_id
            search_results = session.search(
                queries=queries,
                top_k=top_k,
                filters={"db_id": db_id},  # Критичный фильтр!
                batch_size=batch_size
            )
            
            results[db_id] = search_results
            logger.info(f"Searched {len(queries)} queries for db '{db_id}'")
        
        return results
    
    def close_all(self) -> None:
        """Закрывает все сессии и освобождает память."""
        for session in self._session_cache.values():
            session.close()
        self._session_cache.clear()
        logger.info("All vector store sessions closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_all()
        return False
    