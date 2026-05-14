from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class VectorSearchResult:
    """Структурированный результат поиска."""
    text: str
    meta: Dict[str, Any]
    score: float
    rank: int

class BaseVectorStore(ABC):
    """
    Абстрактный базовый класс для векторных хранилищ.
    Позволяет легко заменить бэкенд (Qdrant → Chroma → FAISS).
    """
    
    @abstractmethod
    def build_index(self, documents: List[Dict[str, Any]], collection_name: str) -> None:
        """
        Создает векторный индекс из списка документов.
        
        Args:
            documents: Список dict с ключами 'text' и 'metadata'.
            collection_name: Имя коллекции/индекса.
        """
        pass
    
    @abstractmethod
    def search(
        self,
        queries: List[str],
        top_k: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 32
    ) -> Dict[str, List[VectorSearchResult]]:
        """
        Выполняет семантический поиск.
        
        Args:
            queries: Список поисковых запросов.
            top_k: Число лучших результатов на запрос.
            filters: Словарь фильтров (например, {'db_id': '...'}).
            batch_size: Размер батча для обработки запросов.
            
        Returns:
            Dict: {query: [VectorSearchResult, ...]}
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Освобождает ресурсы (соединения, память)."""
        pass
    
    @property
    @abstractmethod
    def is_loaded(self) -> bool:
        """Проверяет, загружен ли индекс в память."""
        pass
