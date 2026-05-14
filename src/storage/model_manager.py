import json
import logging
import threading
from typing import Optional, Dict, List, Union

import numpy as np
import torch
from sentence_transformers import SentenceTransformer

from src.utils.logger import get_logger


class EmbeddingModelManager:
    """
    Глобальный менеджер моделей с поддержкой:
    - Singleton (одна копия модели в памяти)
    - Thread-safe доступ через locks
    - Автоматический fallback на CPU
    - Мониторинг GPU памяти
    - Поддержка prompt-based encoding
    """
    
    _instance: Optional['EmbeddingModelManager'] = None
    _lock = threading.Lock()
    
    # Конфигурации известных моделей
    MODEL_CONFIGS: Dict[str, Dict] = json.load(open("config/embedding_models.json", encoding='utf-8'))
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, log_path: str = None):
        if hasattr(self, '_initialized') and self._initialized:
            return
        
        self.logger = get_logger("embedding_model_manager", log_path, mode='w') if log_path else None
        self._models: Dict[str, SentenceTransformer] = {}
        self._locks: Dict[str, threading.Lock] = {}
        self._initialized = True
        if self.logger: self.logger.info("EmbeddingModelManager initialized")
        
    def get_model(
        self,
        model_name: str,
        device: str = "cpu",
        dtype: str = "auto"
    ) -> SentenceTransformer:
        """Возвращает или загружает модель с кэшированием."""
        key = f"{model_name}|{device}|{dtype}"
        
        if key not in self._models:
            with self._lock:
                if key not in self._models:
                    if self.logger: self.logger.info(f"Loading model: {model_name} on {device}")
                    
                    # Возвращаемся на CPU, если CUDA недоступен
                    if device.startswith("cuda") and not torch.cuda.is_available():
                        self.logger.warning("CUDA not available, falling back to CPU")
                        device = "cpu"
                    
                    model_kwargs = {"torch_dtype": dtype if dtype != "auto" else "auto"}
                    model = SentenceTransformer(model_name, device=device, model_kwargs=model_kwargs)
                    
                    self._models[key] = model
                    self._locks[key] = threading.Lock()
                    if self.logger: self.logger.info(f"Model loaded: {model_name} ({key})")
        
        return self._models[key]
    
    def encode(
        self,
        model_name: str,
        texts: Union[str, List[str]],
        device: str = "cpu",
        dtype: str = "auto",
        prompt_name: Optional[str] = None,
        normalize: bool = True,
        batch_size: int = 64
    ) -> np.ndarray:
        """
        Потокобезопасное кодирование текстов.
        """
        if isinstance(texts, str):
            texts = [texts]
        
        model = self.get_model(model_name, device, dtype)
        lock_key = f"{model_name}|{device}|{dtype}"
        lock = self._locks.get(lock_key)
        
        if lock:
            with lock:
                return self._do_encode(model, texts, prompt_name, normalize, batch_size)
        else:
            return self._do_encode(model, texts, prompt_name, normalize, batch_size)
    
    def _do_encode(
        self,
        model: SentenceTransformer,
        texts: List[str],
        prompt_name: Optional[str],
        normalize: bool,
        batch_size: int
    ) -> np.ndarray:
        """Внутренний метод кодирования."""
        encode_kwargs = {
            "sentences": texts,
            "batch_size": batch_size,
            "show_progress_bar": False,
            "convert_to_numpy": True,
        }
        if prompt_name:
            encode_kwargs["prompt_name"] = prompt_name
        
        embeddings = model.encode(**encode_kwargs)
        
        # L2 нормализация для косинусного сходства
        if normalize and embeddings.ndim == 2:
            norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
            norms[norms == 0] = 1
            embeddings = embeddings / norms
        
        return embeddings
    
    def get_vector_size(self, model_name: str) -> int:
        """Возвращает размерность вектора для модели."""
        return self.MODEL_CONFIGS[model_name]["dim"]
    
    def get_query_prompt(self, model_name: str) -> Optional[str]:
        """Возвращает имя промпта для запросов."""
        return self.MODEL_CONFIGS[model_name].get(
            "query_prompt", 
            self.MODEL_CONFIGS[model_name].get("prompt_name", None)
        )
    
    def clear_model(self, model_name: str, device: str = "cpu", dtype: str = "auto"):
        """Освобождает память, удаляя модель из кэша."""
        key = f"{model_name}|{device}|{dtype}"
        if key in self._models:
            del self._models[key]
            if key in self._locks:
                del self._locks[key]
            if device.startswith("cuda") and torch.cuda.is_available():
                torch.cuda.empty_cache()

            if self.logger: self.logger.info(f"Cleared model from cache: {key}")


embedding_model_manager = EmbeddingModelManager()
