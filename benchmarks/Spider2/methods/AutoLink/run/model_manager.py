import threading
import os
from sentence_transformers import SentenceTransformer
import torch

class ModelManager:    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ModelManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.model = None
            self.device = None
            self.model_lock = threading.Lock()
            self.initialized = True
    
    def load_model(self, model_path: str = None, device: str = "cuda:0"):
        if model_path is None:
            model_path = "BAAI/bge-large-en-v1.5"
                    
        with self.model_lock:
            if self.model is None or self.device != device:
                print(f"Loading model from {model_path} to {device}")
                if device.startswith("cuda") and not torch.cuda.is_available():
                    device = "cpu"
                    print("CUDA not available, falling back to CPU")
                
                self.model = SentenceTransformer(model_path, device=device)
                self.device = device
                print(f"Model loaded successfully on {device}")
                
                if device.startswith("cuda"):
                    gpu_id = int(device.split(":")[-1])
                    memory_allocated = torch.cuda.memory_allocated(gpu_id) / 1024**3
                    memory_reserved = torch.cuda.memory_reserved(gpu_id) / 1024**3
                    print(f"GPU {gpu_id} Memory - Allocated: {memory_allocated:.2f}GB, Reserved: {memory_reserved:.2f}GB")
    
    def get_model(self):
        if self.model is None:
            raise RuntimeError("Model not loaded. Call load_model() first.")
        return self.model
    
    def encode(self, text: str):
        with self.model_lock:
            if self.model is None:
                raise RuntimeError("Model not loaded. Call load_model() first.")
            return self.model.encode(text, convert_to_numpy=True)
    
    def get_device(self):
        return self.device
    
    def get_memory_usage(self):
        if self.device and self.device.startswith("cuda"):
            gpu_id = int(self.device.split(":")[-1])
            memory_allocated = torch.cuda.memory_allocated(gpu_id) / 1024**3
            memory_reserved = torch.cuda.memory_reserved(gpu_id) / 1024**3
            return {
                "allocated": memory_allocated,
                "reserved": memory_reserved,
                "device": self.device
            }
        return None

model_manager = ModelManager()
