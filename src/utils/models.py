import json
import os
from pathlib import Path
from typing import Optional, List, Dict, Any

from langchain_core.messages import BaseMessage
from langchain_openai import ChatOpenAI


def load_llm_config(model_name: str) -> Dict[str, Any]:
    """
    Загружает конфигурацию модели из configs/llm.json.
    """
    config_path = Path("configs/llm.json")
    if not config_path.exists():
        # raise FileNotFoundError(f"LLM config not found at {config_path}")
        return {}
    
    with open(config_path, "r", encoding="utf-8") as f:
        full_config = json.load(f)
        
    models_cfg = full_config.get("models", {})
    if model_name not in models_cfg:
        raise ValueError(f"Model '{model_name}' not found in configs/llm.json. Available: {list(models_cfg.keys())}")
    
    return models_cfg[model_name]

def get_model(
    model_name: str = "qwen-local", 
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    temperature: float = 1.0
) -> ChatOpenAI:
    """
    Инициализирует LLM через стандартный OpenAI API клиент.
    
    Args:
        model_name: Ключ модели в configs/llm.json
        base_url: Если передан, заменяет URL из конфига
        api_key: Если передан, заменяет API Key из конфига
        temperature: Если передан, заменяет температуру
        
    Returns:
        Экземпляр ChatOpenAI
    """
    cfg = load_llm_config(model_name)
    final_base_url = base_url or cfg.get("base_url")
    final_api_key = api_key or cfg.get("api_key")

    if not final_base_url:
        raise ValueError(f"base_url is not specified for model '{model_name}' and not provided via override.")

    if final_api_key is not None:
        normalized_api = final_api_key.replace("_", "")
        if normalized_api.isupper() and normalized_api.isalnum():
            final_api_key = os.environ.get(final_api_key)

    llm = ChatOpenAI(
        model=model_name,
        base_url=final_base_url,
        api_key=final_api_key or "empty",
        temperature=temperature,
        disable_streaming=True 
    )
    
    return llm

def serialize_message(msg: BaseMessage) -> Dict[str, Any]:
    """Безопасная сериализация LangChain сообщения в dict."""
    try:
        return msg.model_dump()
    except AttributeError:
        return {"type": msg.type, "content": msg.content}

def serialize_messages(messages: List[BaseMessage]) -> List[Dict[str, Any]]:
    """Сериализация списка сообщений."""
    return [serialize_message(m) for m in messages]
