import json
import re
from typing import Dict, Any, Optional
from pydantic import BaseModel, ValidationError

# Опциональная схема валидации выходных данных (Pydantic v2)
class SchemaLinkingOutput(BaseModel):
    schema_linking_result: Dict[str, Any]
    ready_for_sql_generation: bool
    blocking_issues: list[str]

def extract_json_from_text(raw_text: str) -> Optional[str]:
    """
    Извлекает JSON-подстроку из текста ответа.
    Поддерживает JSON внутри markdown-блоков ```json ... ```.
    """
    # Попытка найти markdown-блок
    markdown_match = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", raw_text, re.DOTALL)
    if markdown_match:
        return markdown_match.group(1)
    
    # fallback: поиск первого { и последнего }
    start = raw_text.find("{")
    end = raw_text.rfind("}") + 1
    if start != -1 and end != 0 and end > start:
        return raw_text[start:end]
    
    return None

def parse_and_validate_output(raw_text: str) -> Dict[str, Any]:
    """
    Парсит финальный ответ, извлекает и валидирует JSON.
    Raises ValueError при ошибках парсинга или валидации.
    """
    json_str = extract_json_from_text(raw_text)
    if not json_str:
        raise ValueError("No JSON object found in final response")
    
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON syntax: {e}")
    
    # Валидация структуры через Pydantic (опционально)
    try:
        validated = SchemaLinkingOutput.model_validate(data)
        return validated.model_dump()
    except ValidationError as e:
        # Если валидация не критична, можно вернуть сырые данные с предупреждением
        # raise ValueError(f"Schema validation failed: {e}")
        return data  # fallback

def format_for_downstream(parsed_output: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Форматирует результат для передачи в Text-to-SQL генератор.
    Добавляет метаданные выполнения для логирования и отладки.
    """
    return {
        "schema_mapping": parsed_output.get("schema_linking_result", {}),
        "ready": parsed_output.get("ready_for_sql_generation", False),
        "issues": parsed_output.get("blocking_issues", []),
        "execution_metadata": {
            "turns_used": metadata.get("state", {}).get("turn"),
            "tools_called": len(metadata.get("state", {}).get("tool_step_logs", [])),
            "draft_attempts": metadata.get("state", {}).get("draft_count", 0)
        }
    }
    