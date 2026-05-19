from typing import Tuple, Union, Dict, Any
import pandas as pd
from langchain_core.tools import tool


def _format_df_for_llm(df: pd.DataFrame, max_rows: int = 5) -> str:
    """Сериализует DataFrame в компактный markdown-формат для LLM."""
    return df.head(max_rows).to_markdown(index=False, tablefmt="simple")

@tool
def schema_retrieval(table: str, column: str, description: str) -> str:
    """Explicitly add missing schema elements to context."""
    # Здесь должна быть логика проверки наличия в кэше/БД
    # Для прототипа возвращаем заглушку
    return f"[RETRIEVED] Added {table}.{column} to context."

@tool
def schema_exploration(query: str) -> str:
    """Execute lightweight READ-ONLY SQL query for data inspection."""
    status, result = sql_execution(query)  # Ваша существующая функция
    if status == "success" and isinstance(result, pd.DataFrame):
        preview = _format_df_for_llm(result)
        return f"[EXPLORATION OK]\n{preview}"
    return f"[EXPLORATION FAILED] {result}"

@tool
def join_discovery(
    left_table: str, 
    left_column: str, 
    right_table: str, 
    right_column: str, 
    join_type: str, 
    evidence: Dict[str, Any]
) -> str:
    """Register join path using static/exploration evidence."""
    # Валидация структуры evidence
    required_keys = {"naming_pattern", "type_compatibility", "sample_value_overlap", "semantic_coherence"}
    if not required_keys.issubset(evidence.keys()):
        return f"[JOIN ERROR] Missing evidence keys: {required_keys - evidence.keys()}"
    
    # Сохранение в state происходит через callback или глобальный контекст
    return f"[JOIN DISCOVERED] {left_table}.{left_column} -> {right_table}.{right_column} ({join_type})"

@tool
def sql_draft(query: str, purpose: str = "") -> str:
    """Test schema sufficiency with a preliminary SQL query."""
    status, result = sql_execution(query)
    if status == "success" and isinstance(result, pd.DataFrame):
        return f"[DRAFT VALID] Executed successfully. Rows: {len(result)}"
    return f"[DRAFT FAILED] {result}"

@tool
def stop() -> str:
    """Signal schema linking completion."""
    return "[STOP] Schema linking finalized."

# Реестр инструментов для динамической фильтрации по конфигу
TOOL_REGISTRY = {
    "schema_retrieval": schema_retrieval,
    "schema_exploration": schema_exploration,
    "join_discovery": join_discovery,
    "sql_draft": sql_draft,
    "stop": stop,
}

def get_enabled_tools(enabled_names: list[str]) -> Dict[str, Any]:
    """Возвращает словарь инструментов, разрешённых в текущем эксперименте."""
    return {name: TOOL_REGISTRY[name] for name in enabled_names if name in TOOL_REGISTRY}
