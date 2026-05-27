import functools
import json
from typing import Dict, Any, Optional

import pandas as pd
from langchain_core.tools import tool

from src.storage.vector_manager import VectorStoreManager
from src.utils.sql_execution import SQLExecutor


def _format_df_for_llm(df: pd.DataFrame, max_rows: int = 5) -> str:
    """Сериализует DataFrame в компактный markdown-формат для LLM."""
    return df.head(max_rows).to_markdown(index=False, tablefmt="simple")

@tool
def schema_retrieval(
    table: str, column: str, description: str, db_id: str, 
    vsm: VectorStoreManager, additional_k: int = 5, 
    input_data_root: str = "Spider2/spider2-lite"
) -> str:
    """Explicitly add missing schema elements to context."""
    text = f"Table: {table}. Column: {column}. Description: {description}"
    results = vsm.search_batch(input_data_root, {db_id: [text]}, additional_k, is_query=False)
    if not results:
        return f"[RETRIEVAL EMPTY] No similar columns found for: '{text}'"
    
    retrieved = []
    for hit in results:
        payload = hit.payload
        meta = payload.get("metadata", {})
        desc = payload.get("text")
        desc = None if not desc else desc.split("Description: ", 1)[1]
        col_meta = {
            "column_id": payload.get("id"),
            "table_name": meta.get("table_name"),
            "column_name": meta.get("column_name"),
            "description": desc,
            "similarity_score": hit.score
        }
        retrieved.append(col_meta)
    
    summary = "; ".join([f"{r['table_name']}.{r['column_name']}" for r in retrieved])
    return f"[RETRIEVED {len(retrieved)} columns]\n{summary}\n\nDetails: {json.dumps(retrieved, ensure_ascii=False)}"

@tool
def schema_exploration(query: str, executor: SQLExecutor, db_name: str, dialect: str) -> str:
    """Execute lightweight READ-ONLY SQL query for data inspection."""
    status, result = executor.thread_safe_sql_execution(query, db_name, dialect)
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
    validation_query: Optional[str] = None,
    db_name: Optional[str] = None,
    dialect: Optional[str] = None,
    executor: Optional[SQLExecutor] = None,
    evidence: Optional[Dict[str, Any]] = None
) -> str:
    """Register join path using static/exploration evidence."""
    
    base_response = f"[JOIN DISCOVERY INITIATED]\nLeft: {left_table}.{left_column}\nRight: {right_table}.{right_column}\nType: {join_type}"
    if validation_query is not None:
        status, result = executor.thread_safe_sql_execution(validation_query, db_name, dialect)
        join_valid = False
        preview = "None"
        if status == "success" and isinstance(result, pd.DataFrame):
            if not result.empty:
                join_valid = True
                preview = _format_df_for_llm(result)
        
        validity_status = "VALID (Rows > 0)" if join_valid else "INVALID (Rows = 0 or Error)"            
        return f"{base_response}\n\nValidation Query Execution:\n{preview}\n\nConclusion: Join is {validity_status}"
    
    if evidence is not None:
        required_keys = {"naming_pattern", "type_compatibility", "sample_value_overlap", "semantic_coherence"}
        if not required_keys.issubset(evidence.keys()):
            return f"[JOIN ERROR] Missing evidence keys: {required_keys - set(evidence.keys())}"
    
    return f"{base_response}\n[WARNING] No validation_query provided. Join registered based on static evidence only."

@tool
def sql_draft(query: str, executor: SQLExecutor, db_name: str, dialect: str, purpose: str = "") -> str:
    """Test schema sufficiency with a preliminary SQL query."""
    if "LIMIT" not in query.upper():
        query = query.rstrip(";").strip() + " LIMIT 10;"

    status, result = executor.thread_safe_sql_execution(query, db_name, dialect)
    if status == "success" and isinstance(result, pd.DataFrame):
        preview = _format_df_for_llm(result, 10)
        return f"[SQL DRAFT EXECUTION]\nPurpose: {purpose}\nQuery: {query}\n\nResult:\n{preview}"
    
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
