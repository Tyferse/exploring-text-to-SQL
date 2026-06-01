import json
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Callable 

# ---- Форматы представления схемы

def format_detailed_block(
    table_name: str,
    columns: List[Dict[str, Any]],
    similar_tables: Optional[List[str]] = None,
    include_samples: bool = True,
    include_descriptions: bool = True,
    max_samples: int = 3,
    **kwargs
) -> str:
    """
    Подробный формат: с описаниями, типами, примерами + similar_tables ВСЕГДА.
    """
    lines = [f"###Table full name: {table_name}", "["]
    
    # 🔹 similar_tables — ОБЯЗАТЕЛЬНО, если передан и не пуст
    if similar_tables:
        clean_similar = [str(t).strip() for t in similar_tables if t and str(t).strip()]
        if clean_similar:
            # Вставляем сразу после заголовка таблицы
            lines.insert(1, f"**Some other tables have the similar structure: [{', '.join(clean_similar)}]**")
    
    for col in columns:
        col_name = col.get("column_name", "unknown")
        col_type = col.get("data_type", "TEXT")
        
        parts = [col_name, f"Type: {col_type}"]
        
        if include_samples:
            samples = _process_column_values(col.get("sample_values", []), max_samples)
            if samples:
                parts.append(f"Sample values: {samples}")
        
        if include_descriptions:
            desc = col.get("description", "")
            if desc:
                parts.append(f"Description: {desc}")
        
        content = "; ".join(p for p in parts[1:] if p)
        lines.append(f"\t{parts[0]} ({content})")
    
    lines.append("]")
    lines.append("-" * 50)
    lines.append("")
    return "\n".join(lines)


def format_compact_block(
    table_name: str,
    columns: List[Dict[str, Any]],
    similar_tables: Optional[List[str]] = None,
    compact_separator: str = ", ",
    **kwargs
) -> str:
    """
    Компактный формат: только имена и типы + similar_tables ВСЕГДА.
    """
    lines = [f"###Table: {table_name}"]
    
    if similar_tables:
        clean_similar = [str(t).strip() for t in similar_tables if t and str(t).strip()]
        if clean_similar:
            lines.append(f"**Similar: [{', '.join(clean_similar)}]**")
    
    if columns:
        cols_str = compact_separator.join(
            f"{c.get('column_name', '?')}:{c.get('data_type', '?')}" 
            for c in columns
        )
        lines.append(f"[{cols_str}]")
    else:
        lines.append("[]")
    
    lines.append("-" * 50)
    lines.append("")
    return "\n".join(lines)


def format_minimal_block(
    table_name: str,
    columns: List[Dict[str, Any]],
    similar_tables: Optional[List[str]] = None,
    **kwargs
) -> str:
    """
    Минимальный формат: только имена колонок + similar_tables ВСЕГДА.
    """
    parts = [f"{table_name}: ["]
    
    if similar_tables:
        clean_similar = [str(t).strip() for t in similar_tables if t and str(t).strip()]
        if clean_similar:
            parts.append(f"similar:[{', '.join(clean_similar)}]; ")
    
    col_names = [c.get("column_name", "?") for c in columns]
    parts.append(", ".join(col_names))
    parts.append("]\n")
    
    return "".join(parts)


def format_json_block(
    table_name: str,
    columns: List[Dict[str, Any]],
    similar_tables: Optional[List[str]] = None,
    include_samples: bool = True,
    include_descriptions: bool = True,
    max_samples: int = 3,
    **kwargs
) -> str:
    """
    JSON-формат с обязательным полем similar_tables.
    """
    block = {
        "table_name": table_name,
        "columns": [
            {
                "name": col.get("column_name"),
                "type": col.get("data_type"),
                **({"samples": _process_column_values(col.get("sample_values", []), max_samples)} if include_samples else {}),
                **({"description": col.get("description")} if include_descriptions and col.get("description") else {})
            }
            for col in columns
        ]
    }

    if similar_tables:
        clean_similar = [str(t).strip() for t in similar_tables if t and str(t).strip()]
        block["similar_tables"] = clean_similar
    
    return json.dumps(block, ensure_ascii=False) + "\n\n"

# ---- Обработка значений

def _process_column_values(values: List[Any], max_samples: int = 3, max_len: int = 250) -> List[str]:
    """Обрабатывает примерные значения: сериализует, обрезает, ограничивает количество."""
    if not values:
        return []
    
    result = []
    for v in values[:max_samples]:
        if isinstance(v, (dict, list)):
            try:
                v_str = json.dumps(v, ensure_ascii=False)
            except TypeError:
                v_str = str(v)
        else:
            v_str = str(v)
        
        # Обрезка длинных строк
        s = v_str.replace("\n", " ").replace("\r", " ")
        if len(s) > max_len:
            s = s[:max_len] + "...(truncated)"
        result.append(s)
    
    return result

def estimate_prompt_length(text: str, chars_per_token: float = 4.0) -> int:
    """Оценка количества токенов по длине строки."""
    return max(1, int(len(text) / chars_per_token))

def remove_sample_values(columns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Удаляет поле column_vals из всех колонок."""
    result = []
    for col in columns:
        new_col = {k: v for k, v in col.items() if k != "column_vals"}
        result.append(new_col)
    return result

def remove_descriptions(columns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Удаляет поле description из всех колонок."""
    result = []
    for col in columns:
        new_col = {k: v for k, v in col.items() if k != "description"}
        result.append(new_col)
    return result

def limit_columns_per_table(
    table_mapping: Dict[str, List[Dict[str, Any]]], 
    k: int
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Оставляет только первые k колонок в каждой таблице.
    
    Args:
        table_mapping: {table_name: [col_info, ...]}
        k: Максимальное количество колонок на таблицу
        
    Returns:
        Обрезанное отображение таблиц
    """
    result = {}
    for table_name, columns in table_mapping.items():
        result[table_name] = columns[:k] if len(columns) > k else columns

    return result

def load_schemas(docs_dir: str) -> Dict[str, Dict[str, Dict[int, Dict[str, Any]]]]:
    """Загружает *_docs.json в формат {db_id: {table_name: {column_id: {column, type, ...}}}}."""
    schemas = {}
    docs_path = Path(docs_dir)
    if not docs_path.exists():
        return schemas

    for doc_file in docs_path.glob("*_docs.json"):
        db_id = doc_file.stem.replace("_docs", "")
        with open(doc_file, "r", encoding="utf-8") as f:
            docs = json.load(f)
        
        table_map: Dict[str, Dict[int, Dict[str, Any]]] = {}
        for col in docs:
            meta = col.get("metadata", {})
            tn = meta.get("table_name")
            if not tn:
                continue
            if tn not in table_map:
                table_map[tn] = {}
            
            table_map[tn][col["id"]] = {
                "column_name": meta.get("column_name", col["id"]),
                "data_type": meta.get("data_type", meta.get("column_type", "TEXT")),
                "description": meta.get("description", "None"),
                "sample_values": meta.get("column_vals", meta.get("sample_values", [])),
            }

        schemas[db_id] = table_map
    
    return schemas

def load_similar_tables(meta_path: str) -> Dict[str, Dict[str, List[str]]]:
    similar_tables = {}
    for meta_file in Path(meta_path).glob("*_meta.json"):
        db_id = meta_file.stem.replace("_meta", "")
        with open(meta_file, "r", encoding="utf-8") as f:
            meta_data = json.load(f)["tables"]
            similar_tables[db_id] = {table: meta_data[table]["similar_table"] 
                                     for table in meta_data 
                                     if meta_data[table].get("similar_table")}
    return similar_tables

def compress_schema_to_fit(
    table_mapping: Dict[str, List[Dict[str, Any]]],
    target_max_tokens: int,
    block_formatter: Callable = format_detailed_block,
    chars_per_token: float = 4.0,
    min_columns: int = 1,
    similar_tables: Optional[Dict[str, List[str]]] = None,
    **formatter_kwargs
) -> Tuple[Dict[str, List[Dict[str, Any]]], List[str]]:
    """
    Поэтапно сжимает схему до целевого размера в токенах.
    
    Этапы сжатия (применяются последовательно):
    1. Удаление значений
    2. Удаление описаний  
    3. Ограничение колонок на таблицу (итеративное уменьшение k)
    
    Args:
        table_mapping: Исходное отображение таблиц и колонок
        target_max_tokens: Целевой лимит токенов (обычно 70% от context_length)
        chars_per_token: Коэффициент для оценки токенов
        min_columns: Минимальное количество колонок, которое нужно оставить в таблице
        similar_tables: Отображение из таблицы в список похожих таблиц, если таковые есть
        formatter_kwargs: Дополнительные аргументы для функции block_formatter
        
    Returns:
        Tuple[сжатое_отображение, список_применённых_стратегий]
    """
    strategies_applied = []
    similar_tables = similar_tables if similar_tables else {}
    current_mapping = deepcopy(table_mapping)
    
    # Вспомогательная функция для оценки
    def current_length(mapping: Dict, similar_tables: Dict = {}) -> int:
        schema_text = "".join(block_formatter(tbl, cols, similar_tables.get(tbl, []), **formatter_kwargs) 
                              for tbl, cols in mapping.items())
        return estimate_prompt_length(schema_text, chars_per_token)
    
    # Этап 1: Удаление примеров значений
    if current_length(current_mapping, similar_tables) > target_max_tokens:
        current_mapping = {tbl: remove_sample_values(cols) for tbl, cols in current_mapping.items()}
        strategies_applied.append("removed_sample_values")
    
    # Этап 2: Удаление descriptions
    if current_length(current_mapping, similar_tables) > target_max_tokens:
        current_mapping = {tbl: remove_descriptions(cols) for tbl, cols in current_mapping.items()}
        strategies_applied.append("removed_descriptions")
    
    # Этап 3: Итеративное ограничение колонок
    if current_length(current_mapping, similar_tables) > target_max_tokens:
        # Находим максимальное количество колонок в любой таблице
        max_cols = max(len(cols) for cols in current_mapping.values()) if current_mapping else 0
        
        # Бинарный поиск оптимального k
        low, high = min_columns, max_cols
        best_mapping = current_mapping
        
        while low <= high:
            mid = (low + high) // 2
            trial_mapping = limit_columns_per_table(current_mapping, mid)
            
            if estimate_prompt_length(
                "".join(block_formatter(tbl, cols, similar_tables.get(tbl, []), **formatter_kwargs) 
                        for tbl, cols in trial_mapping.items()),
                chars_per_token
            ) <= target_max_tokens:
                best_mapping = trial_mapping
                high = mid - 1  # Пробуем ещё сильнее сжать
            else:
                low = mid + 1   # Нужно оставить больше колонок
        
        if best_mapping != current_mapping:
            current_mapping = best_mapping
            strategies_applied.append(f"limited_columns_to_k={low}")
    
    if current_length(current_mapping, similar_tables) > target_max_tokens:
        current_mapping = limit_columns_per_table(current_mapping, min_columns)
        if not any(s.startswith("limited_columns_to_k=") for s in strategies_applied):
            strategies_applied.append(f"forced_min_columns_{min_columns}")
    
    return current_mapping, strategies_applied
