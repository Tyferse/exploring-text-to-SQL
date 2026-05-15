import sys
sys.insert(0, '.')

from collections import defaultdict
from typing import List, Dict, Set

from src.storage.core import VectorSearchResult


def enrich_nested_results(
    search_results: List[VectorSearchResult],
    schema_meta: Dict,
    max_expand_per_table: int = 5,
    max_total_results: int = 15
) -> List[VectorSearchResult]:
    """
    Дополняет результаты поиска родительскими структурами и соседними вложенными столбцами.
    """
    enriched: List[VectorSearchResult] = []
    seen_cols: Set[str] = set()
    tables_to_expand: Dict[str, Set[str]] = defaultdict(set)  # table -> {parent_prefix}
    
    # 1. Ищем вложенные столбцы в результатах
    for res in search_results:
        col = res.metadata.get("column_name", "")
        table = res.metadata.get("table_name", "")
        full_key = f"{table}.{col}"
        
        if full_key in seen_cols:
            continue
        seen_cols.add(full_key)
        enriched.append(res)
        
        # Если столбец вложенный, помечаем таблицу и префикс для расширения
        if "." in col:
            prefix = col.split(".")[0]
            tables_to_expand[table].add(prefix)
            
    # 2. Для каждой таблицы находим родительские и соседние столбцы
    for table, prefixes in tables_to_expand.items():
        table_data = schema_meta.get("tables", {}).get(table, {})
        all_cols = table_data.get("columns", [])
        all_types = table_data.get("types", [])
        all_descs = table_data.get("descriptions", [])
        col_info = dict(zip(all_cols, zip(all_types, all_descs)))
        
        expanded_count = 0
        for prefix in prefixes:
            # Добавляем сам родительский столбец, если он есть
            if prefix in col_info and f"{table}.{prefix}" not in seen_cols:
                typ, desc = col_info[prefix]
                enriched.append(VectorSearchResult(
                    id=0, text="",
                    metadata={
                        "table_name": table,
                        "column_name": prefix,
                        "column_type": typ,
                        "description": desc,
                        "role": "parent_structure"
                    },
                    score=0.0,  # Контекстный, не ранжируемый
                    rank=len(enriched)
                ))
                seen_cols.add(f"{table}.{prefix}")
                
            # Добавляем соседние вложенные столбцы
            for col in all_cols:
                if col.startswith(f"{prefix}.") and f"{table}.{col}" not in seen_cols:
                    typ, desc = col_info[col]
                    enriched.append(VectorSearchResult(
                        text="",
                        metadata={
                            "table_name": table,
                            "column_name": col,
                            "column_type": typ,
                            "description": desc,
                            "role": "sibling_column"
                        },
                        score=0.0,
                        rank=len(enriched)
                    ))
                    seen_cols.add(f"{table}.{col}")
                    expanded_count += 1
                    
            if expanded_count >= max_expand_per_table:
                break
                
    # 3. Сортировка: точные совпадения -> родитель -> соседи
    def sort_key(r):
        role = r.metadata.get("role", "exact_match")
        order = {"exact_match": 0, "parent_structure": 1, "sibling_column": 2}
        return (order.get(role, 3), -r.score)
        
    enriched.sort(key=sort_key)
    return enriched[:max_total_results]
