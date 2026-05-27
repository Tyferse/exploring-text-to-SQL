import sys
sys.path.insert(0, ".")

import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from langchain_core.language_models import BaseChatModel
from tqdm import tqdm

from .table_linking import TableLinking
from .column_linking import ColumnLinking, DEFAULT_RETRY_CONFIG
from src.utils.logger import get_logger
from src.utils.models import get_model
from src.utils.run_manager import resolve_run_id


def _load_cached_result(cache_dir: Path, instance_id: str, stage: str) -> Optional[Dict[str, Any]]:
    """Загружает результат этапа из кэша, если он есть и валиден."""
    cache_file = cache_dir / f"{stage}_results" / f"{instance_id}.json"
    if not cache_file.exists():
        return None
    
    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Проверяем, что результат успешный и содержит нужные поля
        if data.get("success") and ("tables_selected" in data or "column_ids" in data):
            return data
    except (json.JSONDecodeError, KeyError):
        pass
    return None


def _save_cached_result(cache_dir: Path, instance_id: str, stage: str, result: Dict[str, Any]):
    """Сохраняет результат этапа в кэш."""
    cache_file = cache_dir / f"{stage}_results" / f"{instance_id}.json"
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)


def _filter_columns_by_tables(
    all_columns: Dict[str, List[Tuple[int, str]]], 
    selected_tables: List[Dict[str, str]]
) -> Dict[str, List[Tuple[int, str]]]:
    """Фильтрует словарь колонок, оставляя только те, что принадлежат выбранным таблицам."""
    table_names = {t["table_name"] for t in selected_tables if t.get("table_name")}
    return {
        tn: cols for tn, cols in all_columns.items() 
        if tn in table_names
    }


class TableColumnLinking:
    def __init__(
        self,
        run_id: str,
        model: BaseChatModel,
        tasks: Optional[List[Dict[str, Any]]] = None,
        run_root: str = "logs/runs",
        input_data_root: str = "Spider2/spider2-lite",
        data_root: str = "data",
        storage_root: str = "storage",
        prompt_dir: str = "config/prompts/schema_linking",
        max_workers: int = 4,
        # Параметры table linking
        table_prompt_name: str = "sl_table_level",
        table_max_schema_length: int = 32000,
        table_max_attempts: int = 4,
        max_tables: Optional[int] = None,
        # Параметры column linking
        column_prompt_name: str = "sl_column_level",
        column_max_schema_length: int = 64000,
        column_max_attempts: int = 4,
        max_columns: Optional[int] = None,
        # Общие параметры
        retry_config: Optional[Dict[str, float]] = None,
        cache_prefix: Optional[str] = ""
    ):
        self.run_id = run_id
        self.cache_dir = Path(run_root) / run_id / "schema_linking"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.input_data_root = input_data_root
        self.data_root = Path(data_root)
        self.storage_root = storage_root
        self.max_workers = max_workers
        self.cache_prefix = cache_prefix

        self.model = model
        retry_config = DEFAULT_RETRY_CONFIG if retry_config is None else retry_config
        self.table_linker = TableLinking(
            run_id, model, tasks,
            run_root=run_root, input_data_root=input_data_root, data_root=data_root, 
            prompt_dir=prompt_dir, prompt_name=table_prompt_name, 
            max_schema_length=table_max_schema_length, 
            retry_config={k: v if k != "max_attempts" else table_max_attempts 
                          for k,v in retry_config.items()}, 
            max_workers=max_workers, max_tables=max_tables,
            stage = cache_prefix + "table_linking"
        )
        self.column_linker = ColumnLinking(
            run_id, model, tasks,
            run_root=run_root, input_data_root=input_data_root, data_root=data_root, 
            prompt_dir=prompt_dir, prompt_name=column_prompt_name, 
            max_schema_length=column_max_schema_length, 
            retry_config={k: v if k != "max_attempts" else column_max_attempts 
                          for k,v in retry_config.items()}, 
            max_workers=max_workers, max_columns=max_columns,
            stage = cache_prefix + "column_linking"
        )
        self.column_linker.schemas = self.table_linker.schemas
        self.column_linker.similar_tables = self.table_linker.similar_tables

        self.logger = get_logger("double_linking", str(self.cache_dir / "table_column_linking.log"))


    def _merge_results(
        self,
        table_result: Dict[str, Any], 
        column_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Объединяет результаты двух этапов в финальный артефакт."""
        # Приоритет: column_result может переопределить таблицы, если нашёл лучше
        tables = column_result.get("tables_selected") or table_result.get("tables_selected", [])
        
        return {
            "instance_id": column_result.get("instance_id"),
            "db_id": column_result.get("db_id") or table_result.get("db_id"),
            "tables_selected": tables,
            "columns_mapped": column_result.get("columns_mapped", []),
            "used_indices": column_result.get("column_ids", []),
            "blocking_issues": column_result.get("blocking_issues", []) or table_result.get("blocking_issues", []),
            "success": column_result.get("success", False),
            "metadata": {
                "table_attempts": table_result.get("total_attempts", 0),
                "column_attempts": column_result.get("total_attempts", 0),
                "table_latency_ms": table_result.get("total_latency_ms", 0),
                "column_latency_ms": column_result.get("total_latency_ms", 0),
                "total_latency_ms": (table_result.get("total_latency_ms", 0) + 
                                    column_result.get("total_latency_ms", 0)),
            }
        }
    
    def extract_all_candidates(self):
        with open(self.cache_dir / f"table_column_linking_candidates.json", "w", encoding="utf-8") as f:
            data = {}
            for file in (self.cache_dir / "table_column_linking_results").glob("*.json"):
                with open(file, "r", encoding="utf-8") as indf:
                    result = json.load(indf)
                
                iid = result["instance_id"]
                data[iid] = {
                    "db_id": self.column_linker.instances[iid]["db_id"],
                    "used_indices": result.get("used_indices", [])
                }
                
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    def _process_single_instance(
        self,
        instance_id: str,
        instance_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Обрабатывает один инстанс: table linking → column linking → merge → save.
        Все промежуточные результаты пишутся на диск.
        """
        try:
            # Шаг 0: Проверка финального кэша (если уже есть колонки — пропускаем)
            final_cache = _load_cached_result(self.cache_dir, instance_id, "table_column_linking")
            if final_cache and final_cache.get("success"):
                self.logger.info(f"{instance_id} | Already processed (column cache hit)")
                final_cache["_from_cache"] = True
                return final_cache
            
            # Шаг 1: Table Linking (с кэшированием)
            table_result = _load_cached_result(self.cache_dir, instance_id, self.cache_prefix + "table_linking")
            
            if not table_result or not table_result.get("success", False):
                self.logger.info(f"{instance_id} | Running table linking...")
                table_result = self.table_linker._process_single_instance(instance_id, instance_data)
                table_result_dict = table_result.to_dict() if hasattr(table_result, "to_dict") else table_result
                # _save_cached_result(cache_dir, instance_id, "table_linking", table_result_dict)
                self.logger.info(f"  Table result saved | Success: {table_result_dict.get('success', False)}")
            else:
                self.logger.info(f"{instance_id} | Table result loaded from cache")
                table_result_dict = table_result
            
            if not table_result_dict.get("success"):
                self.logger.warning(f"{instance_id} | Table linking failed, skipping column stage")
                # Сохраняем неудачный результат как финальный, чтобы не пытаться снова
                # _save_cached_result(self.cache_dir, instance_id, self.cache_prefix +"column_linking", {
                #     **table_result_dict,
                #     "columns_mapped": [],
                #     "column_ids": [],
                #     "final_error": table_result_dict.get("final_error", "Table linking failed")
                # })
                return {
                    **table_result_dict,
                    "columns_mapped": [],
                    "column_ids": [],
                    "final_error": table_result_dict.get("final_error", "Table linking failed")
                }
            
            # Шаг 2: Подготовка фильтра для column linking
            # Загружаем полную схему для этой БД (из колонк-линкера, чтобы не дублировать)
            db_id = instance_data.get("db_id", instance_id.split("_", 1)[0])
            full_schema = self.column_linker.schemas.get(db_id, {})
            
            if not full_schema:
                self.logger.warning(f"{instance_id} | Schema not found for db_id: {db_id}")
                return {"instance_id": instance_id, "success": False, "final_error": "Schema missing"}
            
            # Преобразуем схему в формат {table_name: [column_names]}
            all_columns = {
                tn: [(cid, meta["column_name"]) for cid, meta in cols.items()]
                for tn, cols in full_schema.items()
            }
            
            # Фильтруем: оставляем только колонки из выбранных таблиц
            selected_tables = table_result_dict.get("tables_selected", [])
            filtered_columns = _filter_columns_by_tables(all_columns, selected_tables)
            
            if not filtered_columns:
                self.logger.warning(f"{instance_id} | No columns after filtering by selected tables")
                final_result = {
                    "instance_id": instance_id,
                    "db_id": db_id,
                    "tables_selected": selected_tables,
                    "columns_mapped": [],
                    "column_ids": [],
                    "blocking_issues": ["No columns found in selected tables"],
                    "success": False,
                    "final_error": "Empty filtered schema"
                }
                # _save_cached_result(self.cache_dir, instance_id, self.cache_prefix + "column_linking", final_result)
                return final_result
            
            # Шаг 3: Column Linking (на отфильтрованной схеме)
            self.logger.info(f"{instance_id} | Running column linking on {len(filtered_columns)} tables...")
            
            column_data = {
                **instance_data,
                "available_ids": [cid for tn in filtered_columns for cid, _ in filtered_columns[tn]],
            }
            
            column_result_dict = _load_cached_result(self.cache_dir, instance_id, self.cache_prefix + "column_linking")
            try:
                column_result = self.column_linker._process_single_instance(...)
                column_result_dict = column_result.to_dict() if hasattr(column_result, "to_dict") else column_result
            except Exception as e:
                self.logger.error(f"{instance_id} | Column linking failed: {e}")
                column_result_dict = {"success": False, "final_error": str(e)}
            
            # _save_cached_result(cache_dir, instance_id, "column_linking", column_result_dict)
            self.logger.info(f"  Column result saved | Success: {column_result_dict.get('success', False)}")
            
            # Шаг 4: Слияние результатов и сохранение финального артефакта
            final_result = self._merge_results(table_result_dict, column_result_dict)
            
            # Перезаписываем кэш column_linking финальным слитым результатом
            _save_cached_result(self.cache_dir, instance_id, "table_column_linking", final_result)
            
            self.logger.info(
                f"{instance_id} | Final result | Tables: {len(final_result['tables_selected'])} | "
                f"Columns: {len(final_result['columns_mapped'])} | Success: {final_result['success']}"
            )
            
            return final_result
            
        except Exception as e:
            self.logger.exception(f"✗ {instance_id} | Critical error in double linking")
            error_result = {
                "instance_id": instance_id,
                "success": False,
                "final_error": str(e),
                "tables_selected": [],
                "columns_mapped": [],
                "column_ids": [],
                "blocking_issues": [f"Critical error: {str(e)}"]
            }
            _save_cached_result(self.cache_dir, instance_id, "table_column_linking", error_result)
            return error_result


    def run(self) -> Dict[str, Any]:
        """
        Запускает последовательный пайплайн: table linking → column linking.
        
        Returns:
            Статистика выполнения.
        """
        self.logger.info(f"Starting Table Column linking pipeline | Run: {self.run_id}")
        
        # Загрузка инстансов (берём из column_linker, т.к. он загружает все задачи)
        instances = self.table_linker.instances
        self.logger.info(f"Loaded {len(instances)} instances for processing")
        
        # Статистика
        stats = {"total": len(instances), "successful": 0, "failed": 0, "skipped": 0}
        results = {}
        
        # Параллельная обработка инстансов
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._process_single_instance, iid, data): iid
                for iid, data in instances.items()
            }
            
            for future in tqdm(as_completed(futures), total=len(futures), desc="Table to Column Linking"):
                iid = futures[future]
                try:
                    result = future.result()
                    results[iid] = result
                    if result.get("success"):
                        stats["successful"] += 1
                    elif result.get("_from_cache"):
                        stats["skipped"] += 1
                    else:
                        stats["failed"] += 1
                except Exception as e:
                    stats["failed"] += 1
                    self.logger.exception(f"{iid} | Unhandled exception")
                    results[iid] = {"instance_id": iid, "success": False, "final_error": str(e)}
                except KeyboardInterrupt:
                    self.logger.warning("Interrupted by user, saving partial stats...")
                    executor.shutdown(wait=False, cancel_futures=True)
        
        # Сохранение статистики
        stats["completed_at"] = time.time()
        stats_path = self.cache_dir / "table_column_linking_stats.json"
        with open(stats_path, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Table to Column linking finished | Success: {stats['successful']}/{stats['total']}")
        self.logger.info(f"Stats saved to: {stats_path}")
        
        return results


class ColumnTableLinking:
    """
    Обратный пайплайн: column linking → table linking.
    
    Алгоритм:
    1. Запускаем column linking на полной схеме БД
    2. Извлекаем уникальные таблицы из найденных колонок
    3. Запускаем table linking с подсказкой: "эти таблицы уже подтверждены колонками"
    4. Объединяем результаты: приоритет у колонок, таблицы дополняются описаниями из table-этапа
    """
    
    def __init__(
        self,
        run_id: str,
        model: BaseChatModel,
        tasks: Optional[List[Dict[str, Any]]] = None,
        run_root: str = "logs/runs",
        input_data_root: str = "Spider2/spider2-lite",
        data_root: str = "data",
        storage_root: str = "storage",
        prompt_dir: str = "config/prompts/schema_linking",
        max_workers: int = 4,
        # Параметры column linking (первый этап)
        column_prompt_name: str = "sl_column_level",
        column_max_schema_length: int = 64000,
        column_max_attempts: int = 4,
        max_columns: Optional[int] = None,
        # Параметры table linking (второй этап)
        table_prompt_name: str = "sl_table_level",
        table_max_schema_length: int = 32000,
        table_max_attempts: int = 4,
        max_tables: Optional[int] = None,
        # Общие параметры
        retry_config: Optional[Dict[str, float]] = None,
        cache_prefix: Optional[str] = ""
    ):
        self.run_id = run_id
        self.cache_dir = Path(run_root) / run_id / "schema_linking"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.input_data_root = input_data_root
        self.data_root = Path(data_root)
        self.storage_root = storage_root
        self.max_workers = max_workers
        self.cache_prefix = cache_prefix

        self.model = model
        retry_config = DEFAULT_RETRY_CONFIG if retry_config is None else retry_config
        self.column_linker = ColumnLinking(
            run_id, model, tasks,
            run_root=run_root, input_data_root=input_data_root, data_root=data_root, 
            prompt_dir=prompt_dir, prompt_name=column_prompt_name, 
            max_schema_length=column_max_schema_length, 
            retry_config={k: v if k != "max_attempts" else column_max_attempts 
                          for k,v in retry_config.items()}, 
            max_workers=max_workers, max_columns=max_columns,
            stage=cache_prefix + "table_linking"
        )
        self.table_linker = TableLinking(
            run_id, model, tasks,
            run_root=run_root, input_data_root=input_data_root, data_root=data_root, 
            prompt_dir=prompt_dir, prompt_name=table_prompt_name, 
            max_schema_length=table_max_schema_length, 
            retry_config={k: v if k != "max_attempts" else table_max_attempts 
                          for k,v in retry_config.items()}, 
            max_workers=max_workers, max_tables=max_tables,
            stage=cache_prefix + "column_linking"
        )
        
        # Синхронизация схем (чтобы не грузить дважды)
        self.table_linker.schemas = self.column_linker.schemas
        self.table_linker.similar_tables = self.column_linker.similar_tables

        self.logger = get_logger("column_table_linking", str(self.cache_dir / "column_table_linking.log"))

    def _merge_results_reverse(
        self,
        column_result: Dict[str, Any], 
        table_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Объединяет результаты: приоритет у колонок (они найдены первыми),
        таблицы дополняются описаниями из table-этапа.
        """
        # Приоритет: колонки из column_result, таблицы — объединение
        columns = column_result.get("columns_mapped", [])
        column_ids = column_result.get("column_ids", [])
        
        # Таблицы из column_result (выведенные из колонок) + дополнения из table_result
        tables_from_column_stage = {t["table_name"] for t in column_result.get("tables_selected", []) if t.get("table_name")}
        tables_from_table_stage = {t["table_name"] for t in table_result.get("tables_selected", []) if t.get("table_name")}

        # Объединяем: если таблица есть в обоих, берём описание из table_result (оно богаче)
        table_descriptions = {t["table_name"]: t for t in table_result.get("tables_selected", [])}
        merged_tables = []
        merged_columns = []
        merged_indices = []

        for tn in tables_from_column_stage:
            if tn in table_descriptions:
                # Берём богатое описание из table-этапа, но сохраняем статус "подтверждено колонками"
                merged_tables.append({
                    **table_descriptions[tn],
                    "relevance_reasoning": f"Confirmed by columns + {table_descriptions[tn].get('relevance_reasoning', '')}"
                })
                merged_indices.extend([
                    cid for cid in self.table_linker.schemas[table_result["db_id"]][tn].keys() 
                    if cid in column_ids
                ])
                for cid, cmeta in self.table_linker.schemas[table_result["db_id"]][tn].items():
                    if cid in merged_indices:
                        for c in columns:
                            if c["table_name"] == tn and c["column_name"] == cmeta["column_name"]:
                                merged_columns.append(c)
                                break

        # Добавляем таблицы из table-этапа, которых не было в column-результате
        for tn in tables_from_table_stage:
            if tn not in tables_from_column_stage:
                merged_tables.append({
                    **table_descriptions[tn],
                    "relevance_reasoning": f"Confirmed by tables + {table_descriptions[tn].get('relevance_reasoning', '')}"
                })
                merged_indices.extend(list(self.table_linker.schemas[table_result["db_id"]][tn].keys()))
                merged_columns.extend([{
                    "table_name": tn,
                    "column_name": cmeta["column_name"],
                    "role": "select",  # default
                    "confidence": "medium",
                    "reasoning": "Included because table was selected",
                    "literal_value": None
                } for cmeta in self.table_linker.schemas[table_result["db_id"]][tn].values()])
        
        return {
            "instance_id": column_result.get("instance_id"),
            "db_id": column_result.get("db_id") or table_result.get("db_id"),
            "tables_selected": merged_tables,
            "columns_mapped": merged_columns,
            "used_indices": list(set(merged_indices)),
            "blocking_issues": column_result.get("blocking_issues", []) or table_result.get("blocking_issues", []),
            "success": column_result.get("success", False) and table_result.get("success", False),
            "metadata": {
                "column_attempts": column_result.get("total_attempts", 0),
                "table_attempts": table_result.get("total_attempts", 0),
                "column_latency_ms": column_result.get("total_latency_ms", 0),
                "table_latency_ms": table_result.get("total_latency_ms", 0),
                "total_latency_ms": (column_result.get("total_latency_ms", 0) + 
                                    table_result.get("total_latency_ms", 0)),
                "pipeline_order": "column_first"
            }
        }
    
    def extract_all_candidates(self):
        with open(self.cache_dir / f"column_table_linking_candidates.json", "w", encoding="utf-8") as f:
            data = {}
            for file in (self.cache_dir / "column_table_linking_results").glob("*.json"):
                with open(file, "r", encoding="utf-8") as indf:
                    result = json.load(indf)
                
                iid = result["instance_id"]
                data[iid] = {
                    "db_id": self.table_linker.instances[iid]["db_id"],
                    "used_indices": result.get("used_indices", [])
                }
                
            json.dump(data, f, indent=2, ensure_ascii=False)

    def _process_single_instance(
        self,
        instance_id: str,
        instance_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Обрабатывает один инстанс: column linking → table linking → merge → save.
        """
        try:
            # Шаг 0: Проверка финального кэша
            final_cache = _load_cached_result(self.cache_dir, instance_id, "column_table_linking")
            if final_cache and final_cache.get("success"):
                self.logger.info(f"{instance_id} | Already processed (column_table cache hit)")
                return final_cache
            
            # Шаг 1: Column Linking (на полной схеме)
            column_result = _load_cached_result(self.cache_dir, instance_id, self.cache_prefix + "column_linking")
            
            if not column_result or not column_result.get("success", False):
                self.logger.info(f"{instance_id} | Running column linking (full schema)...")
                column_result = self.column_linker._process_single_instance(instance_id, instance_data)
                column_result_dict = column_result.to_dict() if hasattr(column_result, "to_dict") else column_result
                # _save_cached_result(self.cache_dir, instance_id, "column_linking", column_result_dict)
                self.logger.info(f"  Column result saved | Success: {column_result_dict.get('success', False)}")
            else:
                self.logger.info(f"{instance_id} | Column result loaded from cache")
                column_result_dict = column_result
            
            if not column_result_dict.get("success"):
                self.logger.warning(f"{instance_id} | Column linking failed, skipping table stage")
                # _save_cached_result(self.cache_dir, instance_id, "column_table_linking", {
                #     **column_result_dict,
                #     "tables_selected": [],
                #     "final_error": column_result_dict.get("final_error", "Column linking failed")
                # })
                return {
                    **column_result_dict,
                    "tables_selected": [],
                    "final_error": column_result_dict.get("final_error", "Column linking failed")
                }
            
            # Шаг 2: Извлечение таблиц из найденных колонок
            selected_columns = column_result_dict.get("columns_mapped", [])
            tables_from_columns = list(set(c["table_name"] for c in selected_columns if c.get("table_name")))
            
            if not tables_from_columns:
                self.logger.warning(f"{instance_id} | No tables extracted from columns")
                final_result = {
                    "instance_id": instance_id,
                    "db_id": column_result_dict.get("db_id"),
                    "tables_selected": [],
                    "columns_mapped": selected_columns,
                    "column_ids": column_result_dict.get("column_ids", []),
                    "blocking_issues": ["No tables could be inferred from selected columns"],
                    "success": False,
                    "final_error": "Empty table set from columns"
                }
                # _save_cached_result(self.cache_dir, instance_id, "column_table_linking", final_result)
                return final_result
            
            self.logger.info(f"{instance_id} | Running table linking with {len(tables_from_columns)} hinted tables...")
            
            # Шаг 3: Table Linking
            table_result = _load_cached_result(self.cache_dir, instance_id, self.cache_prefix + "table_linking")
            
            if not table_result or not table_result.get("success", False):
                table_result = self.table_linker._process_single_instance(instance_id, instance_data)
                table_result_dict = table_result.to_dict() if hasattr(table_result, "to_dict") else table_result
                # _save_cached_result(self.cache_dir, instance_id, "table_linking", table_result_dict)
            else:
                table_result_dict = table_result
            
            # Шаг 4: Слияние результатов
            final_result = self._merge_results_reverse(column_result_dict, table_result_dict)
            
            # Сохранение финального результата
            _save_cached_result(self.cache_dir, instance_id, "column_table_linking", final_result)
            
            self.logger.info(
                f"{instance_id} | Final result | Tables: {len(final_result['tables_selected'])} | "
                f"Columns: {len(final_result['columns_mapped'])} | Success: {final_result['success']}"
            )
            
            return final_result
            
        except Exception as e:
            self.logger.exception(f"✗ {instance_id} | Critical error in column_table linking")
            error_result = {
                "instance_id": instance_id,
                "success": False,
                "final_error": str(e),
                "tables_selected": [],
                "columns_mapped": [],
                "column_ids": [],
                "blocking_issues": [f"Critical error: {str(e)}"]
            }
            _save_cached_result(self.cache_dir, instance_id, "column_table_linking", error_result)
            return error_result

    def run(self) -> Dict[str, Any]:
        """Запускает пайплайн: column linking → table linking."""
        self.logger.info(f"Starting column→table linking pipeline | Run: {self.run_id}")
        
        instances = self.column_linker.instances  # Берём из column_linker, он загружает задачи первым
        self.logger.info(f"Loaded {len(instances)} instances for processing")
        
        stats = {"total": len(instances), "successful": 0, "failed": 0, "skipped": 0}
        results = {}
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._process_single_instance, iid, data): iid
                for iid, data in instances.items()
            }
            
            for future in tqdm(as_completed(futures), total=len(futures), desc="Column to Table Linking"):
                iid = futures[future]
                try:
                    result = future.result()
                    results[iid] = result
                    if result.get("success"):
                        stats["successful"] += 1
                    elif result.get("_from_cache"):
                        stats["skipped"] += 1
                    else:
                        stats["failed"] += 1
                except Exception as e:
                    stats["failed"] += 1
                    self.logger.exception(f"{iid} | Unhandled exception")
                    results[iid] = {"instance_id": iid, "success": False, "final_error": str(e)}
                except KeyboardInterrupt:
                    self.logger.warning("Interrupted by user, saving partial stats...")
                    executor.shutdown(wait=False, cancel_futures=True)
        
        stats["completed_at"] = time.time()
        stats_path = self.cache_dir / "column_table_linking_stats.json"
        with open(stats_path, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Column Table linking finished | Success: {stats['successful']}/{stats['total']}")
        return results


class BidirectionalSchemaLinking:
    """
    Двунаправленный пайплайн: TableColumnLinking + ColumnTableLinking.
    
    Алгоритм:
    1. Запускаем TableColumnLinking с cache_prefix="tc_"
    2. Запускаем ColumnTableLinking с cache_prefix="ct_"
    3. Объединяем used_indices из обоих результатов (уникальные)
    4. Сохраняем финальный результат в "bidirectional_results/"
    5. Удаляем промежуточные файлы кэша (опционально)
    """
    
    def __init__(
        self,
        run_id: str,
        model: BaseChatModel,
        tasks: Optional[List[Dict[str, Any]]] = None,
        run_root: str = "logs/runs",
        input_data_root: str = "Spider2/spider2-lite",
        data_root: str = "data",
        storage_root: str = "storage",
        prompt_dir: str = "config/prompts/schema_linking",
        max_workers: int = 4,
        # Общие параметры для обоих пайплайнов
        table_prompt_name: str = "sl_table_level",
        table_max_schema_length: int = 32000,
        max_tables: Optional[int] = None,
        column_prompt_name: str = "sl_column_level",
        column_max_schema_length: int = 64000,
        max_attempts: int = 4,
        max_columns: Optional[int] = None,
        retry_config: Optional[Dict[str, float]] = None,
        cache_prefix: Optional[str] = "",
        # Флаги управления
        cleanup_intermediate: bool = True,  # Удалять ли промежуточные файлы
        require_both_success: bool = False,  # Требовать успех обоих пайплайнов
    ):
        self.run_id = run_id
        self.cache_dir = Path(run_root) / run_id / "schema_linking"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.input_data_root = input_data_root
        self.data_root = Path(data_root)
        self.storage_root = storage_root
        self.max_workers = max_workers
        self.cache_prefix = cache_prefix
        self.cleanup_intermediate = cleanup_intermediate
        self.require_both_success = require_both_success

        self.model = model
        retry_config = DEFAULT_RETRY_CONFIG if retry_config is None else retry_config
        
        # 🔹 Инициализация пайплайнов с разными префиксами кэша
        self.tc_linker = TableColumnLinking(
            run_id, model, tasks,
            run_root=run_root, input_data_root=input_data_root, data_root=data_root, 
            prompt_dir=prompt_dir,
            max_workers=max_workers,
            table_prompt_name=table_prompt_name,
            table_max_schema_length=table_max_schema_length,
            table_max_attempts=max_attempts,
            max_tables=max_tables,
            column_prompt_name=column_prompt_name,
            column_max_schema_length=column_max_schema_length,
            column_max_attempts=max_attempts,
            max_columns=max_columns,
            retry_config=retry_config,
            cache_prefix="tc_"
        )
        
        self.ct_linker = ColumnTableLinking(
            run_id, model, tasks,
            run_root=run_root, input_data_root=input_data_root, data_root=data_root,
            prompt_dir=prompt_dir,
            max_workers=max_workers,
            column_prompt_name=column_prompt_name,
            column_max_schema_length=column_max_schema_length,
            column_max_attempts=max_attempts,
            max_columns=max_columns,
            table_prompt_name=table_prompt_name,
            table_max_schema_length=table_max_schema_length,
            table_max_attempts=max_attempts,
            max_tables=max_tables,
            retry_config=retry_config,
            cache_prefix="ct_"
        )
        
        self.logger = get_logger("bidirectional_linking", str(self.cache_dir / f"{self.cache_prefix}bidirectional_linking.log"))

    def _merge_bidir_results(
        self,
        instance_id: str,
        tc_result: Optional[Dict[str, Any]],
        ct_result: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Объединяет результаты двух пайплайнов: уникальные used_indices + метаданные."""
        # Собираем все used_indices из обоих результатов
        all_indices = set()
        
        if tc_result and tc_result.get("success"):
            all_indices.update(tc_result.get("used_indices", []))
        
        if ct_result and ct_result.get("success"):
            all_indices.update(ct_result.get("used_indices", []))
        
        # Определяем успех: по умолчанию — если хотя бы один пайплайн успешен
        success = (tc_result and tc_result.get("success")) or (ct_result and ct_result.get("success"))
        if self.require_both_success:
            success = bool(tc_result and tc_result.get("success") and 
                           ct_result and ct_result.get("success"))
        
        # Объединяем блокирующие проблемы
        blocking_issues = []
        if tc_result:
            blocking_issues.extend(tc_result.get("blocking_issues", []))
        if ct_result:
            blocking_issues.extend(ct_result.get("blocking_issues", []))
        
        # Метаданные
        metadata = {
            "tc_attempts": tc_result.get("metadata", {}).get("total_attempts", 0) if tc_result else 0,
            "ct_attempts": ct_result.get("metadata", {}).get("total_attempts", 0) if ct_result else 0,
            "tc_latency_ms": tc_result.get("metadata", {}).get("total_latency_ms", 0) if tc_result else 0,
            "ct_latency_ms": ct_result.get("metadata", {}).get("total_latency_ms", 0) if ct_result else 0,
            "pipeline_order": "bidirectional",
            "merged_from": [k for k, v in [("tc", tc_result), ("ct", ct_result)] if v and v.get("success")]
        }
        
        return {
            "instance_id": instance_id,
            "db_id": (tc_result or ct_result or {}).get("db_id"),
            "used_indices": list(all_indices),  # Уникальные, отсортированные
            "tables_selected": (tc_result or ct_result or {}).get("tables_selected", []),
            "columns_mapped": (tc_result or ct_result or {}).get("columns_mapped", []),
            "blocking_issues": list(set(blocking_issues)),  # Уникальные
            "success": success,
            "metadata": metadata
        }

    def extract_all_candidates(self):
        with open(self.cache_dir / f"{self.cache_prefix}bidirectional_linking_candidates.json", "w", encoding="utf-8") as f:
            data = {}
            for file in (self.cache_dir / f"{self.cache_prefix}bidirectional_linking_results").glob("*.json"):
                with open(file, "r", encoding="utf-8") as indf:
                    result = json.load(indf)
                
                iid = result["instance_id"]
                data[iid] = {
                    "db_id": self.tc_linker.table_linker.instances[iid]["db_id"],
                    "used_indices": result.get("used_indices", [])
                }
                
            json.dump(data, f, indent=2, ensure_ascii=False)        

    def _cleanup_intermediate_files(self):
        """Удаляет промежуточные файлы кэша."""
        if not self.cleanup_intermediate:
            return
        
        prefixes = ["tc_", "ct_"]
        stages = ["table_linking", "column_linking", "table_column_linking", "column_table_linking"]
        for prefix in prefixes:
            for stage in stages:
                for cache_file in [self.cache_dir / f"{prefix}{stage}.json", self.cache_dir / f"{stage}.json"]:
                    if cache_file.exists():
                        try:
                            cache_file.unlink()
                            self.logger.debug(f"Deleted intermediate: {cache_file.name}")
                        except Exception as e:
                            self.logger.warning(f"Failed to delete {cache_file}: {e}")

    def _process_single_instance(
        self,
        instance_id: str,
        instance_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Обрабатывает один инстанс: tc -> ct -> merge -> cleanup."""
        try:
            # Шаг 0: Проверка финального кэша
            final_cache = _load_cached_result(self.cache_dir, instance_id, f"{self.cache_prefix}bidirectional_linking")
            if final_cache and final_cache.get("success"):
                self.logger.info(f"{instance_id} | Already processed (bidir cache hit)")
                final_cache["_from_cache"] = True
                return final_cache
            
            # Шаг 1: Запуск TableColumnLinking (table → column)
            self.logger.info(f"{instance_id} | Running TableColumnLinking (tc_)...")
            tc_result = self.tc_linker._process_single_instance(instance_id, instance_data)
            tc_result_dict = tc_result.to_dict() if hasattr(tc_result, "to_dict") else tc_result
            
            # Шаг 2: Запуск ColumnTableLinking (column → table)
            self.logger.info(f"{instance_id} | Running ColumnTableLinking (ct_)...")
            ct_result = self.ct_linker._process_single_instance(instance_id, instance_data)
            ct_result_dict = ct_result.to_dict() if hasattr(ct_result, "to_dict") else ct_result
            
            # Шаг 3: Объединение результатов
            final_result = self._merge_bidir_results(instance_id, tc_result_dict, ct_result_dict)
            
            # Шаг 4: Сохранение финального результата
            _save_cached_result(self.cache_dir, instance_id, f"{self.cache_prefix}bidirectional_linking", final_result)
            
            # Шаг 5: Очистка промежуточных файлов (опционально)
            if self.cleanup_intermediate:
                self._cleanup_intermediate_files()
            
            self.logger.info(
                f"{instance_id} | Final bidir result | Indices: {len(final_result['used_indices'])} | "
                f"Success: {final_result['success']} | Sources: {final_result['metadata'].get('merged_from')}"
            )
            
            return final_result
            
        except Exception as e:
            self.logger.exception(f"{instance_id} | Critical error in bidirectional linking")
            error_result = {
                "instance_id": instance_id,
                "success": False,
                "final_error": str(e),
                "used_indices": [],
                "tables_selected": [],
                "columns_mapped": [],
                "blocking_issues": [f"Critical error: {str(e)}"],
                "metadata": {"pipeline_order": "bidirectional", "error_stage": "unknown"}
            }
            _save_cached_result(self.cache_dir, instance_id, f"{self.cache_prefix}bidirectional_linking", error_result)
            return error_result

    def run(self) -> Dict[str, Any]:
        """Запускает двунаправленный пайплайн."""
        self.logger.info(f"Starting bidirectional linking | Run: {self.run_id}")
        
        # Берём инстансы из одного из линкеров
        instances = self.tc_linker.table_linker.instances
        self.logger.info(f"Loaded {len(instances)} instances for processing")
        
        stats = {"total": len(instances), "successful": 0, "failed": 0, "skipped": 0}
        results = {}
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._process_single_instance, iid, data): iid
                for iid, data in instances.items()
            }
            
            for future in tqdm(as_completed(futures), total=len(futures), desc="Bidirectional Linking"):
                iid = futures[future]
                try:
                    result = future.result()
                    results[iid] = result
                    if result.get("success"):
                        stats["successful"] += 1
                    elif result.get("_from_cache"):
                        stats["skipped"] += 1
                    else:
                        stats["failed"] += 1
                except Exception as e:
                    stats["failed"] += 1
                    self.logger.exception(f"{iid} | Unhandled exception")
                    results[iid] = {"instance_id": iid, "success": False, "final_error": str(e)}
                except KeyboardInterrupt:
                    self.logger.warning("Interrupted by user, saving partial stats...")
                    executor.shutdown(wait=False, cancel_futures=True)
        
        stats["completed_at"] = time.time()
        stats_path = self.cache_dir / f"bidirectional_stats.json"
        with open(stats_path, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Bidirectional linking finished | Success: {stats['successful']}/{stats['total']}")
        return results


if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv(".env")
    
    parser = argparse.ArgumentParser(description="Sequential Table -> Column Schema Linking Pipeline")
    parser.add_argument("run_name", type=str, help="Name of the run (will be combined with input_data_root)")
    parser.add_argument("input-data-root", type=str, default="Spider2/spider2-lite")
    parser.add_argument("--data-root", type=str, default="data")
    parser.add_argument("--storage-root", type=str, default="storage")
    parser.add_argument("--run-root", type=str, default="logs/runs")
    parser.add_argument("--prompt-dir", type=str, default="config/prompts/schema_linking")
    
    # Model
    parser.add_argument("--model-name", type=str, default="qwen-local")
    parser.add_argument("--base-url", type=str, default=None)
    parser.add_argument("--api-key", type=str, default=None)
    parser.add_argument("--temperature", type=float, default=0.0)
    
    # Table linking params
    parser.add_argument("--table-prompt", type=str, default="sl_table_level")
    parser.add_argument("--table-max-schema-tokens", type=int, default=32000)
    parser.add_argument("--max-tables", type=int, default=None)
    
    # Column linking params
    parser.add_argument("--column-prompt", type=str, default="sl_column_level")
    parser.add_argument("--column-max-schema-tokens", type=int, default=64000)
    parser.add_argument("--max-columns", type=int, default=None)
    parser.add_argument("--max-attempts", type=int, default=4)
    
    # Pipeline
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument(
        "--pipeline", type=str, default="bidirectional",
        choices=["table_column", "column_table", "bidirectional"],
        help="Тип пайплайна: table->column, column->table, или двунаправленный"
    )
    parser.add_argument(
        "--cache-prefix", type=str, default="",
        help="Префикс для файлов кэша (по умолчанию: пустой)"
    )
    parser.add_argument(
        "--cleanup-intermediate", action="store_true",
        help="Удалять промежуточные файлы кэша после объединения"
    )
    
    args = parser.parse_args()
    
    run_id = resolve_run_id(input_data_root=args.input_data_root, custom_suffix=args.run_name)
    model = get_model(args.model_name, args.base_url, args.api_key, args.temperature)

    if args.pipeline == "table_column":
        table_column_linking = TableColumnLinking(
            run_id=run_id,
            model=model,
            run_root=args.run_root,
            input_data_root=args.input_data_root,
            data_root=args.data_root,
            storage_root=args.storage_root,
            prompt_dir=args.prompt_dir,
            max_workers=args.max_workers,
            table_prompt_name=args.table_prompt,
            table_max_schema_length=args.table_max_schema_tokens,
            table_max_attempts=args.max_attempts,
            max_tables=args.max_tables,
            column_prompt_name=args.column_prompt,
            column_max_schema_length=args.column_max_schema_tokens,
            column_max_attempts=args.max_attempts,
            max_columns=args.max_columns,
            cache_prefix=args.cache_prefix
        )
        table_column_linking.run()
        table_column_linking.extract_all_candidates()
    elif args.pipeline == "column_table":
        column_table_linking = ColumnTableLinking(
            run_id=run_id,
            model=model,
            run_root=args.run_root,
            input_data_root=args.input_data_root,
            data_root=args.data_root,
            storage_root=args.storage_root,
            prompt_dir=args.prompt_dir,
            max_workers=args.max_workers,
            table_prompt_name=args.table_prompt,
            table_max_schema_length=args.table_max_schema_tokens,
            table_max_attempts=args.max_attempts,
            max_tables=args.max_tables,
            column_prompt_name=args.column_prompt,
            column_max_schema_length=args.column_max_schema_tokens,
            column_max_attempts=args.max_attempts,
            max_columns=args.max_columns,
            cache_prefix=args.cache_prefix
        )
        column_table_linking.run()
        column_table_linking.extract_all_candidates()
    elif args.pipeline == "bidirectional":
        bidirectional_linking = BidirectionalSchemaLinking(
            run_id=run_id,
            model=model,
            run_root=args.run_root,
            input_data_root=args.input_data_root,
            data_root=args.data_root,
            storage_root=args.storage_root,
            prompt_dir=args.prompt_dir,
            max_workers=args.max_workers,
            table_prompt_name=args.table_prompt,
            table_max_schema_length=args.table_max_schema_tokens,
            table_max_attempts=args.max_attempts,
            max_tables=args.max_tables,
            column_prompt_name=args.column_prompt,
            column_max_schema_length=args.column_max_schema_tokens,
            column_max_attempts=args.max_attempts,
            max_columns=args.max_columns,
            cache_prefix=args.cache_prefix,
            cleanup_intermediate=args.cleanup_intermediate
        )
        bidirectional_linking.run()
        bidirectional_linking.extract_all_candidates()
