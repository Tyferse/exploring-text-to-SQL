import json
import time
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from .table_linking import TableLinking
from .column_linking import ColumnLinking
from src.utils.logger import get_logger
from src.utils.models import get_model
from src.utils.run_manager import resolve_run_id


def _compute_config_hash(**kwargs) -> str:
    """Вычисляет хэш конфигурации для инвалидации кэша при изменении параметров."""
    config_str = json.dumps(kwargs, sort_keys=True, default=str)
    return hashlib.md5(config_str.encode()).hexdigest()[:12]


def _load_cached_result(cache_dir: Path, instance_id: str, stage: str) -> Optional[Dict[str, Any]]:
    """Загружает результат этапа из кэша, если он есть и валиден."""
    cache_file = cache_dir / stage / f"{instance_id}.json"
    if not cache_file.exists():
        return None
    
    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Проверяем, что результат успешный и содержит нужные поля
        if data.get("success") and ("tables_selected" in data or "columns_mapped" in data):
            return data
    except (json.JSONDecodeError, KeyError):
        pass
    return None


def _save_cached_result(cache_dir: Path, instance_id: str, stage: str, result: Dict[str, Any]):
    """Сохраняет результат этапа в кэш."""
    cache_file = cache_dir / stage / f"{instance_id}.json"
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)


def _filter_columns_by_tables(
    all_columns: Dict[str, List[str]], 
    selected_tables: List[Dict[str, str]]
) -> Dict[str, List[str]]:
    """Фильтрует словарь колонок, оставляя только те, что принадлежат выбранным таблицам."""
    table_names = {t["table_name"] for t in selected_tables if t.get("table_name")}
    return {
        tn: cols for tn, cols in all_columns.items() 
        if tn in table_names
    }


def _merge_results(
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
        "column_ids": column_result.get("column_ids", []),
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


def _process_single_instance(
    instance_id: str,
    instance_data: Dict[str, Any],
    table_linker: TableLinking,
    column_linker: ColumnLinking,
    cache_dir: Path,
    config_hash: str,
    logger
) -> Dict[str, Any]:
    """
    Обрабатывает один инстанс: table linking → column linking → merge → save.
    Все промежуточные результаты пишутся на диск.
    """
    try:
        # ====================================================================
        # Шаг 0: Проверка финального кэша (если уже есть колонки — пропускаем)
        # ====================================================================
        final_cache = _load_cached_result(cache_dir, instance_id, "column_linking")
        if final_cache and final_cache.get("success"):
            logger.info(f"✓ {instance_id} | Already processed (column cache hit)")
            return final_cache
        
        # ====================================================================
        # Шаг 1: Table Linking (с кэшированием)
        # ====================================================================
        table_result = _load_cached_result(cache_dir, instance_id, "table_linking")
        
        if not table_result:
            logger.info(f"→ {instance_id} | Running table linking...")
            table_result = table_linker._process_single_instance(instance_id, instance_data)
            table_result_dict = table_result.to_dict() if hasattr(table_result, "to_dict") else table_result
            _save_cached_result(cache_dir, instance_id, "table_linking", table_result_dict)
            logger.info(f"  Table result saved | Success: {table_result_dict.get('success')}")
        else:
            logger.info(f"✓ {instance_id} | Table result loaded from cache")
            table_result_dict = table_result
        
        if not table_result_dict.get("success"):
            logger.warning(f"✗ {instance_id} | Table linking failed, skipping column stage")
            # Сохраняем неудачный результат как финальный, чтобы не пытаться снова
            _save_cached_result(cache_dir, instance_id, "column_linking", {
                **table_result_dict,
                "columns_mapped": [],
                "column_ids": [],
                "final_error": table_result_dict.get("final_error", "Table linking failed")
            })
            return table_result_dict
        
        # ====================================================================
        # Шаг 2: Подготовка фильтра для column linking
        # ====================================================================
        # Загружаем полную схему для этой БД (из колонк-линкера, чтобы не дублировать)
        db_id = instance_data.get("db_id", instance_id.split("_", 1)[0])
        full_schema = column_linker.schemas.get(db_id, {})
        
        if not full_schema:
            logger.warning(f"✗ {instance_id} | Schema not found for db_id: {db_id}")
            return {"instance_id": instance_id, "success": False, "final_error": "Schema missing"}
        
        # Преобразуем схему в формат {table_name: [column_names]}
        all_columns = {
            tn: [meta["column_name"] for meta in cols.values()]
            for tn, cols in full_schema.items()
        }
        
        # Фильтруем: оставляем только колонки из выбранных таблиц
        selected_tables = table_result_dict.get("tables_selected", [])
        filtered_columns = _filter_columns_by_tables(all_columns, selected_tables)
        
        if not filtered_columns:
            logger.warning(f"✗ {instance_id} | No columns after filtering by selected tables")
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
            _save_cached_result(cache_dir, instance_id, "column_linking", final_result)
            return final_result
        
        # ====================================================================
        # Шаг 3: Column Linking (на отфильтрованной схеме)
        # ====================================================================
        logger.info(f"→ {instance_id} | Running column linking on {len(filtered_columns)} tables...")
        
        # Подготавливаем данные для column_linker._process_single_instance
        column_data = {
            **instance_data,
            "available_ids": None,  # Не фильтруем по ID, полагаемся на filtered_columns
            # Передаём отфильтрованные колонки через временный атрибут или модификацию
        }
        
        # Хак: временно заменяем схему в column_linker на отфильтрованную
        # (в продакшене лучше передать через аргумент, но это требует изменения сигнатуры)
        original_schema = column_linker.schemas.get(db_id)
        if original_schema:
            # Создаём временную схему только с нужными таблицами
            column_linker.schemas[db_id] = {
                tn: cols for tn, cols in original_schema.items() 
                if tn in filtered_columns
            }
        
        try:
            column_result = column_linker._process_single_instance(instance_id, column_data)
            column_result_dict = column_result.to_dict() if hasattr(column_result, "to_dict") else column_result
        finally:
            # Восстанавливаем оригинальную схему
            if original_schema:
                column_linker.schemas[db_id] = original_schema
        
        _save_cached_result(cache_dir, instance_id, "column_linking", column_result_dict)
        logger.info(f"  Column result saved | Success: {column_result_dict.get('success')}")
        
        # ====================================================================
        # Шаг 4: Слияние результатов и сохранение финального артефакта
        # ====================================================================
        final_result = _merge_results(table_result_dict, column_result_dict)
        
        # Перезаписываем кэш column_linking финальным слитым результатом
        _save_cached_result(cache_dir, instance_id, "column_linking", final_result)
        
        logger.info(
            f"✓ {instance_id} | Final result | Tables: {len(final_result['tables_selected'])} | "
            f"Columns: {len(final_result['columns_mapped'])} | Success: {final_result['success']}"
        )
        
        return final_result
        
    except Exception as e:
        logger.exception(f"✗ {instance_id} | Critical error in double linking")
        error_result = {
            "instance_id": instance_id,
            "success": False,
            "final_error": str(e),
            "tables_selected": [],
            "columns_mapped": [],
            "column_ids": [],
            "blocking_issues": [f"Critical error: {str(e)}"]
        }
        _save_cached_result(cache_dir, instance_id, "column_linking", error_result)
        return error_result


def run_double_linking(
    run_id: str,
    model_name: str,
    run_root: str = "logs/runs",
    input_data_root: str = "Spider2/spider2-lite",
    data_root: str = "data",
    storage_root: str = "storage",
    prompt_dir: str = "config/prompts/schema_linking",
    max_workers: int = 4,
    # Параметры table linking
    table_prompt_name: str = "sl_table_level",
    table_max_schema_length: int = 8000,
    table_max_attempts: int = 4,
    # Параметры column linking
    column_prompt_name: str = "sl_column_level",
    column_max_schema_length: int = 32000,
    column_max_attempts: int = 4,
    column_max_columns: Optional[int] = None,
    # Общие параметры
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    temperature: float = 0.0,
    retry_config: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    """
    Запускает последовательный пайплайн: table linking → column linking.
    
    Returns:
        Статистика выполнения.
    """
    run_path = Path(run_root) / run_id
    cache_dir = run_path / "schema_linking"
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    logger = get_logger("double_linking", str(cache_dir / "double_linking.log"))
    logger.info(f"🚀 Starting double linking pipeline | Run: {run_id}")
    
    # Вычисляем хэш конфигурации для инвалидации кэша
    config_hash = _compute_config_hash(
        table_prompt_name=table_prompt_name,
        table_max_schema_length=table_max_schema_length,
        column_prompt_name=column_prompt_name,
        column_max_schema_length=column_max_schema_length,
        model_name=model_name,
        temperature=temperature,
        retry_config=retry_config
    )
    logger.info(f"Config hash: {config_hash}")
    
    # Инициализация модели
    model = get_model(model_name, base_url, api_key, temperature)
    
    # Инициализация table linker
    table_linker = TableLinking(
        run_id=run_id,
        model=model,
        tasks=None,  # Загрузит из файлов
        run_root=run_root,
        input_data_root=input_data_root,
        data_root=data_root,
        storage_root=storage_root,
        prompt_name=table_prompt_name,
        prompt_dir=prompt_dir,
        max_schema_length=table_max_schema_length,
        retry_config=retry_config,
        max_workers=1,  # Не используем параллелизм внутри, т.к. параллелим на уровне run_double_linking
    )
    
    # Инициализация column linker
    column_linker = ColumnLinking(
        run_id=run_id,
        model=model,
        tasks=None,
        run_root=run_root,
        input_data_root=input_data_root,
        data_root=data_root,
        storage_root=storage_root,
        prompt_name=column_prompt_name,
        prompt_dir=prompt_dir,
        max_schema_length=column_max_schema_length,
        retry_config=retry_config,
        max_workers=1,
        max_columns=column_max_columns,
    )
    
    # Загрузка инстансов (берём из column_linker, т.к. он загружает все задачи)
    instances = column_linker.instances
    logger.info(f"Loaded {len(instances)} instances for processing")
    
    # Статистика
    stats = {"total": len(instances), "successful": 0, "failed": 0, "skipped": 0}
    results = {}
    
    # Параллельная обработка инстансов
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _process_single_instance,
                iid,
                data,
                table_linker,
                column_linker,
                cache_dir,
                config_hash,
                logger
            ): iid
            for iid, data in instances.items()
        }
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Double Linking"):
            iid = futures[future]
            try:
                result = future.result()
                results[iid] = result
                if result.get("success"):
                    stats["successful"] += 1
                elif result.get("final_error") == "Already processed":
                    stats["skipped"] += 1
                else:
                    stats["failed"] += 1
            except Exception as e:
                stats["failed"] += 1
                logger.exception(f"✗ {iid} | Unhandled exception")
                results[iid] = {"instance_id": iid, "success": False, "final_error": str(e)}
    
    # Сохранение статистики
    stats["completed_at"] = time.time()
    stats_path = cache_dir / "double_linking_stats.json"
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    
    logger.info(f"✅ Double linking finished | Success: {stats['successful']}/{stats['total']}")
    logger.info(f"📊 Stats saved to: {stats_path}")
    
    return stats


if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="Sequential Table → Column Schema Linking Pipeline")
    parser.add_argument("run_name", type=str, help="Name of the run (will be combined with input_data_root)")
    parser.add_argument("--input-data-root", type=str, default="Spider2/spider2-lite")
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
    parser.add_argument("--table-max-schema-tokens", type=int, default=8000)
    parser.add_argument("--table-max-attempts", type=int, default=4)
    
    # Column linking params
    parser.add_argument("--column-prompt", type=str, default="sl_column_level")
    parser.add_argument("--column-max-schema-tokens", type=int, default=32000)
    parser.add_argument("--column-max-attempts", type=int, default=4)
    parser.add_argument("--column-max-columns", type=int, default=None)
    
    # Pipeline
    parser.add_argument("--max-workers", type=int, default=4)
    
    args = parser.parse_args()
    
    run_id = resolve_run_id(input_data_root=args.input_data_root, custom_suffix=args.run_name)
    
    stats = run_double_linking(
        run_id=run_id,
        model_name=args.model_name,
        run_root=args.run_root,
        input_data_root=args.input_data_root,
        data_root=args.data_root,
        storage_root=args.storage_root,
        prompt_dir=args.prompt_dir,
        max_workers=args.max_workers,
        table_prompt_name=args.table_prompt,
        table_max_schema_length=args.table_max_schema_tokens,
        table_max_attempts=args.table_max_attempts,
        column_prompt_name=args.column_prompt,
        column_max_schema_length=args.column_max_schema_tokens,
        column_max_attempts=args.column_max_attempts,
        column_max_columns=args.column_max_columns,
        base_url=args.base_url,
        api_key=args.api_key,
        temperature=args.temperature,
    )
