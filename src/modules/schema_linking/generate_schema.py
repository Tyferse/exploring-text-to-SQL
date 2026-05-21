import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Literal
from tqdm import tqdm

from src.utils.logger import get_logger
from src.utils.preprocessing import remove_digits
from src.utils.run_manager import resolve_run_id


def estimate_prompt_length(text: str, chars_per_token: float = 4.0) -> int:
    """
    Приблизительная оценка количества токенов по длине строки.
    
    Args:
        text: Текст промпта
        chars_per_token: Среднее количество символов на токен (эмпирически ~3.5-4.5 для смешанного текста)
        
    Returns:
        Примерное количество токенов
    """
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

def compress_schema_to_fit(
    table_mapping: Dict[str, List[Dict[str, Any]]],
    target_max_tokens: int,
    chars_per_token: float = 3.0,
    min_columns: int = 1
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
        
    Returns:
        Tuple[сжатое_отображение, список_применённых_стратегий]
    """
    strategies_applied = []
    current_mapping = {tbl: [col.copy() for col in cols] for tbl, cols in table_mapping.items()}
    
    # Вспомогательная функция для оценки
    def current_length(mapping: Dict) -> int:
        schema_text = "".join(format_schema_block(tbl, cols) for tbl, cols in mapping.items())
        return estimate_prompt_length(schema_text, chars_per_token)
    
    # Этап 1: Удаление примеров значений
    if current_length(current_mapping) > target_max_tokens:
        current_mapping = {tbl: remove_sample_values(cols) for tbl, cols in current_mapping.items()}
        strategies_applied.append("removed_sample_values")
    
    # Этап 2: Удаление descriptions
    if current_length(current_mapping) > target_max_tokens:
        current_mapping = {tbl: remove_descriptions(cols) for tbl, cols in current_mapping.items()}
        strategies_applied.append("removed_descriptions")
    
    # Этап 3: Итеративное ограничение колонок
    if current_length(current_mapping) > target_max_tokens:
        # Находим максимальное количество колонок в любой таблице
        max_cols = max(len(cols) for cols in current_mapping.values()) if current_mapping else 0
        
        # Бинарный поиск оптимального k
        low, high = min_columns, max_cols
        best_mapping = current_mapping
        
        while low <= high:
            mid = (low + high) // 2
            trial_mapping = limit_columns_per_table(current_mapping, mid)
            
            if estimate_prompt_length(
                "".join(format_schema_block(tbl, cols) for tbl, cols in trial_mapping.items()),
                chars_per_token
            ) <= target_max_tokens:
                best_mapping = trial_mapping
                high = mid - 1  # Пробуем ещё сильнее сжать
            else:
                low = mid + 1   # Нужно оставить больше колонок
        
        if best_mapping != current_mapping:
            current_mapping = best_mapping
            strategies_applied.append(f"limited_columns_to_k={low}")
    
    if current_length(current_mapping) > target_max_tokens:
        current_mapping = limit_columns_per_table(current_mapping, min_columns)
        if not any(s.startswith("limited_columns_to_k=") for s in strategies_applied):
            strategies_applied.append(f"forced_min_columns_{min_columns}")
    
    return current_mapping, strategies_applied

def truncate_value(val: Any, max_len: int = 250) -> str:
    """Безопасное приведение значения к строке с обрезкой длинных значений."""
    s = str(val).replace("\n", " ").replace("\r", " ")
    if len(s) > max_len:
        return s[:max_len] + "...(truncated)"
    return s

def process_column_values(values: List[Any], max_samples: int = 3, max_len: int = 250) -> List[str]:
    """
    Извлекает до max_samples примеров, сериализует сложные типы (dict/list),
    обрезает длинные строки.
    """
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
        result.append(truncate_value(v_str, max_len))
    return result

def format_schema_block(table_name: str, columns: List[Dict[str, Any]]) -> str:
    """Формирует текстовый блок схемы по заданному шаблону."""
    lines = [f"###Table full name: {table_name}", "["]
    
    for col in columns:
        col_name = col.get("column_name", "unknown")
        col_type = col.get("data_type", "TEXT")
        desc = col.get("description", "")
        samples = process_column_values(col.get("sample_values", []))
        samples_str = f"Sample values: {samples}" if samples else ""
        desc_str = f"Description: {desc}" if desc else ""
        
        parts = [col_name, f"Type: {col_type}", samples_str, desc_str]
        content = "; ".join(p for p in parts[1:] if p)
        lines.append(f"\t{parts[0]} ({content})")
        
    lines.append("]")
    lines.append("-" * 50)
    lines.append("")
    return "\n".join(lines)

def process_single_instance(
    instance_id: str,
    col_ids: List[str],
    doc_data: Dict[int, Dict[str, Any]],
    output_path: Path,
    target_max_tokens: int = 64_000,
    log: Optional[logging.Logger] = None
) -> Tuple[bool, Dict[str, Any]]:
    """
    Обрабатывает один пример: строит текстовое описание схемы с учётом лимита контекста.
    
    Args:
        instance_id: Уникальный идентификатор примера
        col_ids: Список ID столбцов для включения в схему
        doc_data: Загруженные словари {db_id: {col_id: col_info}}
        output_dir: Директория для сохранения .txt файлов
        target_max_tokens: Максимальная длина описания схемы в токенах.
        log: Опциональный логгер. Если None, создаётся автоматически.
        
    Returns:
        Tuple[успех: bool, метаданные: dict]
    """
    if log is None:
        log = get_logger(output_path.parent / "gen_schema" / (instance_id + ".log"))

    log.info(f"Begin processing | {instance_id}")
    metadata = {"instance_id": instance_id, "strategies_applied": [], "final_token_estimate": 0}

    if not col_ids:
        log.warning(f"List col_ids is empty. Skip. | {instance_id}")
        return False, metadata

    try:
        log.info(f"Target: {target_max_tokens} tokens | {instance_id}")
        table_mapping: Dict[str, List[Dict[str, Any]]] = {}

        # 1. Сбор данных по столбцам
        valid_columns_found = 0
        for cid in col_ids:
            col_info = doc_data.get(cid)
            if not col_info:
                log.warning(f"Column with id={cid} doesn't exist | {instance_id}")
                continue

            table_name = col_info.get("table_name", "unknown_table")
            if table_name not in table_mapping:
                table_mapping[table_name] = []

            desc = col_info.get("text", "")
            desc = desc.split("Description: ", 1)[1] if desc else ""
            table_mapping[table_name].append({
                "column_name": col_info["metadata"].get("column_name", cid),
                "data_type": col_info["metadata"].get("data_type", "TEXT"),
                "description": desc,
                "sample_values": col_info["metadata"].get("sample_values", [])
            })
            valid_columns_found += 1

        if valid_columns_found == 0:
            log.warning(f"Valid columns have not been found in metadata | {instance_id}")
            return False, metadata

        # 2. Сжатие схемы до целевого размера
        compressed_mapping, strategies = compress_schema_to_fit(
            table_mapping=table_mapping,
            target_max_tokens=target_max_tokens
        )
        metadata["strategies_applied"] = strategies
        if strategies:
            log.info(f"Применены стратегии сжатия: {strategies}")

        # 2. Форматирование и финальная оценка
        schema_text = "".join(
            format_schema_block(tbl, cols) for tbl, cols in compressed_mapping.items()
        )
        final_tokens = estimate_prompt_length(schema_text)
        metadata["final_token_estimate"] = final_tokens
        
        if final_tokens > target_max_tokens:
            log.warning(f"Scheme is still over limit: {final_tokens} > {target_max_tokens} | {instance_id}")
        else:
            log.info(f"Schema is in limit: {final_tokens}/{target_max_tokens} tokens | {instance_id}")
        
        tables_count = len(compressed_mapping)
        cols_count = sum(len(cols) for cols in compressed_mapping.values())
        log.info(f"Tables: {tables_count} | Columns: {cols_count} | tokens: {final_tokens} | {instance_id}")
        
        return schema_text, metadata

    except Exception as e:
        log.exception(f"Critical processing error {instance_id}: {e}")
        metadata["error"] = str(e)
        return "", metadata

def generate_schemas(
    run_id: str,
    run_root: str = "logs/runs",
    data_root: str = "data",
    input_data_root: str = "Spider2/spider2-lite",
    output_dir: str = "initial_schema",
    docs_path: Optional[str] = None,
    included: Literal["retrieved", "full"] = "full",
    target_max_tokens: int = 64_000
):
    """
    Читает used_indices.json, загружает *_docs.json, 
    итерирует по примерам и вызывает process_single_instance с контролем контекста.
    """
    run_path = Path(run_root) / run_id
    output_path = run_path / "schema_linking" / output_dir
    log_dir = run_path / "schema_linking"

    # Загрузка документов схем
    docs_path = Path(docs_path)
    db_docs: Dict[str, Dict[str, Any]] = {}
    
    for doc_file in docs_path.glob("*_docs.json"):
        db_id = doc_file.stem.replace("_docs", "")
        with open(doc_file, "r", encoding="utf-8") as f:
            docs_data = json.load(f)
            db_docs[db_id] = {col["id"]: {key: col[key] for key in col if key != "id"} for col in docs_data}
    
    if not db_docs:
        raise FileNotFoundError(f"*_docs.json files not found in " + str(docs_path))

    if included == "retrieved":
        indices_path = run_path / "schema_linking" / "retrieval_cache" / "used_indices.json"
        if not indices_path.exists():
            raise FileNotFoundError(f"used_indices.json not found: {indices_path}")

        # Загрузка индексов
        with open(indices_path, "r", encoding="utf-8") as f:
            indices_data = json.load(f)

    elif included == "full":
        tasks_file = [file for file in os.listdir(os.path(data_root, input_data_root)) 
                      if file.endswith('.jsonl')][0]
        with open(Path(data_root) / input_data_root / tasks_file, encoding="utf-8") as f:
            tasks = [json.loads(line.strip()) for line in f.readlines()]

        if input_data_root == "Spider2/spider2-lite":
            inst2dialect = {"sf": "snowflake", "bq": "bigquery", "ga": "bigquery", "local": "sqlite"}
            tasks = [(instance["instance_id"], 
                      inst2dialect[remove_digits(instance["instance_id"]).split("_")[0]] + "_" + instance["db_id"])
                     for instance in tasks]
        else:
            tasks = [(instance["instance_id"], 
                      instance.get("dialect", "") + ("_" if instance.get("dialect") else "") + instance["db_id"])
                     for instance in tasks]
        
        indices_data = {instance_id: {"db_id": db_id, "used_indices": list(db_docs[db_id].keys())} 
                        for instance_id, db_id in tasks}

    # Подготовка директорий
    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Главный логгер
    main_logger = get_logger("schema_generation", str(log_dir / "schema_gen.log"))
    main_logger.info(
        f"Launch schema generation | Max tokens: {target_max_tokens} | Instances: {len(indices_data)}"
    )

    # Цикл обработки
    stats = {"success": 0, "failed": 0, "strategies_count": {}, "token_stats": []}
    
    for instance_id, inst_retrival_data in tqdm(indices_data.items(), desc="Schema generation"):    
        schema_text, meta = process_single_instance(
            instance_id=instance_id,
            col_ids=inst_retrival_data["used_indices"],
            doc_data=db_docs[inst_retrival_data["db_id"]],
            output_dir=output_path,
            target_max_tokens=target_max_tokens,
            log=main_logger
        )
        
        if schema_text:
            output_file = output_path / f"{instance_id}.txt"
            output_file.write_text(schema_text, encoding="utf-8")
            main_logger.info(f"Schema saved: {output_file.name} | {instance_id}")

            stats["success"] += 1
            # Сбор статистики по стратегиям сжатия
            for strategy in meta.get("strategies_applied", []):
                stats["strategies_count"][strategy] = stats["strategies_count"].get(strategy, 0) + 1

            stats["token_stats"].append(meta.get("final_token_estimate", 0))
        else:
            stats["failed"] += 1

    # Итоговая статистика
    if stats["token_stats"]:
        stats["token_avg"] = sum(stats["token_stats"]) / len(stats["token_stats"])
        stats["token_max"] = max(stats["token_stats"])
        stats["token_min"] = min(stats["token_stats"])
    
    main_logger.info(
        f"Generation has finished | Successfull: {stats['success']}/{len(indices_data)} | "
        f"Average tokens: {stats.get('token_avg', 0):.0f} | "
        f"Stratages: {stats['strategies_count']}"
    )
    
    # Сохранение статистики
    with open(str(log_dir / "schema_stats.json"), "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    
    return stats


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Генерация описаний схем для schema linking")
    parser.add_argument(
        "input_data_root", type=str, default="Spider2/spider2-lite",
        help="Относительный путь к папке датасета внутри data_root. "
             "Используется для получения вопросов."
    )
    parser.add_argument(
        "run_name", type=str, default="", 
        help="Название запуска, использовавшегося для формирования логов в logs/runs директории."
    )
    parser.add_argument(
        "--data_root", type=str, default="data",
        help="Путь к папке с входными данными"
    )
    parser.add_argument(
        "--storage_root", type=str, default="storage",
        help="Корневая директория для кэшированных схем и векторных баз данных."
    )
    parser.add_argument(
        "--output_dir", type=str, default="initial_schema",
        help="Директория, в которой будут сохранены представления схем БД."
    )
    parser.add_argument(
        "--max_tokens", type=int, default=64000,
        help="Максимальное длина схемы базы данных в токенах (оценочно)."
    )
    parser.add_argument(
        "--full_schema", type=bool, action="store_true",
        help="Использовать полную схему."
    )
    args = parser.parse_args()

    run_id = resolve_run_id(input_data_root=args.input_data_root, custom_suffix=args.run_name)
    generate_schemas(
        run_id, data_root=args.data_root, input_data_root=args.input_data_root, 
        output_dir=args.output_dir, docs_dir=str(Path(args.storage_root) / args.input_data_dir), 
        included="full" in args.full_schema else "retrieved", target_max_tokens=args.max_tokens
    )
