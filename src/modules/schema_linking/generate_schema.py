import os
import json
import logging
import random
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Literal, Callable

from tqdm import tqdm

from .schema_formatter import format_detailed_block, compress_schema_to_fit, load_similar_tables, estimate_prompt_length
from src.utils.logger import get_logger
from src.utils.preprocessing import remove_digits
from src.utils.run_manager import resolve_run_id


def generate_single_schema(
    instance_id: str,
    col_ids: List[str],
    doc_data: Dict[int, Dict[str, Any]],
    format_schema_block: Callable = format_detailed_block,
    target_max_tokens: int = 64_000,
    similar_tables: Optional[Dict[str, Dict[str, List[str]]]] = None,
    chars_per_token: float = 4.0,
    output_path: Optional[Path] = None,
    log: Optional[logging.Logger] = None,
    **formatter_kwargs
) -> Tuple[str, Dict[str, Any]]:
    """
    Обрабатывает один пример: строит текстовое описание схемы с учётом лимита контекста.
    
    Args:
        instance_id: Уникальный идентификатор примера
        db_id; Идентификатор базы данных
        col_ids: Список ID столбцов для включения в схему
        doc_data: Загруженный словарь {col_id: col_info}
        target_max_tokens: Максимальная длина описания схемы в токенах.
        similar_tables: Словарь отображения таблиц в список похожих таблиц
        output_path: Директория для сохранения .txt файлов
        log: Опциональный логгер. Если None, создаётся автоматически.
        
    Returns:
        Tuple[текст схемы: str, метаданные: dict]
    """
    if log is None and output_path is not None:
        log = get_logger("gen_single_schema", output_path.parent / "gen_schema" / (instance_id + ".log"))

    if log: log.info(f"Begin processing | {instance_id}")
    metadata = {"instance_id": instance_id, "strategies_applied": [], "final_token_estimate": 0}

    if not col_ids:
        if log: log.warning(f"List col_ids is empty. Skip. | {instance_id}")
        return False, metadata

    try:
        if log: log.info(f"Target: {target_max_tokens} tokens | {instance_id}")
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

            colmeta = col_info["metadata"]
            table_mapping[table_name].append({
                "column_name": colmeta.get("column_name", cid),
                "data_type": colmeta.get("data_type", "TEXT"),
                "description": desc,
                "sample_values": colmeta.get("sample_values", colmeta.get("column_vals", []))
            })
            valid_columns_found += 1

        if valid_columns_found == 0:
            if log: log.warning(f"Valid columns have not been found in metadata | {instance_id}")
            return False, metadata

        # 2. Сжатие схемы до целевого размера
        compressed_mapping, strategies = compress_schema_to_fit(
            table_mapping=table_mapping,
            block_formatter=format_schema_block,
            target_max_tokens=target_max_tokens,
            similar_tables=similar_tables,
            **formatter_kwargs
        )
        metadata["strategies_applied"] = strategies
        if strategies:
            if log: log.info(f"Применены стратегии сжатия: {strategies}")

        # 2. Форматирование и финальная оценка
        schema_blocks = [
            format_schema_block(tbl, cols, similar_tables.get(tbl, {}), **formatter_kwargs) 
            for tbl, cols in compressed_mapping.items()
        ]
        random.shuffle(schema_blocks)  # Перемешиваем для предотвращения влияния порядка таблиц
        schema_text = "".join(schema_blocks)
        final_tokens = estimate_prompt_length(schema_text, chars_per_token)
        metadata["final_token_estimate"] = final_tokens
        
        if final_tokens > target_max_tokens:
            if log: log.warning(f"Scheme is still over limit: {final_tokens} > {target_max_tokens} | {instance_id}")
        else:
            if log: log.info(f"Schema is in limit: {final_tokens}/{target_max_tokens} tokens | {instance_id}")
        
        tables_count = len(compressed_mapping)
        cols_count = sum(len(cols) for cols in compressed_mapping.values())
        if log: log.info(f"Tables: {tables_count} | Columns: {cols_count} | tokens: {final_tokens} | {instance_id}")
        
        return schema_text, metadata

    except Exception as e:
        if log: log.exception(f"Critical processing error {instance_id}: {e}")
        metadata["error"] = str(e)
        return "", metadata

def generate_schemas(
    run_id: str,
    tasks: Optional[Dict[str, Any]] = None,
    run_root: str = "logs/runs",
    data_root: str = "data",
    input_data_root: str = "Spider2/spider2-lite",
    output_dir: str = "initial_schema",
    docs_path: Optional[str] = None,
    included: Literal["retrieved", "tables", "full"] = "full",
    target_max_tokens: int = 64_000
):
    """
    Читает *_indices.json, загружает *_docs.json, 
    итерирует по примерам и вызывает process_single_instance с контролем контекста.
    """
    run_path = Path(run_root) / run_id
    log_dir = run_path / "schema_linking"
    output_path = log_dir / output_dir
    schema_tasks = deepcopy(tasks) if tasks is not None else None

    # Загрузка документов схем
    docs_path = Path(docs_path)
    db_docs: Dict[str, Dict[str, Any]] = {}
    similar_tables: Dict[str, Dict[str, List[str]]] = {}

    for doc_file in docs_path.glob("*_docs.json"):
        db_id = doc_file.stem.replace("_docs", "")
        with open(doc_file, "r", encoding="utf-8") as f:
            docs_data = json.load(f)
            db_docs[db_id] = {col["id"]: {key: col[key] for key in col if key != "id"} for col in docs_data}

    similar_tables = load_similar_tables(docs_path)    
    
    if not db_docs:
        raise FileNotFoundError(f"*_docs.json files not found in " + str(docs_path))

    # Загружаем примеры
    if schema_tasks is None:
        tasks_file = [file for file in os.listdir(os.path(data_root, input_data_root)) 
                      if file.endswith('.jsonl')][0]
        with open(Path(data_root) / input_data_root / tasks_file, encoding="utf-8") as f:
            schema_tasks = [json.loads(line.strip()) for line in f.readlines()]
    
    if input_data_root == "Spider2/spider2-lite":
        inst2dialect = {"sf": "snowflake", "bq": "bigquery", "ga": "bigquery", "local": "sqlite"}
        schema_tasks = [(instance["instance_id"], 
                         inst2dialect[remove_digits(instance["instance_id"]).split("_")[0]] + "_" + instance["db_id"])
                        for instance in schema_tasks]
    else:
        schema_tasks = [(instance["instance_id"], 
                         instance.get("dialect", "") + ("_" if instance.get("dialect") else "") + instance["db_id"])
                        for instance in schema_tasks]

    if included == "retrieved":
        indices_data = {}
        # Получаем начальные id
        indices_path = run_path / "schema_linking" / "retrieved_indices.json"
        if indices_path.exists():
            # Загрузка индексов 
            with open(indices_path, "r", encoding="utf-8") as f:
                indices_data = json.load(f)

        if not indices_data:
            indices_data = {iid: {"db_id": db_id, "used_indices": []} 
                            for iid, db_id in schema_tasks}

        # Добавляем id, найденные в результате работы агента
        candidates_file = run_path / "schema_linking" / "agent_candidates.json"
        if candidates_file.exists():
            with open(candidates_file, "r", encoding="utf-8") as f:
                all_candidates = json.load(f)

            for instance_id in all_candidates:
                indices_data[instance_id]["used_indices"] = list(set(
                    indices_data[instance_id]["used_indices"] 
                    + all_candidates[instance_id]["used_indices"]
                ))

    elif included == "tables":
        # Получаем начальные id
        tables_path = run_path / "schema_linking" / "table_candidates.json"
        if not tables_path.exists():
            raise FileNotFoundError(f"table_candidates.json not found: {indices_path}")

        # Загрузка индексов
        with open(tables_path, "r", encoding="utf-8") as f:
            tables_data = json.load(f)

        indices_data = {iid: {
                "db_id": tables_data[iid]["db_id"], 
                "used_indices": [
                    cid for cid in db_docs[tables_data["db_id"]] 
                    if db_docs[tables_data[iid]["db_id"]][cid]["metadata"]["table_name"] in tables_data[iid]["used_tables"]
                ]
            } 
            for iid in tables_data
        }
    
    elif included == "selected":
        indices_path_list = (run_path / "schema_linking").glob("*_indices.json")
        indices_path_list += (run_path / "schema_linking").glob("*_candidates.json") 
        indices_data = {}
        for file in indices_path_list:
            with open(file, "r", encoding="utf-8") as f:
                partial_data = json.load(f)

            if file.endswith("table_candidates.json"):
                partial_data = {
                    iid: {
                        "db_id": partial_data[iid]["db_id"], 
                        "used_indices": [
                            cid for cid in db_docs[partial_data[iid]["db_id"]] 
                            if db_docs[partial_data[iid]["db_id"]][cid]["metadata"]["table_name"] in partial_data[iid]["used_tables"]
                        ]
                    }
                    for iid in partial_data
                }
            
            for instance_id in partial_data:
                if instance_id in indices_data:
                    indices_data[instance_id]["used_indices"] = list(set(
                        indices_data[instance_id]["used_indices"] 
                        + partial_data[instance_id]["used_indices"]
                    ))
                else:
                    indices_data[instance_id]["used_indices"] = list(set(
                        partial_data[instance_id]["used_indices"]
                    ))

    elif included == "full":
        indices_data = {instance_id: {"db_id": db_id, "used_indices": list(db_docs[db_id].keys())} 
                        for instance_id, db_id in schema_tasks}

    # Подготовка директорий
    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Главный логгер
    main_logger = get_logger("schema_generation", str(log_dir / "schema_gen.log"))
    main_logger.info(
        f"Launch schema generation | Max tokens: {target_max_tokens} | Instances: {len(indices_data)}"
    )

    stats = {"success": 0, "failed": 0, "strategies_count": {}, "token_stats": []}
    
    for instance_id, inst_retrival_data in tqdm(indices_data.items(), desc="Schema generation"):    
        schema_text, meta = generate_single_schema(
            instance_id=instance_id,
            col_ids=inst_retrival_data["used_indices"],
            doc_data=db_docs[inst_retrival_data["db_id"]],
            similar_tables=similar_tables[inst_retrival_data["db_id"]],
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
        "--included", type=bool, action="store_true",
        help="""Режим загрузки индексов столбцов: 
        retrieved - столбцы, найденные через векторный поиск и\или итоговые из all_columns.json;
        tables - найденные таблицы, для которых используются все их столбцы;
        selected - объединение результатов поиска во всех *_indices.json и *_candidates.json файлах
        full - используются все столбцы."""
    )
    args = parser.parse_args()

    run_id = resolve_run_id(input_data_root=args.input_data_root, custom_suffix=args.run_name)
    generate_schemas(
        run_id, data_root=args.data_root, input_data_root=args.input_data_root, 
        output_dir=args.output_dir, docs_dir=str(Path(args.storage_root) / args.input_data_dir / "schema_cache"), 
        included=args.included, target_max_tokens=args.max_tokens
    )
