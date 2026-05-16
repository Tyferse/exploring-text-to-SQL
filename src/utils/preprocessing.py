import argparse
import hashlib
import json
import logging
import os
import re
from copy import deepcopy
from typing import Dict, List, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.utils.logger import get_logger


def remove_digits(text: str) -> str:
    """Удаляет все цифры из строки для нормализации имени таблицы."""
    return re.sub(r'\d+', '', text)

def get_column_hash(meta: dict):
    "Геренирует хэш столбца по метаданным"
    return int(hashlib.md5(f"{meta['db_id']}.{meta['table_name']}.{meta['column_name']}".encode()).hexdigest(), 16) % (10**15)

def process_single_database(db_path: str, db_id: str, schema_cache_dir: str, logger: logging.Logger = None) -> Dict[str, Any]:
    """
    Обрабатывает одну базу данных (папку с JSON файлами таблиц).
    Возвращает словарь с метаданными и документами для эмбеддинга.
    """
    if logger:
        logger.info(f"Processing database: {db_id} at {db_path}")
    
    if not os.path.exists(db_path):
        if logger:
            logger.warning(f"Path does not exist: {db_path}")
        return {}

    # Ключ группы: (шаблон_имени, сигнатура_набора_столбцов)
    # Это гарантирует, что таблицы объединяются только если у них одинаковые имена (без цифр)
    # И абсолютно одинаковый набор столбцов.
    processed_groups: Dict[Tuple[str, str], List[Dict]] = {}
    
    json_files = [f for f in os.listdir(db_path) if f.endswith('.json')]

    # Возможно схема разделена на папки
    if not json_files:
        json_files = [os.path.join(folder, f) for folder in os.listdir(db_path) 
                      if os.path.isdir(os.path.join(db_path, folder))
                      for f in os.listdir(os.path.join(db_path, folder)) 
                      if f.endswith('.json')]

    if not json_files:
        if logger:
            logger.warning(f"No JSON files found in {db_path}")
        return {}

    def recursive_key_map(obj, keys=tuple()):
        val = deepcopy(obj)
        for key in keys:
            if isinstance(val, dict) and val.get(key) or isinstance(val, list) and isinstance(key, int) and key < len(val):
                val = val[key]
        
        return val

    for json_file in json_files:
        file_path = os.path.join(db_path, json_file)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Извлечение основных полей
            table_fullname = data.get("table_fullname", data.get("table_name", "unknown"))
            column_names = data.get("column_names", [])
            column_types = data.get("column_types", [])
            descriptions = data.get("description", [])
            sample_rows = data.get("sample_rows", [])
            
            # Обработка nested columns (если есть)
            if "nested_column_names" in data and len(data["nested_column_names"]) > 0:
                nested_names = data["nested_column_names"]
                nested_types = data.get("nested_column_types", [])
                
                # Если nested columns больше, используем их
                if len(nested_names) >= len(column_names):
                    column_names = nested_names
                    column_types = nested_types
                    # Если несоответствие числа описаний и столбцов, 
                    # используем для всех составленные описания по умолчанию
                    if len(descriptions) != len(column_names):
                        descriptions = [""] * len(column_names)

            # Нормализация описаний
            if not descriptions or len(descriptions) < len(column_names):
                descriptions = [""] * len(column_names)
            
            # Создание подписи схемы для группировки
            template_name = remove_digits(table_fullname)
            
            table_info = {
                "original_name": table_fullname,
                "columns": column_names,
                "types": column_types,
                "descriptions": descriptions,
                "sample_rows": sample_rows
            }
            
            # Группировка по шаблону имени и набору столбцов
            col_set_sig = json.dumps(sorted(list(set(column_names))), ensure_ascii=False)
            group_key = (template_name, col_set_sig)

            if group_key not in processed_groups:
                processed_groups[group_key] = []
            processed_groups[group_key].append(table_info)
            
        except Exception as e:
            if logger:
                logger.error(f"Error processing file {json_file} in {db_id}: {str(e)}")
            continue

    # Пост-обработка групп: объединение схожих таблиц
    final_db_metadata = {
        "db_id": db_id,
        "tables": {},
    }
    documents = []

    for (template_name, _), group in processed_groups.items():
        # Если в группе одна таблица или все имеют одинаковую сигнатуру
        # Мы берем первую как репрезентативную, и сохраняем список схожих таблиц
        representative = group[0]
        similar_tables = [t["original_name"] for t in group[1:]]
        
        merged_columns = representative["columns"]
        merged_types = representative["types"]
        
        # Собираем описания столбцов с известных таблиц в группе
        merged_descs = dict(zip(merged_columns, representative["descriptions"]))
        for t in group:
            for col, desc in zip(t['columns'], t["descriptions"]):
                if desc and not merged_descs[col]:
                    merged_descs[col] = desc
    
        table_meta = {
            "similar_tables": similar_tables,
            "columns": merged_columns,
            "types": merged_types,
            "descriptions": [merged_descs[col] for col in merged_columns],
            "sample_rows": representative["sample_rows"] # Берем семплы из первой таблицы
        }
        final_db_metadata["tables"][representative["original_name"]] = table_meta
        
        # Генерация документов и метаданных столбцов для эмбеддингов
        for col, typ, desc in zip(merged_columns, merged_types, table_meta['descriptions']):           
            doc_text = (
                f"Table: {representative['original_name']}. "
                f"Column: {col}. "
                f"Type: {typ}. "
                f"Description: {desc}."
            )
            has_nested_vals = representative["sample_rows"] and not isinstance(recursive_key_map(representative["sample_rows"][0], col.split('.')), dict) 
            documents.append({
                "text": doc_text,
                "metadata": {
                    "db_id": db_id,
                    "table_name": representative["original_name"],
                    "column_name": col,
                    "column_type": typ,
                    "column_vals": [recursive_key_map(line, col.split('.')) for line in representative["sample_rows"]] 
                    if has_nested_vals else []
                }
            })
            documents[-1]["id"] = get_column_hash(documents[-1]["metadata"])

    os.makedirs(schema_cache_dir, exist_ok=True)
    
    meta_path = os.path.join(schema_cache_dir, f"{db_id}_meta.json")
    docs_path = os.path.join(schema_cache_dir, f"{db_id}_docs.json")

    # Сохранение метаданных в JSON
    with open(meta_path, 'w', encoding='utf-8') as f:
        json.dump(final_db_metadata, f, indent=2, ensure_ascii=False)
    
    # Сохранение документов для эмбеддинга
    with open(docs_path, 'w', encoding='utf-8') as f:
        json.dump(documents, f, indent=2, ensure_ascii=False)

    if logger:
        logger.info(f"Finished processing {db_id}. Found {len(final_db_metadata['tables'])} unique table groups.")
    return final_db_metadata

def spider2preprocess(
    input_data_root: str,
    data_root: str = "data",
    output_storage_root: str = "storage",
    is_multidialect: bool = True,
    max_workers: int = 4,
    log_root: str = "logs",
    force_update: bool = False
) -> Dict[str, str]:
    """
    Основная функция предобработки Spider 2 datasets.
    
    Args:
        input_data_root: Путь к папке с данными относительно каталога data_root (например, Spider2/spider2-lite).
        data_root: Путь к корневой папке со всеми входными данным.
        output_storage_root: Путь к папке storage для сохранения кэша.
        is_multidialect: Если True, ожидает структуру data/{root}/{dialect}/resource/databases/{db_id}/.
                         Если False, ожидает data/{root}/resource/databases/{db_id}/.
        max_workers: Количество потоков для параллельной обработки БД.
        log_root: Путь до папки, в которой будут сохранены все логи.
        force_update: Если True, предобработка полностью происходит заново вне зависимости от наличия кэша.
        
    Returns:
        Словарь, отображающий идентификатор базы данных db_id в путь к сохранённым метаданным.
    """
    os.makedirs(os.path.join(log_root, 'dbs', input_data_root), exist_ok=True)
    logger = get_logger(
        "preprocessing", 
        os.path.join(log_root, 'dbs', input_data_root, 'preprocessing.log'), 
        mode='w', 
        force_reconfigure=True
    )
    logger.info(f"Starting preprocessing. Root: {input_data_root}, Multidialect: {is_multidialect}")
    
    os.makedirs(output_storage_root, exist_ok=True)
    schema_cache_dir = os.path.join(output_storage_root, input_data_root, "schema_cache")
    os.makedirs(schema_cache_dir, exist_ok=True)
    
    full_dbs_path = os.path.join(data_root, input_data_root, "resource", "databases")
    if not os.path.exists(full_dbs_path):
        logger.error(f"Input path {full_dbs_path} does not exist.")
        return {}

    # 1. Определение списка баз данных
    db_paths = {} # db_id -> absolute_path
    
    if is_multidialect:
        # Ожидаем папки dialects внутри root
        dialects = [d for d in os.listdir(full_dbs_path) if os.path.isdir(os.path.join(full_dbs_path, d))]
        if input_data_root.endswith("spider2-lite") and "spider2-localdb" in dialects:
            dialects.remove("spider2-localdb")

        logger.info(f"Found dialects: {dialects}")
        
        for dialect in dialects:
            dialect_path = os.path.join(full_dbs_path, dialect)
            dbs = [d for d in os.listdir(dialect_path) if os.path.isdir(os.path.join(dialect_path, d))]
            for db in dbs:
                db_id = f"{dialect}_{db}"
                db_paths[db_id] = os.path.join(dialect_path, db)
    else:
        # Все папки в корне - это базы данных
        dbs = [d for d in os.listdir(full_dbs_path) if os.path.isdir(os.path.join(full_dbs_path, d))]
        for db in dbs:
            db_paths[db] = os.path.join(full_dbs_path, db)
            
    if not db_paths:
        logger.error(f"No databases found. Check input path and structure at {full_dbs_path}")
        return {}

    # 3. Фильтрация уже обработанных, если не указано force_update=True
    tasks_to_process = {}
    skipped_count = 0
    
    for db_id, path in db_paths.items():
        meta_file = os.path.join(schema_cache_dir, f"{db_id}_meta.json")
        docs_file = os.path.join(schema_cache_dir, f"{db_id}_docs.json")
        
        if not force_update and os.path.exists(meta_file) and os.path.exists(docs_file):
            logger.info(f"Skipping {db_id} (already processed). Use --force to overwrite.")
            skipped_count += 1
        else:
            tasks_to_process[db_id] = path

    if not tasks_to_process:
        logger.info("Nothing to process. All databases are up to date.")
        return {db_id: os.path.join(schema_cache_dir, f"{db_id}_meta.json") for db_id in db_paths.keys()}        
    
    logger.info(f"Total DBs: {len(db_paths)}. Skipped: {skipped_count}. To Process: {len(tasks_to_process)}")

    # 3. Параллельная обработка
    results = {}
    
    def _worker(db_id: str, path: str):
        nonlocal schema_cache_dir, logger
        try:
            meta = process_single_database(path, db_id, schema_cache_dir, logger)
            if not meta:
                return None
                
            return db_id, os.path.join(schema_cache_dir, f"{db_id}_meta.json")
        except Exception as e:
            logger.error(f"Critical error processing {db_id}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_db = {executor.submit(_worker, db_id, path): db_id for db_id, path in db_paths.items()}
        
        for future in as_completed(future_to_db):
            try:
                result = future.result()
                if result:
                    db_id, save_path = result
                    results[db_id] = save_path
                    logger.info(f"Saved metadata for {db_id}")
            except Exception as e:
                logger.error(f"Failed to process {db_id}: {str(e)}")

    logger.info(f"Preprocessing completed. Processed {len(results)} databases.")
    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Preprocess Spider 2 structured dataset for Text-to-SQL pipeline.")
    parser.add_argument(
        "input_data", type=str, default="Spider2\spider2-lite",
        help="Path to the directory of the Spider 2 structured dataset in data_root directory (default: Spider2\spider2-lite)"
    )
    parser.add_argument(
        "--data_root", type=str, default="data",
        help="Path to the root directory with all input data (default: data)"
    )
    parser.add_argument(
        "--output_storage", type=str, default="storage",
        help="Path to the storage directory for cached schemas and embeddings (default: storage)"
    )
    parser.add_argument(
        "--log_root", type=str, default="logs",
        help="Path to the directory with all saved logs (default: logs)"
    )
    parser.add_argument(
        "--workers", type=int, default=4,
        help="Number of parallel workers for processing databases (default: 4)"
    )
    parser.add_argument(
        "--multidialect", action="store_true",
        help="If set, expects structure {root}/{dialect}/{db_id}/, otherwise {root}/{db_id}/ (all DBs in root)"
    )
    parser.add_argument(
        "--force", action="store_true", 
        help="Force reprocessing of all databases, ignoring cache"
    )
    
    args = parser.parse_args()
    
    print(f"Starting preprocessing...")
    print(f"Input: {args.input_data}")
    print(f"Storage: {args.output_storage}")
    print(f"Mode: {'Multidialect' if args.multidialect else 'Single Dialect'}")
    
    spider2preprocess(
        input_data_root=args.input_data,
        data_root=args.data_root,
        output_storage_root=args.output_storage,
        is_multidialect=args.multidialect,
        max_workers=args.workers,
        log_root=args.log_root,
        force_update=args.force
    )
    
    print(f"Preprocessing finished. Check {os.path.join(args.log_root, 'dbs', args.input_data, 'preprocessing.log')} for details.")
