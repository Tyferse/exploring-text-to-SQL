import sys
sys.path.insert(0, ".")

import os
import re
import json
import time
import logging
import tempfile
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from langchain_core.language_models import BaseChatModel

from src.modules.schema_linking.generate_schema import generate_single_schema
from src.modules.schema_linking.schema_formatter import (load_similar_tables, load_schemas, 
                                                         format_detailed_block, format_compact_block)
from src.utils.logger import get_logger
from src.utils.models import get_model
from src.utils.preprocessing import remove_digits, fill_prompt_template
from src.utils.run_manager import resolve_run_id
from src.utils.sql_execution import SQLExecutor


DEFAULT_RETRY_CONFIG = {
    "max_attempts": 4,
    "initial_delay": 2.0,
    "max_delay": 30.0,
    "backoff_multiplier": 2.0,
}

def _load_prompt_template(prompt_name: str = "gen_basic", prompt_dir: str = "config/prompts/generation") -> str:
    """Загружает шаблон промпта."""
    prompt_path = Path(prompt_dir) / f"{prompt_name}.md"
    if not prompt_path.exists():
        raise FileNotFoundError(f"Prompt template {prompt_name} not found in {prompt_dir}")
    
    return prompt_path.read_text(encoding="utf-8")

def _load_optimization_rules(prompt_dir: str, dialect: str) -> str:
    """
    Загружает правила оптимизации для конкретного диалекта.
    """
    rules_path = Path(prompt_dir) / "dialects" / f"{dialect}_rules.txt"
    if rules_path.exists():
        return rules_path.read_text(encoding="utf-8").strip()
    
    return f"-- No specific optimization rules for {dialect}"

def _load_or_make_schema(
    instance_id: str,
    run_id: str,
    runs_root: str = "logs/runs",
    schema_dir: str = "final_schema",
    schemas: Optional[Dict[str, Dict[int, Dict[str, Any]]]] = None,
    similar_tables: Optional[Dict[str, List[str]]] = None,
    chars_per_token: float = 3.0,
    target_max_tokens: int = 64_000,
    logger: Optional[logging.Logger] = None,
    **formatter_kwargs
) -> str:
    """
    Загружает или генерирует схему для примера.
    1. {runs_root}/{run_id}/schema_linking/{schema_dir}/{instance_id}.txt
    3. Генерация через generate_single_schema
    """
    final_schema_path = Path(runs_root) / run_id / "schema_linking" / schema_dir / f"{instance_id}.txt"
    if final_schema_path.exists():
        if logger: logger.info(f"Loaded schema from {schema_dir}: {final_schema_path}")
        return final_schema_path.read_text(encoding="utf-8")
    
    # Попытка 3: генерация
    if logger: logger.info(f"Schema not found, generating full schema for instance {instance_id}")
    # Берём все column IDs из всех таблиц БД
    col_ids = [cid for table_cols in schemas.values() for cid in table_cols.keys()]
    schema_text, _ = generate_single_schema(
        instance_id=instance_id,
        col_ids=col_ids,
        doc_data=schemas,
        format_schema_block=format_compact_block,
        target_max_tokens=target_max_tokens,
        similar_tables=similar_tables,
        chars_per_token=chars_per_token,
        output_path=Path(runs_root) / run_id / "schema_linking" / "final_schema",
        log=logger,
        **formatter_kwargs
    )
    
    if logger: logger.info(f"Full schema generated for {instance_id}: {len(schema_text)} chars, tokens~{round(len(schema_text) / chars_per_token)}")
    return schema_text

def _load_exploration_block(exploration_dir: str, instance_id: str, logger: Optional[logging.Logger] = None) -> Optional[str]:
    """
    Загружает блок исследовательских запросов из dbc_retrieval.
    Возвращает уже отформатированный текст (последнее сообщение из JSON) или None.
    """
    # exploration_dir = runs_root / run_id / "dbc_retrieval" / "exec_exploration_results"
    exploration_path = Path(exploration_dir)
    if not exploration_path.exists():
        if logger: logger.debug(f"Exploration dir not found: {exploration_path}")
        return None
    
    # Ищем файл с результатами для instance_id
    target_file = exploration_path / f"{instance_id}.json"
    if not target_file.exists():
        if logger: logger.debug(f"No exploration results found for {instance_id}")
        return None
    
    try:
        with open(target_file, "r", encoding="utf-8") as f:
            messages = json.load(f)
        
        # Ожидаем структуру с тремя сообщениями, берём последнее (результаты исполнения)
        if len(messages) >= 3:
            exploration_text = messages[-1].get("content", "")
            if exploration_text.strip():
                if logger: logger.info(f"Loaded exploration block for {instance_id} ({len(exploration_text)} chars)")
                return exploration_text
        
        if logger: logger.debug(f"Exploration file {target_file.name} has insufficient messages for {instance_id}")
        return None
        
    except Exception as e:
        if logger: logger.warning(f"Failed to load exploration for {instance_id}: {e}")
        return None

def _load_few_shots(few_shots_dir: str, instance_id: str, logger: Optional[logging.Logger] = None) -> dict:
    """
    Загрузка few-shot примеров.
    """
    few_shots_file = Path(few_shots_dir) / f"{instance_id}.json"
    if few_shots_file.exists():
        content = json.loads(few_shots_file.read_text(encoding="utf-8").strip())
        if logger: logger.info(f"Loaded few-shots for {instance_id} from {few_shots_file.parent}")
        return content
    
    # if logger: logger.debug(f"No few-shots found for {instance_id} at {few_shots_file.parent}")
    return {}

def _load_instances(
    run_id: str,
    runs_root: str = "logs/runs",
    tasks: Optional[List[Dict[str, Any]]] = None,
    input_data_root: str = "Spider2/spider2-lite",
    data_root: str = "data",
    storage_root: str = "storage",
    gen_prefix: str = "one_step",
    schema_dir: str = "final_schema",
    chars_per_token: float = 3.0,
    max_schema_tokens: int = 64_000,
    logger: Optional[logging.Logger] = None
) -> Dict[str, Dict[str, Any]]:
    """Загрузка задач из файла."""
    assert tasks is not None or input_data_root is not None, "tasks or input_data_root argument must be not None"

    # Ищем JSON файл с задачами
    if tasks is None and input_data_root is not None:
        for file_path in (Path(data_root) / input_data_root).glob("*.jsonl"):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    tasks = [json.loads(line.strip()) for line in f.readlines()]
                break
            except Exception:
                continue
    
    tasks_dict = {
        instance["instance_id"]: {
            "instance_id": instance["instance_id"],
            "dialect": instance.get("dialect", ""),
            "db_id": instance.get("dialect", "") + ("_" if instance.get("dialect") else "") + instance["db_id"], 
            "question": instance.get("question", instance.get("instruction", ""))
        } 
        for instance in tasks if not (Path(runs_root) / run_id / "generation" / gen_prefix / "manifests" / f"{instance['instance_id']}.json").exists()
    }
    if input_data_root == "Spider2/spider2-lite":
        inst2dialect = {"sf": "snowflake", "bq": "bigquery", "ga": "bigquery", "local": "sqlite"}
        for iid in tasks_dict:
            tasks_dict[iid]["dialect"] = inst2dialect[remove_digits(iid).split("_")[0]]
            tasks_dict[iid]["db_id"] = tasks_dict[iid]["dialect"] + "_" + tasks_dict[iid]["db_id"]

    docs_path = str(Path(storage_root) / input_data_root / "schema_cache")
    schemas = load_schemas(docs_path)
    similar_tables = load_similar_tables(docs_path)
    for iid in tasks_dict:
        db_id = tasks_dict[iid]["db_id"]
        tasks_dict[iid]["schema"] = _load_or_make_schema(
            iid, run_id, runs_root, schema_dir, 
            schemas[db_id], similar_tables[db_id], 
            chars_per_token, max_schema_tokens, logger
        )
        exploration_block = _load_exploration_block(
            str(Path(runs_root) / run_id / "dbc_retrieval" / "exec_exploration_results"), iid, logger
        )
        if exploration_block:
            tasks_dict[iid]["exploration_block"] = exploration_block
        
        few_shot_examples = _load_few_shots(
            str(Path(runs_root) / run_id / "additional_information" / "few_shots"), iid, logger
        )
        if few_shot_examples:
            tasks_dict[iid]["few_shots"] = few_shot_examples

    return tasks_dict

def extract_sql_from_response(response_text: str, logger: Optional[logging.Logger] = None) -> Optional[str]:
    """
    Извлекает SQL из ответа модели.
    Приоритет: блок ```sql ... ```, затем fallback по ключевым словам.
    """
    # Основной паттерн
    match = re.search(r"```sql\s*(.*?)\s*```", response_text, re.DOTALL | re.IGNORECASE)
    if match:
        sql = match.group(1).strip()
        if sql:
            return sql
    
    if logger: logger.debug("No ```sql``` block found, attempting fallback extraction...")
    
    # Fallback: ищем строки, начинающиеся с SQL-ключевых слов
    lines = response_text.strip().split("\n")
    sql_lines = []
    in_sql = False
    
    for line in lines:
        stripped = line.strip()
        if re.match(r"^(SELECT|WITH|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER)\b", stripped, re.IGNORECASE):
            in_sql = True
        if in_sql and stripped and not stripped.startswith("```"):
            sql_lines.append(line)
        if stripped.endswith("```") and in_sql:
            break
    
    result = "\n".join(sql_lines).strip()
    if result:
        if logger: logger.debug(f"Fallback extraction succeeded: {len(result)} chars")
        return result
    
    if logger: logger.warning("Failed to extract SQL from response")
    return None

def generate_sql_simple(
    instance_data: Dict[str, Any],
    run_id: str,
    model: BaseChatModel,
    executor: SQLExecutor,
    runs_root: str = "logs/runs",
    promt_name: str = "gen_basic",
    prompt_dir: str = "config/prompts/generation",
    n_candidates: int = 1,
    prefix: str = "one_step",
    retry_config: Dict[str, float] = DEFAULT_RETRY_CONFIG
) -> Dict[str, Any]:
    """
    Обрабатывает один пример: генерирует n SQL-кандидатов, исполняет их, сохраняет артефакты.
    
    Returns:
        Dict с результатами: {instance_id: {candidate_01: {...}, candidate_02: {...}, ...}}
    """
    instance_id = instance_data["instance_id"]
    db_id = instance_data["db_id"]
    question = instance_data["question"]
    dialect = instance_data.get("dialect", "sqlite")
    schema = instance_data["schema"]
    external_knowledge = instance_data.get("external_knowledge", "None")
    exploration_block = instance_data.get("exploration_block", "No exploration data available.")
    few_shots = instance_data.get("few_shots", "None")
    # Заглушка, по умолчанию просто сериализуем
    few_shots = json.dumps(few_shots, ensure_ascii=False) if isinstance(few_shots, dict) else few_shots
    
    # === 1. Инициализация логгера для инстанса ===
    base_dir = Path(runs_root) / run_id / "generation" / prefix
    events_dir = base_dir / "events"
    events_dir.mkdir(parents=True, exist_ok=True)
    logger = get_logger(f"gen_{instance_id}", str(events_dir / f"{instance_id}.log"))
    
    logger.info(f"=== START INSTANCE: {instance_id} (DB: {db_id}, Dialect: {dialect}) ===")
    start_time = time.perf_counter()
    
    results = {
        "instance_id": instance_id,
        "db_id": db_id,
        "question": question,
        "dialect": dialect,
        "candidates": {},
        "metadata": {
            "start_time": start_time,
            "end_time": None,
            "status": "processing"
        }
    }
    
    try:
        # 2. Промпт
        prompt_template = _load_prompt_template(promt_name, prompt_dir)
        optimization_rules = _load_optimization_rules(prompt_dir, dialect)
        
        # Рендеринг промпта (простая замена плейсхолдеров)
        prompt = fill_prompt_template(
            prompt_template, 
            {
                "{{QUESTION}}": question,
                "{{SCHEMA}}": schema,
                "{{EXPLORATION_BLOCK}}": exploration_block,
                "{{EXTERNAL_KNOWLEDGE}}": external_knowledge,
                "{{FEW_SHOT_EXAMPLES}}": few_shots,
                "{{DIALECT}}": dialect,
                "{{DIALECT_OPTIMIZATION_RULES}}": optimization_rules
            }
        )
        
        # === 3. Генерация кандидатов ===
        for c in range(n_candidates):
            c_str = f"{c:02d}"
            sql_dir = base_dir / f"sql_{c_str}"
            results_dir = base_dir / f"results_{c_str}"

            logger.info(f"--- Generating candidate {c_str} ---")
            
            candidate_result = {
                "candidate_id": c_str,
                "generation": {"status": "pending"},
                "execution": {"status": "pending"},
                "artifacts": {}
            }
            
            # Retry loop для генерации
            attempt = 0
            max_attempts = retry_config["max_attempts"]
            last_error = None
            
            while attempt < max_attempts:
                delay = retry_config["initial_delay"] * (retry_config["backoff_multiplier"] ** attempt)
                delay = min(delay, retry_config["max_delay"])
                
                if attempt > 0:
                    logger.info(f"Attempt {attempt+1}/{max_attempts} after {delay:.2f}s delay")
                    time.sleep(delay)
                
                try:
                    pre_call_time = time.perf_counter()
                    logger.info(f"Calling LLM (attempt {attempt+1})...")
                    
                    response = model.invoke(prompt)
                    response_text = response.content if hasattr(response, "content") else str(response)
                    
                    call_duration = time.perf_counter() - pre_call_time
                    logger.info(f"LLM response received in {call_duration:.2f}s ({len(response_text)} chars)")
                    
                    # Извлечение SQL
                    sql = extract_sql_from_response(response_text, logger)
                    
                    if not sql:
                        raise ValueError("Empty or invalid SQL extracted from response")
                    
                    # Успешная генерация
                    candidate_result["generation"] = {
                        "status": "success",
                        "sql": sql,
                        "attempt": attempt + 1,
                        "call_duration_sec": round(call_duration, 3),
                        "response_length": len(response_text),
                        "timestamp": time.time()
                    }
                    
                    # Сохранение артефактов В gen_meta.json
                    gen_meta = {
                        "candidate_id": c_str,
                        "instance_id": instance_id,
                        "generation": candidate_result["generation"],
                        "prompt": prompt,  # Полный промпт
                        "raw_response": response_text  # Сырой ответ
                    }
                    
                    meta_path = sql_dir / f"{instance_id}_meta.json"
                    # Атомарная запись
                    fd, tmp_path = tempfile.mkstemp(dir=str(sql_dir), suffix=".tmp")
                    try:
                        with os.fdopen(fd, "w", encoding="utf-8") as tmp:
                            json.dump(gen_meta, tmp, ensure_ascii=False, indent=2)
                        os.replace(tmp_path, str(meta_path))
                    except Exception:
                        if os.path.exists(tmp_path):
                            os.unlink(tmp_path)
                        raise
                    
                    # Также сохраняем чистый SQL для удобства
                    sql_path = sql_dir / f"{instance_id}.sql"
                    sql_path.write_text(sql, encoding="utf-8")
                    
                    logger.info(f"Candidate {c_str} generated successfully. SQL saved.")
                    break  # Выход из retry loop
                    
                except Exception as e:
                    last_error = str(e)
                    logger.warning(f"Attempt {attempt+1} failed: {last_error}")
                    attempt += 1
            
            # Если все попытки исчерпаны
            if attempt >= max_attempts:
                candidate_result["generation"] = {
                    "status": "failed",
                    "error": last_error,
                    "attempts": max_attempts,
                    "timestamp": time.time()
                }
                gen_meta = {
                    "candidate_id": c_str,
                    "instance_id": instance_id,
                    "generation": candidate_result["generation"],
                    "prompt": prompt,  # Промпт всё равно сохраняем для отладки
                    "raw_response": None
                }
                meta_path = sql_dir / f"{instance_id}_meta.json"
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(gen_meta, f, ensure_ascii=False, indent=2)

                logger.error(f"Candidate {c_str} generation failed after {max_attempts} attempts")
            
            # === 4. Исполнение SQL (только если генерация успешна) ===
            if candidate_result["generation"]["status"] == "success":
                sql = candidate_result["generation"]["sql"]
                logger.info(f"--- Executing candidate {c_str} ---")
                
                try:
                    exec_start = time.perf_counter()
                    status, df = executor.thread_safe_sql_execution(
                        sql=sql,
                        db_name=db_id.split("_", 1)[1] if "_" in db_id else db_id,
                        dialect=dialect
                    )
                    exec_duration = time.perf_counter() - exec_start
                    
                    candidate_result["execution"] = {
                        "status": status,
                        "duration_sec": round(exec_duration, 3),
                        "timestamp": time.time()
                    }
                    
                    if status == "success":
                        # Сохраняем результат в CSV
                        csv_path = results_dir / f"{instance_id}.csv"
                        df.to_csv(csv_path, index=False, encoding="utf-8")
                        candidate_result["execution"]["rows"] = len(df)
                        candidate_result["execution"]["cols"] = len(df.columns)
                        logger.info(f"Executed successfully: {len(df)} rows, {exec_duration:.2f}s")
                        
                    elif status == "empty":
                        # Пустой результат — тоже успех, но без данных
                        csv_path = results_dir / f"{instance_id}.csv"
                        csv_path.write_text("# Empty result\n", encoding="utf-8")
                        candidate_result["execution"]["rows"] = 0
                        logger.info("Executed: empty result")
                        
                    else:  # error
                        logger.warning(f"Execution error: {status}")
                        
                except Exception as e:
                    candidate_result["execution"] = {
                        "status": "error",
                        "error": str(e),
                        "timestamp": time.time()
                    }
                    logger.error(f"Execution exception: {e}")
            
            # Добавляем кандидата в результаты
            results["candidates"][c_str] = candidate_result
        
        # === 5. Финализация ===
        end_time = time.perf_counter()
        results["metadata"]["end_time"] = end_time
        results["metadata"]["total_duration_sec"] = round(end_time - start_time, 3)
        results["metadata"]["status"] = "completed"
        
        # Сохраняем сводный манифест примера
        manifest_path = base_dir / "manifests" / f"{instance_id}.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"=== COMPLETED INSTANCE: {instance_id} | Duration: {results['metadata']['total_duration_sec']}s ===")
        
        return results
        
    except Exception as e:
        # Критическая ошибка на уровне инстанса
        logger.critical(f"UNHANDLED EXCEPTION: {e}", exc_info=True)
        results["metadata"]["status"] = "error"
        results["metadata"]["error"] = str(e)
        return results

def simple_generation(
    run_id: str,
    model: BaseChatModel,
    executor: SQLExecutor,
    tasks: Optional[List[Dict[str, Any]]] = None,
    input_data_root: str = "Spider2/spider2-lite",
    runs_root: str = "logs/runs",
    data_root: str = "data",
    storage_root: str = "storage",
    schema_dir: str = "final_schema",
    prompt_name: str = "gen_basic",
    prompt_dir: str = "config/prompts/generation",
    n_candidates: int = 1,
    prefix: str = "one_step",
    max_workers: int = 3,
    retry_config: Dict[str, float] = DEFAULT_RETRY_CONFIG,
    max_schema_tokens: int = 64_000,
    chars_per_token: float = 3.0
) -> Dict[str, Any]:
    """
    Запускает пайплайн генерации для множества инстансов.
    
    Returns:
        Сводная статистика по всем инстансам.
    """
    prompt_dir = Path(prompt_dir)
    
    # === Глобальный логгер для оркестратора ===
    base_path = Path(runs_root) / run_id / "generation" / prefix
    main_log = base_path / "main.log"
    main_log.parent.mkdir(parents=True, exist_ok=True)
    
    logger = get_logger("simple_gen", str(main_log))
    logger.info(f"=== ONE STEP GENERATION START: run_id={run_id}, instances={len(tasks) if tasks else 'loading...'} ===")
    
    # === Загрузка данных ===
    
    # 1. Примеры
    tasks = _load_instances(
        run_id, runs_root, tasks, input_data_root, data_root, storage_root, 
        prefix, schema_dir, chars_per_token, max_schema_tokens, logger
    )     
    logger.info(f"Loaded {len(tasks)} instances")
    
    
    # === Подготовка директорий ===
    for c in range(n_candidates):
        (base_path / f"sql_{c:02d}").mkdir(parents=True, exist_ok=True)
        (base_path / f"results_{c:02d}").mkdir(parents=True, exist_ok=True)
    
    # === Параллельное выполнение ===
    all_results = {}
    start_pipeline = time.perf_counter()
    
    logger.info(f"Starting parallel execution with {max_workers} workers")
    
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="GenWorker") as pool:
        future_to_instance = {
            pool.submit(
                generate_sql_simple,
                instance_data=tasks[iid],
                run_id=run_id,
                model=model,
                executor=executor,
                runs_root=runs_root,
                prompt_name=prompt_name,
                prompt_dir=prompt_dir,
                n_candidates=n_candidates,
                prefix=prefix,
                retry_config=retry_config
            ): iid
            for iid in tasks
        }
        
        for future in as_completed(future_to_instance):
            instance_id = future_to_instance[future]
            try:
                result = future.result()
                all_results[instance_id] = result
                logger.info(f"Completed: {instance_id}")
            except Exception as e:
                logger.error(f"Failed {instance_id}: {e}", exc_info=True)
                all_results[instance_id] = {"instance_id": instance_id, "error": str(e)}
    
    # === Сводная статистика ===
    total_duration = time.perf_counter() - start_pipeline
    stats = {
        "total_instances": len(tasks),
        "completed": sum(1 for r in all_results.values() if r.get("metadata", {}).get("status") == "completed"),
        "errors": sum(1 for r in all_results.values() if r.get("metadata", {}).get("status") == "error"),
        "total_duration_sec": round(total_duration, 2),
        "candidates_generated": sum(
            sum(1 for c in r.get("candidates", {}).values() if c.get("generation", {}).get("status") == "success")
            for r in all_results.values()
        ),
        "queries_executed": sum(
            sum(1 for c in r.get("candidates", {}).values() if c.get("execution", {}).get("status") in ("success", "empty"))
            for r in all_results.values()
        )
    }
    
    # Сохраняем статистику
    stats_path = base_path / "pipeline_stats.json"
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    
    logger.info(f"=== PIPELINE FINISHED ===")
    logger.info(f"Stats: {json.dumps(stats, indent=2)}")
    
    return {
        "results": all_results,
        "stats": stats
    }


if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv(".env")

    def parse_dialect_path_pair(value: str) -> tuple[str, str]:
        if ':' in value:
            dialect, path = value.split(':', 1)
        elif '=' in value:
            dialect, path = value.split('=', 1)
        else:
            raise argparse.ArgumentTypeError(
                f"Invalid format '{value}'. Use 'dialect:path' or 'dialect=path'"
            )
        
        dialect = dialect.strip().lower()
        path = path.strip().rstrip('/') 
        
        if not dialect or not path:
            raise argparse.ArgumentTypeError("Both dialect and path must be non-empty")
        

    parser = argparse.ArgumentParser(description="SQL generation from natural language")
    
    # === Обязательные ===
    parser.add_argument("--run-name", required=True, help="Название запуска")
    parser.add_argument("--model-name", required=True, help="Имя модели (qwen-local, gpt-4o, etc.)")
    
    # === Модель ===
    parser.add_argument("--base-url", default=None, help="Base URL API модели")
    parser.add_argument("--api-key", default=None, help="API ключ модели")
    parser.add_argument("--temperature", type=float, default=1.0, help="Температура модели")
    
    # === Данные ===
    parser.add_argument("input-data-root", default="Spider2/spider2-lite", help="Путь к данным внутри data-root")
    parser.add_argument("--run-root", default="logs/runs", help="Корень для результатов")
    parser.add_argument("--data-root", default="data", help="Корень входных данных")
    parser.add_argument("--storage-root", default="storage", help="Корень кэша схем")
    parser.add_argument("--prefix", default="simple", help="Название папки с результатами генерации внутри папки логов.")
    parser.add_argument(
        "--local-dbs",
        type=parse_dialect_path_pair,
        nargs="*",  # Принимает 0 или более значений
        default=None,  # None означает "использовать дефолты из SQLExecutor"
        metavar="DIALECT:PATH",
        help="Пути к папкам локальных БД относительно data_root/input_data_root. "
            "Формат: 'dialect:path' (можно указать несколько через пробел). "
            "Пример: --local-dbs sqlite:databases snowflake:sf_data bigquery:local_bq"
    )

    # === Промпты ===
    parser.add_argument("--prompt-name", default="gen_basic", help="Имя шаблона промпта (без .md)")
    parser.add_argument("--prompt-dir", default="config/prompts/generation", help="Директория промптов")
    parser.add_argument("--schema-dir", default="final_schema", help="Папка с заранее сформированными схемами БД (относительно папки schema_linking)")
    
    # === Генерация ===
    parser.add_argument("--n-candidates", type=int, default=1, help="Кандидатов на пример")
    parser.add_argument("--max-workers", type=int, default=3, help="Параллельных воркеров")
    parser.add_argument("--max-schema-tokens", type=int, default=64_000, help="Лимит токенов схемы")
    parser.add_argument("--chars-per-token", type=float, default=3.0, help="Символов на токен")
    
    # === Retry (плоские аргументы) ===
    parser.add_argument("--max-attempts", type=int, default=4, help="Макс. попыток генерации")
    parser.add_argument("--initial-delay", type=float, default=2.0, help="Начальная задержка (сек)")
    parser.add_argument("--max-delay", type=float, default=30., help="Макс. задержка (сек)")
    args = parser.parse_args()
    
    # === Инициализация ===
    
    model = get_model(
        model_name=args.model_name,
        base_url=args.model_base_url,
        api_key=args.model_api_key,
        temperature=args.temperature
    )
    
    executor = SQLExecutor(
        args.input_data_root, args.data_root, args.storage_root, 
        dict(args.local_dbs) if args.local_dbs else None
    )
    
    # Сборка retry_config
    retry_config = {
        "max_attempts": args.max_retries,
        "initial_delay": args.initial_retry_delay,
        "max_delay": args.max_retry_delay,
        "backoff_multiplier": 2.0,
    }

    # === Запуск ===
    run_id = resolve_run_id(args.runs_root, args.input_data_root, args.run_name)
    output = simple_generation(
        run_id=run_id,
        model=model,
        executor=executor,
        input_data_root=args.input_data_root,
        runs_root=args.runs_root,
        data_root=args.data_root,
        storage_root=args.storage_root,
        schema_dir=args.schema_dir,
        prompt_name=args.prompt_name,
        prompt_dir=args.prompt_dir,
        n_candidates=args.n_candidates,
        prefix=args.prefix,
        max_workers=args.max_workers,
        retry_config=retry_config,
        max_schema_tokens=args.max_schema_tokens,
        chars_per_token=args.chars_per_token,
    )
    
    # === Вывод ===
    stats = output.get("stats", {})
    print("\n📊 Результаты:")
    print(f"  Всего примеров:  {stats.get('total_instances', 0)}")
    print(f"  Завершено:       {stats.get('completed', 0)}")
    print(f"  Ошибок:          {stats.get('errors', 0)}")
    print(f"  Кандидатов:      {stats.get('candidates_generated', 0)}")
    print(f"  Запросов:        {stats.get('queries_executed', 0)}")
    print(f"  Время:           {stats.get('total_duration_sec', 0):.2f} сек")
