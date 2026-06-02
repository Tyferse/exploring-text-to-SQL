import sys
sys.path.insert(0, ".")

import os
import re
import json
import time
import logging
import tempfile
from pathlib import Path
from typing import Dict, Any, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

from src.utils.logger import get_logger
from src.utils.models import get_model, serialize_messages
from src.utils.preprocessing import fill_prompt_template
from src.utils.run_manager import resolve_run_id
from src.utils.sql_execution import SQLExecutor, parse_dialect_path_pair
from analysis.clause_recognition import find_error_operator, find_error_operator_simple


DEFAULT_RETRY_CONFIG = {
    "max_attempts": 4,
    "initial_delay": 2.0,
    "max_delay": 30.0,
    "backoff_multiplier": 2.0,
}


def _load_prompt(prompt_name: str, prompt_dir: str = "config/prompts/correction") -> str:
    """Загружает шаблон промпта по имени."""
    prompt_path = Path(prompt_dir) / f"{prompt_name}.md"
    if not prompt_path.exists():
        raise FileNotFoundError(f"Prompt template {prompt_name} not found in {prompt_dir}")

    return prompt_path.read_text(encoding="utf-8")

def _get_failed_operator(sql: str, error_message: str, dialect: str) -> str:
    """Определяет оператор с фоллбэком на простой метод."""
    operator = find_error_operator(sql=sql, error_message=error_message, dialect=dialect)
    if operator == "Unknown":
        operator = find_error_operator_simple(sql=sql, error_message=error_message)

    return operator

def _is_empty_or_null_result(status: str, df: Any) -> bool:
    """Проверяет, является ли результат пустым или состоящим только из NULL."""
    if status == "empty":
        return True
    if status == "success" and df is not None:
        if len(df) == 0:
            return True
        # Проверка на все NULL значения (если df - это pandas DataFrame)
        if hasattr(df, 'isna') and hasattr(df, 'all'):
            if df.isna().all().all():
                return True
    return False

def _load_correction_context(instance_id: str, run_id: str, runs_root: str, schema_dir: str = "final_schema", logger: Optional[logging.Logger] = None) -> Dict[str, str]:
    """Загружает схему, exploration и few-shots из структуры папок генерации."""
    runs_path = Path(runs_root) / run_id
    
    # 1. Schema
    schema_path = runs_path / "schema_linking" / schema_dir / f"{instance_id}.txt"
    schema = schema_path.read_text(encoding="utf-8") if schema_path.exists() else "Schema not found."
    
    # 2. Exploration Block
    exploration = "No exploration data available."
    expl_path = runs_path / "dbc_retrieval" / "exec_exploration_results" / f"{instance_id}.json"
    if expl_path.exists():
        try:
            with open(expl_path, "r", encoding="utf-8") as f:
                messages = json.load(f)
            if len(messages) >= 3:
                exploration = messages[-1].get("content", "No content in last message.")
        except Exception as e:
            if logger: logger.warning(f"Failed to load exploration for {instance_id}: {e}")

    # 3. Few Shots
    few_shots = "None"
    fs_path = runs_path / "additional_information" / "few_shots" / f"{instance_id}.json"
    if fs_path.exists():
        try:
            with open(fs_path, "r", encoding="utf-8") as f:
                few_shots = json.dumps(json.load(f), ensure_ascii=False, indent=2)
        except Exception as e:
            if logger: logger.warning(f"Failed to load few shots for {instance_id}: {e}")

    return {
        "schema": schema,
        "exploration_block": exploration,
        "few_shots": few_shots
    }

def _load_failed_candidates(
    run_id: str,
    runs_root: str = "logs/runs",
    gen_prefix: str = "simple"
) -> List[Dict[str, Any]]:
    """
    Загружает только те кандидаты из манифестов генерации, которые завершились с ошибкой 
    или пустым результатом, и формирует структуру с полем `corrections`.
    """
    base_path = Path(runs_root) / run_id / "generation" / gen_prefix / "manifests"
    if not base_path.exists():
        raise FileNotFoundError(f"Generation manifests not found at {base_path}")

    tasks = []
    for manifest_file in base_path.glob("*.json"):
        with open(manifest_file, "r", encoding="utf-8") as f:
            gen_data = json.load(f)
        
        instance_id = gen_data["instance_id"]
        db_id = gen_data["db_id"]
        question = gen_data["question"]
        dialect = gen_data.get("dialect", "sqlite")
        
        # Формируем словарь corrections только для неудачных кандидатов
        corrections = {}
        for cand_id_str, cand_data in gen_data.get("candidates", {}).items():
            exec_status = cand_data.get("execution", {}).get("status", "pending")
            if exec_status in ("error", "empty"):
                cand_int = int(cand_id_str)
                corrections[cand_int] = {
                    "sql": cand_data.get("generation", {}).get("sql", ""),
                    "error": cand_data.get("execution", {}).get("error", "Empty result")
                }
        
        if corrections:
            tasks.append({
                "instance_id": instance_id,
                "db_id": db_id,
                "dialect": dialect,
                "question": question,
                "external_knowledge": gen_data.get("external_knowledge", "None"),
                "corrections": corrections
            })
            
    return tasks

def extract_sql_from_response(response_text: str, logger: Optional[logging.Logger] = None) -> Optional[str]:
    """Извлекает SQL из ответа модели (копия из скрипта генерации)."""
    match = re.search(r"```sql\s*(.*?)\s*```", response_text, re.DOTALL | re.IGNORECASE)
    if match:
        sql = match.group(1).strip()
        if sql:
            return sql
    
    if logger: logger.debug("No ```sql``` block found, attempting fallback extraction...")
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
        return result
    
    return None

def correct_single_candidate(
    instance_data: Dict[str, Any],
    candidate_id: int,
    run_id: str,
    model: BaseChatModel,
    executor: SQLExecutor,
    prompt_dir: str = "config/prompts/correction",
    prompt_names: Optional[Dict[str, str]] = None,
    runs_root: str = "logs/runs",
    schema_dir: str = "final_schema",
    gen_prefix: str = "gen",
    max_turns: int = 3,
    max_messages: int = 5,
    retry_config: Dict[str, float] = DEFAULT_RETRY_CONFIG
) -> Dict[str, Any]:
    """Обрабатывает один конкретный кандидат на исправление."""

    instance_id = instance_data["instance_id"]
    db_id = instance_data["db_id"]
    question = instance_data["question"]
    dialect = instance_data.get("dialect", "sqlite")
    external_knowledge = instance_data.get("external_knowledge", "None")
    corrections = instance_data["corrections"]
    
    current_sql = corrections[candidate_id]["sql"]
    current_error = corrections[candidate_id]["error"]
    
    # 1. Инициализация пер-инстанс логгера
    base_dir = Path(runs_root) / run_id / "correction" / gen_prefix
    events_dir = base_dir / "events"
    events_dir.mkdir(parents=True, exist_ok=True)
    logger = get_logger(
        name=f"corr_{instance_id}_cand{candidate_id}", 
        log_file=str(events_dir / f"{instance_id}_cand{candidate_id}.log"),
        console=False
    )
    
    logger.info(f"=== START CORRECTION: {instance_id} | Candidate: {candidate_id} ===")
    start_time = time.perf_counter()
    
    # 2. Загрузка контекста
    logger.info("Loading correction context (schema, exploration, few-shots)...")
    context = _load_correction_context(instance_id, run_id, runs_root, schema_dir, logger)

    dialect_rules = "None"
    rules_file = Path(prompt_dir).parent / "generation" / "dialects" / f"{dialect}_rules.txt"
    if rules_file.exists():
        dialect_rules = rules_file.read_text(encoding="utf-8")

    # 3. Загрузка обязательных промптов
    if prompt_names is None:
        prompt_names = {"system": "cor_syntax_system", "user": "cor_syntax_user"}

    system_template = _load_prompt(prompt_names["system"])
    sys_prompt = fill_prompt_template(system_template, {
        "{{DIALECT}}": dialect,
        "{{DIALECT_OPTIMIZATION_RULES}}": dialect_rules
    })
    user_template = _load_prompt(prompt_names["user"])
    input_prompt = fill_prompt_template(user_template, {
        "{{QUESTION}}": question,
        "{{SCHEMA}}": context["schema"],
        "{{EXTERNAL_KNOWLEDGE}}": external_knowledge,
        "{{EXPLORATION_BLOCK}}": context["exploration_block"],
        "{{FEW_SHOT_EXAMPLES}}": context["few_shots"],
        "{{ORIGINAL_SQL}}": current_sql,
        "{{ERROR_MESSAGE}}": current_error,
        "{{FAILED_OPERATOR}}": _get_failed_operator(current_sql, current_error, dialect),
        "{{DIALECT}}": dialect
    })
    null_template = (_load_prompt(prompt_names["null"]) 
                     if prompt_names.get("null") else None)
    antiretry_template = (_load_prompt(prompt_names["antiretry"]) 
                          if prompt_names.get("antiretry") else None)
    
    results = {
        "instance_id": instance_id,
        "candidate_id": candidate_id,
        "db_id": db_id,
        "dialect": dialect,
        "original_sql": current_sql,
        "original_error": current_error,
        "attempts": [],
        "final_status": "processing",
        "final_sql": None,
        "metadata": {"start_time": start_time, "end_time": None}
    }
    
    prev_sql = current_sql
    is_null_logic = False
    turn = 0
    initial_messages = [
        SystemMessage(content=sys_prompt),
        HumanMessage(content=input_prompt)
    ]
    state_messages = []
    
    # 4. Цикл коррекции
    while turn < max_turns:
        turn += 1
        logger.info(f"--- Correction Attempt {turn}/{max_turns} ---")
        
        # Определение типа промпта
        if is_null_logic and null_template is not None:
            logger.info("Using NULL/Empty Result Logic Prompt.")
            usr_template = null_template
            prompt_vars = {
                "{{ORIGINAL_SQL}}": current_sql
            }
        elif turn > 1 and antiretry_template is not None and current_sql == prev_sql:
            logger.info("Using Anti-Retry Prompt.")
            usr_template = antiretry_template
            prompt_vars = {
                "{{CURRENT_SQL}}": current_sql,
                "{{ERROR_MESSAGE}}": current_error,
                "{{FAILED_OPERATOR}}": _get_failed_operator(current_sql, current_error, dialect),
                "{{DIALECT}}": dialect
            }
        elif turn > 1:
            logger.info("Using Standard Syntax Correction Prompt.")
            usr_template = """The SQL query is failed to execute.\nYou should fix it based on the current state below.\n\n## Current State\n- **Current failed SQL**:\n```sql\n{{CURRENT_SQL}}\n```\n- **Current Error**: {{ERROR_MESSAGE}}\n- **Failed Operator**: {{FAILED_OPERATOR}}"""
            prompt_vars = {
                "{{CURRENT_SQL}}": current_sql,
                "{{ERROR_MESSAGE}}": current_error,
                "{{FAILED_OPERATOR}}": _get_failed_operator(current_sql, current_error, dialect)
            }
        
        if turn > 1:
            user_prompt = fill_prompt_template(usr_template, prompt_vars)
            state_messages.append(HumanMessage(content=user_prompt))
        
        # Retry loop для вызова LLM
        llm_success = False
        response_text = ""
        for llm_retry in range(retry_config["max_attempts"]):
            delay = retry_config["initial_delay"] * (retry_config["backoff_multiplier"] ** llm_retry)
            delay = min(delay, retry_config["max_delay"])
            
            if llm_retry > 0:
                logger.info(f"LLM API retry {llm_retry + 1}/{retry_config['max_attempts']} after {delay:.2f}s delay")
                time.sleep(delay)
            
            try:
                pre_call_time = time.perf_counter()
                logger.info(f"Calling LLM (attempt {llm_retry+1})...")
                
                response = model.invoke(initial_messages + state_messages)
                response_text = response.content if hasattr(response, "content") else str(response)
                
                call_duration = time.perf_counter() - pre_call_time
                logger.info(f"LLM response received in {call_duration:.2f}s ({len(response_text)} chars)")
                llm_success = True
                break
            except Exception as e:
                logger.warning(f"LLM API call failed: {e}")
                
        if not llm_success:
            logger.error("LLM API max retries reached. Failing this correction step.")
            results["final_status"] = "failed_llm"
            break

        # Извлечение SQL
        corrected_sql = extract_sql_from_response(response_text, logger)
        if not corrected_sql:
            logger.error("Failed to extract SQL from LLM response.")
            current_error = "LLM returned empty or unparseable SQL"
            state_messages.append(AIMessage(content=response_text))
            continue
        
        logger.info(f"Extracted SQL:\n{corrected_sql}")
        
        # 5. Исполнение SQL
        logger.info(f"Executing corrected SQL on DB: {db_id}")
        exec_start = time.perf_counter()
        exec_error = None
        df = None
        try:
            exec_status, df = executor.thread_safe_sql_execution(
                sql=corrected_sql,
                db_name=db_id.split("_", 1)[1] if "_" in db_id else db_id,
                dialect=dialect
            )
            logger.info(f"Execution finished in {time.perf_counter() - exec_start:.2f}s. Status: {exec_status}")
        except Exception as e:
            exec_status = "error"
            exec_error = str(e)
            df = None
            logger.error(f"Execution exception: {exec_error}")
            
        # Сохранение артефактов попытки
        attempt_record = {
            "turn": turn,
            "is_null_logic_mode": is_null_logic,
            "messages": serialize_messages(initial_messages + state_messages), 
            "raw_response": response_text,
            "extracted_sql": corrected_sql,
            "execution_status": exec_status,
            "execution_duration_sec": round(time.perf_counter() - exec_start, 3),
            "error": exec_error if exec_status == "error" else None,
            "timestamp": time.time()
        }
        results["attempts"].append(attempt_record)
        
        # 6. Анализ результата
        if exec_status == "success":
            if _is_empty_or_null_result(exec_status, df):
                if not is_null_logic:
                    logger.warning("Query returned empty/NULL. Triggering logical investigation mode for next attempt.")
                    is_null_logic = True
                    prev_sql = current_sql
                    current_sql = corrected_sql
                    current_error = "Returned empty result or NULLs"
                    state_messages.append(AIMessage(content=response_text))
                    continue
                else:
                    logger.warning("Logical investigation also returned empty/NULL. Stopping.")
                    results["final_status"] = "success_empty"
                    results["final_sql"] = corrected_sql
                    break
            else:
                logger.info("SUCCESS: Query executed and returned valid data.")

                results["final_status"] = "success"
                results["final_sql"] = corrected_sql
                break
        else:
            logger.error(f"Execution failed with status: {exec_status}, error: {exec_error}")
            prev_sql = current_sql
            current_sql = corrected_sql
            current_error = exec_error
            is_null_logic = False # Сброс, так как теперь есть явная ошибка исполнения
        
        state_messages.append(AIMessage(content=response_text))
        if len(state_messages) // 2 >= max_messages:
            state_messages = state_messages[-max_messages*2:]
         
    # Финализация
    results["metadata"]["end_time"] = time.perf_counter()
    results["metadata"]["total_duration_sec"] = round(results["metadata"]["end_time"] - start_time, 3)
    
    # Сохранение артефактов кандидата
    cand_dir = base_dir / f"sql_{candidate_id:02d}"
    results_dir = base_dir / f"result_{candidate_id:02d}"
    cand_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)
    
    meta_path = results_dir / f"{instance_id}_meta.json"
    fd, tmp_path = tempfile.mkstemp(dir=str(results_dir), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as tmp:
            json.dump(results, tmp, ensure_ascii=False, indent=2)
        os.replace(tmp_path, str(meta_path))
    except Exception as e:
        logger.error(f"Failed to save meta.json: {e}")
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
            
    if results["final_sql"]:
        sql_path = cand_dir / f"{instance_id}.sql"
        sql_path.write_text(results["final_sql"], encoding="utf-8")

    if results["final_status"] == "success" and df is not None:
        df.to_csv(str(results_dir / f"{instance_id}.csv"), index=False, encoding="utf-8")
        
    logger.info(f"=== COMPLETED CORRECTION: {instance_id} | Status: {results['final_status']} ===")
    return results

def simple_correction(
    run_id: str,
    model: BaseChatModel,
    executor: SQLExecutor,
    tasks: Optional[List[Dict[str, Any]]] = None,
    prompt_dir: str = "config/prompts/correction",
    prompt_names: Optional[Dict[str, str]] = None,
    runs_root: str = "logs/runs",
    schema_dir: str = "final_schema",
    gen_prefix: str = "gen",
    max_messages: int = 5,
    max_turns: int = 3,
    max_workers: int = 2,
    retry_config: Dict[str, float] = DEFAULT_RETRY_CONFIG,
    **kwargs
) -> Dict[str, Any]:
    """Запускает пайплайн коррекции для множества примеров."""
    
    base_path = Path(runs_root) / run_id / "correction" / gen_prefix
    main_log = base_path / "main.log"
    main_log.parent.mkdir(parents=True, exist_ok=True)
    
    logger = get_logger("simple_corr", str(main_log))
    
    if tasks is None:
        logger.info("Loading failed candidates from generation manifests...")
        tasks = _load_failed_candidates(run_id, runs_root, gen_prefix)
        logger.info(f"Loaded {len(tasks)} instances with failed candidates.")
    else:
        logger.info(f"Starting with {len(tasks)} provided tasks.")

    all_results = {}
    start_pipeline = time.perf_counter()
    
    logger.info(f"Starting parallel correction with {max_workers} workers")
    
    # Разворачиваем задачи: один пример может иметь несколько кандидатов для исправления
    job_queue = []
    for task in tasks:
        for cand_id in task["corrections"].keys():
            job_queue.append((task, cand_id))
            
    logger.info(f"Total correction jobs to process: {len(job_queue)}")
    
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="CorrWorker") as pool:
        future_to_job = {
            pool.submit(
                correct_single_candidate,
                instance_data=task,
                candidate_id=cand_id,
                run_id=run_id,
                model=model,
                executor=executor,
                prompt_dir=prompt_dir,
                prompt_names=prompt_names,
                runs_root=runs_root,
                schema_dir=schema_dir,
                gen_prefix=gen_prefix,
                max_turns=max_turns,
                max_messages=max_messages,
                retry_config=retry_config
            ): (task["instance_id"], cand_id)
            for task, cand_id in job_queue
        }
        
        for future in as_completed(future_to_job):
            instance_id, cand_id = future_to_job[future]
            try:
                result = future.result()
                all_results[f"{instance_id}_c{cand_id}"] = result
                logger.info(f"Completed job: {instance_id} (Cand {cand_id}) | Status: {result['final_status']}")
            except Exception as e:
                logger.error(f"Failed job {instance_id} (Cand {cand_id}): {e}", exc_info=True)
                all_results[f"{instance_id}_c{cand_id}"] = {"instance_id": instance_id, "candidate_id": cand_id, "error": str(e)}
    
    # Сводная статистика
    total_duration = time.perf_counter() - start_pipeline
    stats = {
        "total_jobs": len(job_queue),
        "success": sum(1 for r in all_results.values() if r.get("final_status") == "success"),
        "success_empty": sum(1 for r in all_results.values() if r.get("final_status") == "success_empty"),
        "failed": sum(1 for r in all_results.values() if r.get("final_status") in ("failed_llm", "processing")),
        "total_duration_sec": round(total_duration, 2)
    }
    
    stats_path = base_path / "pipeline_stats.json"
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
        
    logger.info("=== CORRECTION PIPELINE FINISHED ===")
    logger.info(f"Stats: {json.dumps(stats, indent=2)}")
    
    return {"results": all_results, "stats": stats}


if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv(".env")

    parser = argparse.ArgumentParser(description="SQL correction from previous generation errors")
    
    # Обязательные
    parser.add_argument("--run-name", required=True, help="Название запуска")
    parser.add_argument("--model-name", required=True, help="Имя модели")
    
    # Модель
    parser.add_argument("--base-url", default=None, help="Base URL API модели")
    parser.add_argument("--api-key", default=None, help="API ключ модели")
    parser.add_argument("--temperature", type=float, default=0.2, help="Низкая температура для детерминированных исправлений")
    

    # Данные
    parser.add_argument("--run-root", default="logs/runs", help="Корень для результатов")
    parser.add_argument("--data-root", default="data", help="Корень входных данных")
    parser.add_argument("--storage-root", default="storage", help="Корень кэша схем")
    parser.add_argument("--input-data-root", default="Spider2/spider2-lite", help="Путь к данным")
    parser.add_argument("--gen-prefix", default="simple", help="Префикс папки генерации, откуда брать ошибки")
    parser.add_argument("--schema-dir", default="final_schema", help="Папка с схемами БД")
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

    # Промпты
    parser.add_argument("--prompt-dir", default="config/prompts/correction", help="Папка с промптами")
    parser.add_argument("--sys-prompt", default="cor_syntax_system", help="Системный промпт исправления SQL")
    parser.add_argument("--user-prompt", default="cor_syntax_user", help="Промпт с пользовательскими данными")
    parser.add_argument("--null-prompt", default="cor_syntax_null", help="Промпт для исправления пустых или null результатов")
    parser.add_argument("--ar-prompt", default="cor_syntax_antiretry", help="Промпт для предотвращения безрезультатных попыток")

    # Выполнение
    parser.add_argument("--max-workers", type=int, default=2, help="Параллельных воркеров")
    
    # Retry
    parser.add_argument("--max-turns", type=int, default=3, help="Макс. попыток коррекции")
    parser.add_argument("--max-messages", type=int, default=5, help="Макс. число сообщений в контексте LLM")
    parser.add_argument("--max-attempts", type=int, default=3, help="Макс. попыток генерации")
    parser.add_argument("--initial-delay", type=float, default=2.0, help="Начальная задержка (сек)")
    parser.add_argument("--max-delay", type=float, default=30.0, help="Макс. задержка (сек)")
    
    args = parser.parse_args()
    
    # Инициализация
    model = get_model(
        model_name=args.model_name,
        base_url=args.base_url,
        api_key=args.api_key,
        temperature=args.temperature
    )
    
    executor = SQLExecutor(
        input_data_root=args.input_data_root,
        data_root=args.data_root,
        storage_root=args.storage_root,
        local_dbs=dict(args.local_dbs) if args.local_dbs else None 
    )
    
    prompt_names = {
        "system": args.sys_prompt,
        "user": args.user_prompt,
        "null": args.null_prompt,
        "antiretry": args.ar_prompt
    }

    retry_config = {
        "max_attempts": args.max_attempts,
        "initial_delay": args.initial_delay,
        "max_delay": args.max_delay,
        "backoff_multiplier": 2.0,
    }
    
    run_id = resolve_run_id(args.run_root, args.input_data_root, args.run_name)
    
    print(f"\nStarting correction pipeline for run: {run_id}")
    output = simple_correction(
        run_id=run_id,
        model=model,
        executor=executor,
        prompt_dir=args.prompt_dir,
        prompt_names=prompt_names,
        runs_root=args.run_root,
        schema_dir=args.schema_dir,
        gen_prefix=args.gen_prefix,
        max_messages=args.max_messages,
        max_turns=args.max_turns,
        max_workers=args.max_workers,
        retry_config=retry_config
    )
    
    stats = output.get("stats", {})
    print("\nРезультаты коррекции:")
    print(f"  Всего задач:     {stats.get('total_jobs', 0)}")
    print(f"  Успешно:         {stats.get('success', 0)}")
    print(f"  Успешно (пусто): {stats.get('success_empty', 0)}")
    print(f"  Ошибки:          {stats.get('failed', 0)}")
    print(f"  Время:           {stats.get('total_duration_sec', 0):.2f} сек")
