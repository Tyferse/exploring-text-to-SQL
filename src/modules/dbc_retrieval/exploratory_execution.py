import sys
sys.path.insert(0, ".")

import json
import time
import logging
import concurrent.futures
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Union

from langchain_core.language_models import BaseChatModel

from src.utils.logger import get_logger
from src.utils.models import get_model
from src.utils.preprocessing import remove_digits
from src.utils.run_manager import resolve_run_id
from src.utils.sql_execution import SQLExecutor, parse_dialect_path_pair, df_to_markdown


DEFAULT_RETRY_CONFIG = {
    "max_attempts": 4,
    "initial_delay": 2.0,
    "max_delay": 30.0,
    "backoff_multiplier": 2.0,
}

def _load_instances(
    schemas_dir: str,
    results_dir: str,
    tasks: Optional[Union[List[Dict[str, Any]], str]] = None,
    data_root: str = "data",
    input_data_root: Optional[str] = None
) -> Dict[str, Dict[str, Any]]:
    """Загрузка задач из файла."""
    assert tasks is not None or input_data_root is not None, "tasks or input_data_root argument must be not None"

    # Ищем JSON файл с задачами
    if tasks is None or isinstance(tasks, str):
        if isinstance(tasks, str):
            tasks_file = tasks
        else:
            tasks_file = (data_root / input_data_root).glob("*.jsonl")[0]

        with open(tasks_file, "r", encoding="utf-8") as f:
            tasks = [json.loads(line.strip()) for line in f.readlines()]
    
    tasks = {
        instance["instance_id"]: {
            "instance_id": instance["instance_id"],
            "dialect": instance.get("dialect", ""),
            "db_id": instance.get("dialect", "") + ("_" if instance.get("dialect") else "") + instance["db_id"], 
            "question": instance.get("question", instance.get("instruction", ""))
        } 
        for instance in tasks if not (Path(results_dir) / f"{instance['instance_id']}.json").exists()
    }
    if input_data_root == "Spider2/spider2-lite":
        inst2dialect = {"sf": "snowflake", "bq": "bigquery", "ga": "bigquery", "local": "sqlite"}
        for iid in tasks:
            tasks[iid]["dialect"] = inst2dialect[remove_digits(iid).split("_")[0]]
            tasks[iid]["db_id"] = tasks[iid]["dialect"] + "_" + tasks[iid]["db_id"]

    for iid in tasks:
        schema_file = Path(schemas_dir, f"{iid}.txt")
        if schema_file.exists(): 
            tasks[iid]["schema"] = schema_file.read_text(encoding="utf-8")
        else:
            tasks[iid]["schema"] = "None"

    return tasks


def _build_prompt_from_template(
    template_path: Path,
    rules_path: Path,
    specifics_path: Path,
    replacements: Dict[str, str]
) -> str:
    """
    Сборка промпта через .replace() из шаблона и файлов правил.
    replacements: dict {placeholder: value}, где placeholder включает {{}}
    """
    # Загрузка базовых компонентов
    template = template_path.read_text(encoding='utf-8')
    rules_text = rules_path.read_text(encoding='utf-8')
    specifics_text = specifics_path.read_text(encoding='utf-8')
    
    # Добавляем правила в replacements
    replacements["{{DIALECT_RULES}}"] = rules_text
    replacements["{{DIALECT_SPECIFICS}}"] = specifics_text
    
    # Последовательная замена всех плейсхолдеров
    prompt = template
    for placeholder, value in replacements.items():
        if placeholder in prompt:
            prompt = prompt.replace(placeholder, str(value))
    
    return prompt


def _parse_sql_queries(response_text: str, max_queries: int, max_rows: int) -> List[Dict[str, str]]:
    """
    Парсинг ответа модели: извлечение пар (описание, SQL).
    Добавляет LIMIT, если отсутствует.
    """
    queries = []
    # Паттерн: – Description: ... \n ```sql ... ```
    pattern = r"–\s*Description:\s*(.*?)\n\s*```sql\s*(.*?)\s*```"
    matches = re.findall(pattern, response_text, re.DOTALL | re.IGNORECASE)
    
    for desc, sql in matches:
        clean_sql = sql.strip()
        if "LIMIT" not in clean_sql.upper():
            clean_sql = f"{clean_sql.rstrip(';')} LIMIT {max_rows};"
        queries.append({"description": desc.strip(), "sql": clean_sql})
    
    # Fallback: поиск любых SQL блоков
    if not queries:
        sql_blocks = re.findall(r"```sql\s*(.*?)\s*```", response_text, re.DOTALL)
        for i, sql in enumerate(sql_blocks[:max_queries]):
            clean_sql = sql.strip()
            if "LIMIT" not in clean_sql.upper():
                clean_sql = f"{clean_sql.rstrip(';')}\nLIMIT {max_rows};"
            queries.append({"description": f"Auto-query {i+1}", "sql": clean_sql})
    
    return queries[:max_queries]


def _format_query_result(status: str, result_df: Any, sql: str, description: str) -> str:
    """Форматирование результата выполнения запроса в строку."""
    if status == "empty":
        return f"[Query]: {description}\nSQL: {sql}\nResult: <empty>\n"
    
    if status == "error":
        return f"[Query]: {description}\nSQL: {sql}\nResult: <execution_error>\n"
    
    # status == "success": форматирование DataFrame в markdown-таблицу
    if hasattr(result_df, 'to_markdown'):
        table_str = df_to_markdown(result_df)
    else:
        # Fallback для списка словарей
        table_str = json.dumps(result_df, ensure_ascii=False, indent=2) if result_df else "<no_data>"
    
    return f"[Query]: {description}\nSQL: {sql}\nResult:\n{table_str}\n"


def _process_single_example(
    instance_id: str,
    task: Dict[str, Any],
    model: BaseChatModel, 
    executor: SQLExecutor,
    runs_root: str,
    run_id: str,
    prompt_dir: str,
    prompt_name: str,
    max_queries: int = 10,
    max_rows: int = 20,
    retry_config: Dict[str, Any] = DEFAULT_RETRY_CONFIG
) -> List[Dict[str, str]]:
    """
    Обработка одного примера: генерация запросов, выполнение, сохранение.
    Возвращает список из 3 сообщений для последующего использования.
    """
    # Извлечение параметров из задачи
    iid = instance_id
    db_id = task["db_id"]
    db_name = db_id.split("_", 1)[1] if "_" in db_id else db_id
    dialect = task.get("dialect", "sqlite")
    question = task["question"]
    schema_text = task.get("schema", "None")
    
    # Настройка логгера
    log_dir = Path(runs_root) / run_id / "dbc_retrieval"
    log_file = log_dir / "exec_exploration_logs" / f"{iid}.log"
    logger = get_logger(f"exec_explore_{iid}", log_file)
    
    logger.info(f"=== Starting processing for example {iid} ===")
    logger.debug(f"Task params: dialect={dialect}, db_id={db_id}, question_len={len(question)}")
    
    # Подготовка путей к промптам
    prompt_base = Path(prompt_dir)
    template_path = prompt_base / f"{prompt_name}.md"
    rules_path = prompt_base / "dialects" / f"{dialect}_rules.txt"
    specifics_path = prompt_base / "dialects" / f"{dialect}_specifics.txt"
    
    # Формирование replacements для промпта
    replacements = {
        "{{DIALECT}}": dialect,
        "{{MAX_QUERIES}}": max_queries,
        "{{MAX_ROWS}}": max_rows,
        "{{QUESTION}}": question,
        "{{SCHEMA}}": schema_text,
    }
    
    # 1. Сборка промпта
    prompt = _build_prompt_from_template(
        template_path, rules_path, specifics_path, replacements
    )

    output_dir = log_dir / "exec_exploration_results"
    output_dir.mkdir(parents=True, exist_ok=True)

    last_error = None
    for attempt in range(retry_config['max_attempts'] + 1):
        if attempt > 0:
            logger.warning(f"Retry attempt {attempt}/{retry_config['max_attempts']} for example {iid}")
            delay = min(
                retry_config["initial_delay"] * 
                (retry_config["backoff_multiplier"] ** (attempt - 1)),
                retry_config["max_delay"]
            )
            time.sleep(delay)
        
        try:
            # 2. Вызов модели
            logger.info(f"[Model Call] Start for example {iid}, attempt {attempt+1}")
            t_model_start = time.perf_counter()
            
            response = model.invoke(prompt)
            response_text = response.content if hasattr(response, 'content') else str(response)
            
            t_model_end = time.perf_counter()
            logger.info(f"[Model Call] Finished. Duration: {t_model_end - t_model_start:.3f}s")
            
            # 3. Парсинг запросов
            parsed_queries = _parse_sql_queries(
                response_text, 
                max_queries=int(replacements["{{MAX_QUERIES}}"]),
                max_rows=int(replacements["{{MAX_ROWS}}"])
            )
            logger.info(f"Parsed {len(parsed_queries)} queries from model response")
            
            if not parsed_queries:
                raise ValueError("No valid SQL queries parsed from model response")
            
            # 4. Выполнение запросов с логированием
            logger.info(f"[SQL Execution Loop] Start for {len(parsed_queries)} queries")
            t_exec_loop_start = time.time()
            
            results_output = []
            successful_count = 0
            
            for i, q in enumerate(parsed_queries, 1):
                sql = q['sql']
                desc = q['description']
                logger.info(f"[SQL #{i}] Executing: {sql[:100].replace(chr(10), ' ')}...")
                t_query_start = time.perf_counter()
                
                # Потокобезопасное выполнение
                status, result_df = executor.thread_safe_sql_execution(
                    sql=sql,
                    db_name=db_name,
                    dialect=dialect
                )
                
                t_query_end = time.perf_counter()
                logger.info(f"[SQL #{i}] Status: {status}. Duration: {t_query_end - t_query_start:.3f}s")
                
                if status == "success":
                    successful_count += 1
                
                # Форматируем результат (ошибки тоже можно логировать, но не добавлять в финальный вывод)
                if status != "error":
                    results_output.append(_format_query_result(status, result_df, sql, desc))
            
            t_exec_loop_end = time.perf_counter()
            logger.info(f"[SQL Execution Loop] Finished. Duration: {t_exec_loop_end - t_exec_loop_start:.3f}s. Success: {successful_count}/{len(parsed_queries)}")
            
            # 5. Проверка: если ни один запрос не выполнен успешно -> выбросить исключение для ретрая
            if successful_count == 0:
                raise RuntimeError("No queries executed successfully")
            
            # 6. Формирование сообщений
            messages = [
                {"role": "user", "content": prompt}, 
                {"role": "assistant", "content": response_text}, 
                {
                    "role": "user", 
                    "content": "Results of exploratory queries:\n\n" + "\n".join(results_output)
                }
            ]
            
            # 7. Сохранение
            output_path = output_dir / f"{iid}.json"
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(messages, f, ensure_ascii=False, indent=2)

            logger.info(f"Results saved to {output_path}")
            logger.info(f"=== Successfully completed example {iid} ===")
            return messages
            
        except Exception as e:
            last_error = e
            logger.error(f"Attempt {attempt+1} failed for {iid}: {e}", exc_info=True)
            continue
    
    # Если все ретраи исчерпаны
    logger.error(f"=== FAILED after {retry_config['max_attempts']+1} attempts for example {iid}: {last_error} ===")
    
    # Возвращаем заглушку с ошибкой, чтобы пайплайн не падал
    return [
        {"role": "user", "content": f"Processing failed for {iid}"},
        {"role": "assistant", "content": f"Error: {str(last_error)}"},
        {"role": "user", "content": "No results available."}
    ]


def exec_exploration(
    run_id: str,
    model: BaseChatModel,
    executor: SQLExecutor,
    tasks: Optional[List[Dict[str, Any]]] = None,
    runs_root: str = "logs/runs",
    max_workers: int = 4,
    data_root: str = "data",
    input_data_root: str = "Spider2/spider2-lite",
    prompt_dir: str = "config/prompts/dbc_retrieval",
    prompt_name: str = "exploratory_execution",
    max_queries: int = 10,
    max_rows: int = 20,
    retry_config: Dict[str, Any] = DEFAULT_RETRY_CONFIG,
    **kwargs
) -> Dict[str, List[Dict[str, str]]]:
    """
    Главная функция модуля.
    
    Args:
        runs_root: Корневая директория запуска
        run_id: ID текущего запуска (для организации подпапок)
        model_name: Название модели для инициализации
        ... (остальные аргументы как описано выше)
        tasks: Опциональный список задач. Если None, загружается из файла.
    
    Returns:
        Dict[example_id -> messages_list]
    """
    # 1. Инициализация логгера для оркестратора
    main_log_dir = Path(runs_root) / run_id / "dbc_retrieval"
    main_log_dir.mkdir(parents=True, exist_ok=True)
    logger = get_logger("exec_exploration", main_log_dir / "exec_explore.log")
    logger.info(f"Starting DB Content Retrieval.")
    
    # 2. Загрузка/подготовка задач
    schema_dir = main_log_dir.parent / "schema_linking" / "final_schema"
    if not schema_dir.exists():
        schema_dir = main_log_dir.parent / "schema_linking" / "initial_schema"

    tasks = _load_instances(schema_dir, str(main_log_dir / "exec_exploration_results"), tasks, data_root, input_data_root)
    if not tasks:
        logger.error("No tasks found to process")
        return {}
    
    logger.info(f"Loaded {len(tasks)} tasks")
    
    # 3. Параллельное выполнение
    results = {}
    logger.info(f"Starting parallel execution with {max_workers} workers")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_id = {
            pool.submit(
                _process_single_example,
                iid,
                task=tasks[iid],
                model=model,
                executor=executor,
                runs_root=runs_root,
                run_id=run_id,
                prompt_dir=prompt_dir,
                prompt_name=prompt_name,
                max_queries=max_queries,
                max_rows=max_rows,
                retry_config=retry_config
            ): iid for iid in tasks}
        
        # Сбор результатов по мере завершения
        for future in concurrent.futures.as_completed(future_to_id):
            iid = future_to_id[future]
            try:
                messages = future.result()
                results[iid] = messages
                logger.info(f"Completed: {iid}")
            except Exception as e:
                logger.error(f"Future failed for {iid}: {e}", exc_info=True)
                # Добавляем заглушку
                results[iid] = [
                    {"role": "user", "content": f"Error processing {iid}"},
                    {"role": "assistant", "content": str(e)},
                    {"role": "user", "content": "No results"}
                ]
    
    logger.info(f"=== DB Content Retrieval finished. Processed {len(results)} examples ===")
    return results


if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv(".env")
    
    parser = argparse.ArgumentParser(
        description="Database Content Retrieval: exploratory SQL generation and execution",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Обязательные аргументы
    parser.add_argument(
        "--run_name", "-r", 
        type=str, 
        required=True,
        help="Run name (used for output subdirectories)"
    )
    parser.add_argument(
        "--model_name", "-m", 
        type=str, 
        default="qwen-local",
        help="Model name for initialization via get_model()"
    )
    
    # Опции модели
    parser.add_argument(
        "--model_base_url", 
        type=str, 
        default=None,
        help="Base URL for model API (for local/remote OpenAI-compatible endpoints)"
    )
    parser.add_argument(
        "--model_api_key", 
        type=str, 
        default=None,
        help="API key for model authentication"
    )
    parser.add_argument(
        "--temperature", 
        type=float, 
        default=0.1,
        help="Sampling temperature for the model"
    )
    
    # Опции выполнения
    parser.add_argument(
        "--max_queries",
        type=int, 
        default=10,
        help="Maximal namber of generated SQL queries"
    )
    parser.add_argument(
        "--max_rows",
        type=int, 
        default=4,
        help="Maximal namber of rows in generated SQL queries execution results"
    )
    parser.add_argument(
        "--max_workers",
        type=int, 
        default=4,
        help="Number of parallel worker threads"
    )
    parser.add_argument(
        "--max_retries", 
        type=int, 
        default=4,
        help="Maximum retry attempts for failed examples (exponential backoff)"
    )
    parser.add_argument(
        "--initial_retry_delay", 
        type=float, 
        default=2.0,
        help="Initial delay in seconds before first retry"
    )
    parser.add_argument(
        "--max_retry_delay", 
        type=float, 
        default=30.0,
        help="Maximum delay cap for exponential backoff"
    )

    # Параметры исполнения запросов
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
    parser.add_argument(
        "--exec-timeout", type=float, default=600, 
        help="Максимальное время ожидания исполнения SQL в секундах"
    )
    
    # Пути к данным
    parser.add_argument(
        "--runs_root", 
        type=str, 
        default="logs/runs",
        help="Root directory for run outputs and logs"
    )
    parser.add_argument(
        "--data_root", 
        type=str, 
        default=".",
        help="Root directory containing input data"
    )
    parser.add_argument(
        "--input_data_root", 
        type=str, 
        default="input",
        help="Subdirectory under data_root containing input JSONL files"
    )
    parser.add_argument(
        "--prompt_dir", 
        type=str, 
        default="config/prompts/dbc_retrieval",
        help="Directory containing prompt templates and dialect rules"
    )
    parser.add_argument(
        "--prompt_name", 
        type=str, 
        default="exploratory_execution",
        help="Base name of the main prompt template file (without extension)"
    )
    
    # Логирование
    parser.add_argument(
        "--debug", 
        action="store_true",
        help="Enable debug-level logging"
    )
    
    args = parser.parse_args()
    
    # === Инициализация ===
    
    # Инициализация модели
    print(f"[INIT] Loading model: {args.model_name}")
    model = get_model(
        model_name=args.model_name,
        base_url=args.model_base_url,
        api_key=args.model_api_key,
        temperature=args.temperature
    )
    
    # Инициализация SQL Executor
    executor = SQLExecutor(
        args.input_data_root, args.data_root, args.storage_root, 
        dict(args.local_dbs) if args.local_dbs else None, args.exec_timeout
    )
    
    # Сборка retry_config
    retry_config = {
        "max_attempts": args.max_retries,
        "initial_delay": args.initial_retry_delay,
        "max_delay": args.max_retry_delay,
        "backoff_multiplier": 2.0,
    }

    # === Запуск ===
    print(f"[START] Run ID: {args.run_id} | Workers: {args.max_workers}")
    print(f"[START] Input: {Path(args.data_root) / args.input_data_root}")
    print(f"[START] Output: {Path(args.runs_root) / args.run_id / 'dbc_retrieval'}")
    
    try:
        run_id = resolve_run_id(args.runs_root, args.input_data_root, args.run_name)
        results = exec_exploration(
            run_id=run_id,
            model=model,
            executor=executor, 
            runs_root=args.runs_root,
            max_workers=args.max_workers,
            data_root=args.data_root,
            input_data_root=args.input_data_root,
            prompt_dir=args.prompt_dir,
            prompt_name=args.prompt_name,
            max_queries=args.max_queries,
            max_rows=args.max_rows,
            retry_config=retry_config,
        )
        
        # Итоговая статистика
        success_count = sum(
            1 for msgs in results.values() 
            if len(msgs) == 3 and "Results of exploratory queries" in msgs[2]["content"]
        )
        print(f"\n[FINISHED] Processed {len(results)} examples | Successful: {success_count}")
        
        # Выход с кодом ошибки, если были неудачи
        sys.exit(0 if success_count == len(results) else 1)
        
    except KeyboardInterrupt:
        print("\n[INTERRUPT] Execution cancelled by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n[ERROR] Unhandled exception: {e}", file=sys.stderr)
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)
