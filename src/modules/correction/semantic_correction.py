import sys
sys.path.insert(0, ".")

import os
import re
import json
import time
import logging
import tempfile
from pathlib import Path
from typing import Dict, Any, Literal, Optional, List, Tuple, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, AIMessage

from src.utils.logger import get_logger
from src.utils.models import get_model
from src.utils.preprocessing import fill_prompt_template
from src.utils.run_manager import resolve_run_id
from src.utils.sql_execution import SQLExecutor, parse_dialect_path_pair, df_to_markdown


DEFAULT_RETRY_CONFIG = {
    "max_attempts": 4,
    "initial_delay": 2.0,
    "max_delay": 30.0,
    "backoff_multiplier": 2.0,
}


def _load_prompt(prompt_name: str, prompt_dir: str = "config/prompts/semantic") -> str:
    prompt_path = Path(prompt_dir) / f"{prompt_name}.md"
    if not prompt_path.exists():
        raise FileNotFoundError(f"Prompt template {prompt_name} not found in {prompt_dir}")
    return prompt_path.read_text(encoding="utf-8")

def _find_artifact_path(instance_id: str, candidate_id: int, run_id: str, runs_root: str, gen_prefix: str, artifact_type: str) -> Optional[Path]:
    cand_str = f"{candidate_id:02d}"
    if artifact_type == "sql":
        rel_path = Path(f"correction/{gen_prefix}/sql_{cand_str}/{instance_id}.sql")
        fallback_path = Path(f"generation/{gen_prefix}/sql_{cand_str}/{instance_id}.sql")
    elif artifact_type == "csv":
        rel_path = Path(f"correction/{gen_prefix}/result_{cand_str}/{instance_id}.csv")
        fallback_path = Path(f"generation/{gen_prefix}/result_{cand_str}/{instance_id}.csv")
    elif artifact_type == "meta":
        rel_path = Path(f"correction/{gen_prefix}/result_{cand_str}/{instance_id}_meta.json")
        fallback_path = Path(f"generation/{gen_prefix}/result_{cand_str}/{instance_id}_meta.json")
    else:
        return None
    
    primary = Path(runs_root) / run_id / rel_path
    if primary.exists(): return primary
    fallback = Path(runs_root) / run_id / fallback_path
    if fallback.exists(): return fallback
    return None

def _load_df_from_csv(csv_path: Optional[Path]) -> Optional[pd.DataFrame]:
    if csv_path is None or not csv_path.exists(): return None
    try:
        content = csv_path.read_text(encoding="utf-8").strip()
        if content.startswith("# Empty result"): return pd.DataFrame()
        return pd.read_csv(csv_path, encoding="utf-8")
    except Exception: return None

def parse_classification_response(response_text: str) -> Dict[str, Any]:
    json_match = re.search(r'\{[\s\S]*\}', response_text)
    if json_match:
        try:
            result = json.loads(json_match.group(0))
            if "verdict" in result and result["verdict"] in ("VALID", "INVALID"):
                result["reasons"] = result.get("reasons", [])
                return result
        except json.JSONDecodeError: pass
    return {"verdict": "INVALID", "reasons": ["Model failed to return valid JSON classification"]}

def extract_think_answer(response_text: str) -> Tuple[str, str]:
    think_match = re.search(r'<think>([\s\S]*?)</think>', response_text, re.IGNORECASE)
    answer_match = re.search(r'<answer>([\s\S]*?)</answer>', response_text, re.IGNORECASE)
    return (think_match.group(1).strip() if think_match else ""), (answer_match.group(1).strip() if answer_match else response_text)

def extract_sql_from_response(response_text: str) -> Optional[str]:
    match = re.search(r"```sql\s*(.*?)\s*```", response_text, re.DOTALL | re.IGNORECASE)
    if match and match.group(1).strip(): return match.group(1).strip()
    lines = response_text.strip().split("\n")
    sql_lines, in_sql = [], False
    for line in lines:
        stripped = line.strip()
        if re.match(r"^(SELECT|WITH)\b", stripped, re.IGNORECASE): in_sql = True
        if in_sql and stripped and not stripped.startswith("```"): sql_lines.append(line)
        if stripped.endswith("```") and in_sql: break
    return "\n".join(sql_lines).strip() or None

def _load_external_knowledge_path(
    tasks: Optional[Union[List[Dict[str, Any]], str]] = None, 
    input_data_root: str = "Spider2/spider2-lite", 
    data_root: str = "data"
) -> List[str]:
    if tasks is None or isinstance(tasks, str):
        if isinstance(tasks, str):
            tasks_file = tasks
        else:
            tasks_file = (data_root / input_data_root).glob("*.jsonl")[0]

        with open(tasks_file, "r", encoding="utf-8") as f:
            taskl = [json.loads(line.strip()).get("external_knowledge") for line in f.readlines()]
    
    ek_paths = {instance["instance_id"]: instance.get("external_knowledge") for instance in taskl}

    return {iid: str(Path(data_root) / input_data_root / "resource" / "documents" / file) if file else None 
            for iid, file in ek_paths.items()}

def _load_semantic_context(
    instance_id: str, candidate_id: int, run_id: str, runs_root: str, 
    gen_prefix: str, schema_dir: str, ek_path: Optional[str] = None, 
    logger: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    runs_path = Path(runs_root) / run_id
    schema_path = runs_path / "schema_linking" / schema_dir / f"{instance_id}.txt"
    schema = schema_path.read_text(encoding="utf-8") if schema_path.exists() else "Schema not found."
    
    external_knowledge = "None"
    if ek_path is not None and Path(ek_path).exists():
        external_knowledge = Path(ek_path).read_text(encoding="utf-8")

    meta_path = _find_artifact_path(instance_id, candidate_id, run_id, runs_root, gen_prefix, "meta")
    question, dialect, db_id = "Unknown question", "sqlite", instance_id
    current_sql = ""
    
    if meta_path and meta_path.exists():
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta_data = json.load(f)

            question = meta_data.get("question", "Unknown question")
            dialect = meta_data.get("dialect", "sqlite")
            db_id = meta_data.get("db_id", instance_id)
            current_sql = meta_data.get("final_sql", "") or meta_data.get("candidates", {}).get(f"{candidate_id:02d}", {}).get("generation", {}).get("sql", "")
        except Exception as e: logger.warning(f"Failed to load meta for {instance_id}: {e}")
    
    csv_path = _find_artifact_path(instance_id, candidate_id, run_id, runs_root, gen_prefix, "csv")
    df = _load_df_from_csv(csv_path)
    
    return {
        "schema": schema, "question": question, "dialect": dialect, 
        "db_id": db_id, "external_knowledge": external_knowledge,
        "current_sql": current_sql, "execution_result": df_to_markdown(df), "df": df
    }

def _load_semantic_candidates(run_id: str, runs_root: str = "logs/runs", gen_prefix: str = "simple") -> List[Dict[str, Any]]:
    tasks, processed_pairs = [], set()
    for base_name in ["correction", "generation"]:
        base = Path(runs_root) / run_id / base_name / gen_prefix
        if not base.exists(): continue
        for result_dir in base.glob("result_*"):
            for meta_file in result_dir.glob("*_meta.json"):
                try:
                    with open(meta_file, "r", encoding="utf-8") as f: meta = json.load(f)
                    instance_id, candidate_id = meta.get("instance_id"), meta.get("candidate_id")

                    if (Path(runs_root) / run_id / "correction" / gen_prefix / f"valid_result_{candidate_id:02d}" / f"{instance_id}.csv").exists():
                        processed_pairs.add((instance_id, candidate_id))

                    if (instance_id, candidate_id) in processed_pairs: continue
                    
                    # Проверка успеха в зависимости от структуры
                    is_success = False
                    if base_name == "correction":
                        is_success = meta.get("final_status") in ("success", "success_empty") and meta.get("final_sql")
                    else:
                        cand_str = f"{candidate_id:02d}"
                        cand_data = meta.get("candidates", {}).get(cand_str, {})
                        is_success = cand_data.get("execution", {}).get("status") in ("success", "empty") and cand_data.get("generation", {}).get("sql")
                    
                    if is_success:
                        processed_pairs.add((instance_id, candidate_id))
                        tasks.append({
                            "instance_id": instance_id, "candidate_id": candidate_id,
                            "db_id": meta.get("db_id"), "dialect": meta.get("dialect", "sqlite"),
                            "question": meta.get("question", ""), "source": base_name
                        })

                except Exception: 
                    continue
    
    return tasks


def run_classification_check(
    model: BaseChatModel,
    dialect:str, question: str, schema: str,
    current_sql: str, execution_result: str,
    prompt_template: str, retry_config: Dict[str, float], 
    external_knowledge: Optional[str] = None,
    logger: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    """Только классификация. Возвращает вердикт и причины."""
    prompt = fill_prompt_template(prompt_template, {
        "{{DIALECT}}": dialect, "{{QUESTION}}": question, 
        "{{SCHEMA}}": schema, "{{EXTERNAL_KNOWLEDGE}}": external_knowledge,
        "{{EXECUTED_SQL}}": current_sql, "{{EXECUTION_RESULT}}": execution_result
    })
    messages = [HumanMessage(content=prompt)]
    
    for llm_retry in range(retry_config["max_attempts"]):
        delay = min(retry_config["initial_delay"] * (retry_config["backoff_multiplier"] ** llm_retry), retry_config["max_delay"])
        if llm_retry > 0:
            if logger: logger.info(f"Classification LLM retry {llm_retry + 1} after {delay:.2f}s")
            time.sleep(delay)

        try:
            response = model.invoke(messages)
            response_text = response.content if hasattr(response, "content") else str(response)
            return {
                "messages": messages, "raw_response": response_text,
                "result": parse_classification_response(response_text), "success": True
            }
        except Exception as e:
            if logger: logger.warning(f"Classification LLM call failed: {e}")
            
    return {"messages": messages, "raw_response": "", "result": {"verdict": "INVALID", "reasons": ["LLM API failed"]}, "success": False}


def run_unified_validation(
    model: BaseChatModel,
    question: str, schema: str, current_sql: str, execution_result: str, reasons: str,
    dialect: str, dialect_rules: str, prompt_template: str, retry_config: Dict[str, float], 
    external_knowledge: Optional[str] = None, logger: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    """Единый промпт исправления. Возвращает исправленный SQL и рассуждения."""
    prompt = fill_prompt_template(prompt_template, {
        "{{QUESTION}}": question, "{{SCHEMA}}": schema, 
        "{{EXTERNAL_KNOWLEDGE}}": external_knowledge, "{{CURRENT_SQL}}": current_sql,
        "{{EXECUTION_RESULT}}": execution_result, "{{VALIDATION_REASONS}}": reasons,
        "{{DIALECT}}": dialect, "{{DIALECT_OPTIMIZATION_RULES}}": dialect_rules
    })
    messages = [HumanMessage(content=prompt)]
    
    for llm_retry in range(retry_config["max_attempts"]):
        delay = min(retry_config["initial_delay"] * (retry_config["backoff_multiplier"] ** llm_retry), retry_config["max_delay"])
        
        if llm_retry > 0:
            if logger: logger.info(f"Unified Validation LLM retry {llm_retry + 1} after {delay:.2f}s")
            time.sleep(delay)

        try:
            response = model.invoke(messages)
            response_text = response.content if hasattr(response, "content") else str(response)
            think_text, answer_text = extract_think_answer(response_text)
            corrected_sql = extract_sql_from_response(answer_text)
            return {
                "messages": messages, "raw_response": response_text,
                "think": think_text, "answer": answer_text, "corrected_sql": corrected_sql, "success": True
            }
        except Exception as e:
           if logger: logger.warning(f"Unified Validation LLM call failed: {e}")
            
    return {"messages": messages, "raw_response": "", "corrected_sql": None, "success": False}


def run_double_validation(
    model: BaseChatModel, executor: SQLExecutor,
    question: str, db_name: str, schema: str, current_sql: str, execution_result: str, reasons: str,
    dialect: str, dialect_rules: str, rules_prompt_template: str, output_prompt_template: str,
    retry_config: Dict[str, float], external_knowledge: Optional[str] = None, 
    logger: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    """Двухэтапное исправление: сначала правила, затем формат вывода."""
    all_messages = []
    
    # Этап 1: Правила
    rules_prompt = fill_prompt_template(rules_prompt_template, {
        "{{QUESTION}}": question, "{{SCHEMA}}": schema, "{{CURRENT_SQL}}": current_sql,
        "{{EXTERNAL_KNOWLEDGE}}": external_knowledge,
        "{{EXECUTION_RESULT}}": execution_result, "{{DERIVED_RULES}}": reasons,
        "{{DIALECT}}": dialect, "{{DIALECT_OPTIMIZATION_RULES}}": dialect_rules
    })
    messages_1 = [HumanMessage(content=rules_prompt)]
    sql_step1 = None
    
    for llm_retry in range(retry_config["max_attempts"]):
        delay = min(retry_config["initial_delay"] * (retry_config["backoff_multiplier"] ** llm_retry), retry_config["max_delay"])
        if llm_retry > 0: 
            if logger: logger.info(f"Rules Validation LLM retry {llm_retry + 1} after {delay:.2f}s")
            time.sleep(delay)
        try:
            response = model.invoke(messages_1)
            response_text = response.content if hasattr(response, "content") else str(response)
            think_text, answer_text = extract_think_answer(response_text)
            sql_step1 = extract_sql_from_response(answer_text)
            all_messages.append({"step": 1, "messages": messages_1, "raw_response": response_text, "think": think_text, "answer": answer_text})
            break
        except Exception as e:
            if logger: logger.warning(f"Double Validation Rules Step LLM call failed: {e}")
            
    if not sql_step1:
        return {"all_messages": all_messages, "final_sql": None, "success": False}

    # Исполняем запрос
    logger.info(f"Executing corrected SQL on DB: {db_name}")
    exec_start = time.perf_counter()
    exec_error = None
    df = None
    try:
        exec_status, df = executor.thread_safe_sql_execution(
            sql=sql_step1, db_name=db_name, dialect=dialect
        )
        exec_duration = time.perf_counter() - exec_start
        logger.info(f"Execution finished in {exec_duration:.2f}s. Status: {exec_status}")
    except Exception as e:
        exec_status, exec_error, df = "error", str(e), None
        exec_duration = time.perf_counter() - exec_start
        logger.error(f"Execution exception: {exec_error}")
        
    all_messages[-1]["execution"] = {
        "status": exec_status, "duration_sec": round(exec_duration, 3), "error": exec_error
    }

    # Этап 2: Формат вывода (берем SQL из шага 1)
    output_prompt = fill_prompt_template(output_prompt_template, {
        "{{QUESTION}}": question, "{{SCHEMA}}": schema, "{{CURRENT_SQL}}": sql_step1,
        "{{EXTERNAL_KNOWLEDGE}}": external_knowledge,
        "{{EXECUTION_RESULT}}": df_to_markdown(df), "{{DIALECT}}": dialect,
        "{{DIALECT_OPTIMIZATION_RULES}}": dialect_rules
    })
    messages_2 = [HumanMessage(content=output_prompt)]
    sql_step2 = None
    
    for llm_retry in range(retry_config["max_attempts"]):
        delay = min(retry_config["initial_delay"] * (retry_config["backoff_multiplier"] ** llm_retry), retry_config["max_delay"])
        if llm_retry > 0: 
            if logger: logger.info(f"Output Validation LLM retry {llm_retry + 1} after {delay:.2f}s")
            time.sleep(delay)
        try:
            response = model.invoke(messages_2)
            response_text = response.content if hasattr(response, "content") else str(response)
            think_text, answer_text = extract_think_answer(response_text)
            sql_step2 = extract_sql_from_response(answer_text)
            all_messages.append({"step": 2, "messages": messages_2, "raw_response": response_text, "think": think_text, "answer": answer_text})
            break
        except Exception as e:
            logger.warning(f"Double Validation Output Step LLM call failed: {e}")

    return {"all_messages": all_messages, "final_sql": sql_step2, "success": bool(sql_step2)}


def correct_semantic_single_candidate(
    instance_data: Dict[str, Any],
    run_id: str,
    model: BaseChatModel,
    executor: SQLExecutor,
    prompt_dir: str = "config/prompts/semantic",
    prompt_names: Optional[Dict[str, str]] = None,
    runs_root: str = "logs/runs",
    schema_dir: str = "final_schema",
    ek_path: Optional[str] = None,
    gen_prefix: str = "simple",
    validation_mode: Literal["unified", "double"] = "unified",
    max_turns: int = 2,
    retry_config: Dict[str, float] = DEFAULT_RETRY_CONFIG
) -> Dict[str, Any]:
    
    if prompt_names is None:
        prompt_names = {
            "classify": "semantic_classify",
            "validation": "semantic_validation",
            "rules": "semantic_rules_validation",
            "output": "semantic_output_validation"
        }
    
    instance_id = instance_data["instance_id"]
    candidate_id = instance_data["candidate_id"]
    db_id = instance_data["db_id"]
    dialect = instance_data.get("dialect", "sqlite")
    question = instance_data["question"]
    
    # 1. Логгер
    base_dir = Path(runs_root) / run_id / "correction" / gen_prefix
    events_dir = base_dir / "events"
    events_dir.mkdir(parents=True, exist_ok=True)
    logger = get_logger(
        name=f"sem_{instance_id}_cand{candidate_id}",
        log_file=str(events_dir / f"{instance_id}_cand{candidate_id}.log"),
        console=False
    )
    
    logger.info(f"=== START SEMANTIC VALIDATION ({validation_mode}): {instance_id} | Cand: {candidate_id} ===")
    start_time = time.perf_counter()
    
    # 2. Контекст
    context = _load_semantic_context(instance_id, candidate_id, run_id, runs_root, gen_prefix, schema_dir, ek_path, logger)
    current_sql = context["current_sql"]
    
    if not current_sql:
        logger.error("No SQL found for semantic correction.")
        return {
            "instance_id": instance_id,  "candidate_id": candidate_id, "final_status": "failed_no_sql", 
            "metadata": {"start_time": start_time, "end_time": time.perf_counter()}
        }
    
    # 3. Правила диалекта
    dialect_rules = "None"
    rules_path = Path(prompt_dir).parent / "generation" / "dialects" / f"{dialect}_rules.txt"
    if rules_path.exists():
        dialect_rules = rules_path.read_text(encoding="utf-8")
    
    # 4. Загрузка шаблонов
    classify_tpl = _load_prompt(prompt_names["classify"], prompt_dir)
    validation_tpl = _load_prompt(prompt_names["validation"], prompt_dir) if validation_mode == "unified" else None
    rules_tpl = _load_prompt(prompt_names["rules"], prompt_dir) if validation_mode == "double" else None
    output_tpl = _load_prompt(prompt_names["output"], prompt_dir) if validation_mode == "double" else None
    
    results = {
        "instance_id": instance_id, "candidate_id": candidate_id, "db_id": db_id,
        "dialect": dialect, "question": question, "original_sql": current_sql,
        "validation_mode": validation_mode,
        "turns": [], "final_status": "processing", "final_sql": current_sql, "final_verdict": None,
        "metadata": {"start_time": start_time, "end_time": None}
    }
    
    # 5. Цикл
    for turn in range(1, max_turns + 1):
        logger.info(f"--- Semantic Turn {turn}/{max_turns} ---")
        turn_record = {"turn": turn, "classification": None, "correction": None, "execution": None}
        
        # ШАГ A: Классификация
        logger.info("Running classification check...")
        check_result = run_classification_check(
            model, dialect, question, context["schema"], 
            current_sql, context["execution_result"],
            classify_tpl, retry_config, 
            context["external_knowledge"], logger
        )
        turn_record["classification"] = check_result
        verdict = check_result["result"]["verdict"]
        reasons = check_result["result"]["reasons"]
        results["final_verdict"] = verdict
        logger.info(f"Verdict: {verdict}, Reasons: {reasons}")
        
        if verdict == "VALID":
            logger.info("SQL is VALID. Finishing.")
            results["final_status"] = "valid"
            results["turns"].append(turn_record)
            break
            
        if turn == max_turns:
            logger.warning("Max turns reached. SQL remains INVALID.")
            results["final_status"] = "invalid_max_turns"
            results["turns"].append(turn_record)
            break
            
        # ШАГ B: Коррекция (в зависимости от режима)
        logger.info(f"Running correction (mode: {validation_mode})...")
        reasons_text = "\n".join([f"- {r}" for r in reasons]) or "- The result does not fully answer the user's question."
        corrected_sql = None
        correction_result = {}
        
        if validation_mode == "unified":
            corr_res = run_unified_validation(
                model, question, context["schema"], 
                current_sql, context["execution_result"], 
                reasons_text, dialect, dialect_rules, validation_tpl, 
                retry_config, context["external_knowledge"], logger
            )
            corrected_sql = corr_res.get("corrected_sql")
            correction_result = corr_res
            
        elif validation_mode == "double":
            corr_res = run_double_validation(
                model, executor, question, 
                db_id.split("_", 1)[1] if "_" in db_id else db_id, 
                context["schema"], current_sql, context["execution_result"], 
                reasons_text, dialect, dialect_rules, rules_tpl, output_tpl, 
                retry_config, context["external_knowledge"], logger
            )
            corrected_sql = corr_res.get("final_sql")
            correction_result = corr_res
            
        turn_record["correction"] = correction_result
        
        if not corrected_sql:
            logger.error("Failed to extract corrected SQL.")
            results["final_status"] = "failed_extraction"
            results["turns"].append(turn_record)
            break
            
        logger.info(f"Corrected SQL:\n{corrected_sql}")
        
        # ШАГ C: Исполнение
        logger.info(f"Executing corrected SQL on DB: {db_id}")
        exec_start = time.perf_counter()
        exec_error = None
        df = None
        try:
            exec_status, df = executor.thread_safe_sql_execution(
                sql=corrected_sql, db_name=db_id.split("_", 1)[1] if "_" in db_id else db_id, dialect=dialect
            )
            exec_duration = time.perf_counter() - exec_start
            logger.info(f"Execution finished in {exec_duration:.2f}s. Status: {exec_status}")
        except Exception as e:
            exec_status, exec_error, df = "error", str(e), None
            exec_duration = time.perf_counter() - exec_start
            logger.error(f"Execution exception: {exec_error}")
            
        turn_record["execution"] = {
            "status": exec_status, "duration_sec": round(exec_duration, 3), "error": exec_error
        }
        
        if exec_status == "error":
            logger.error("Corrected SQL failed execution. Stopping.")
            results["final_status"] = "failed_execution_after_correction"
            results["turns"].append(turn_record)
            break
            
        # Обновляем контекст для следующей итерации
        current_sql = corrected_sql
        context["execution_result"] = df_to_markdown(df)
        context["df"] = df
        results["turns"].append(turn_record)
        
    # 6. Финализация и сохранение
    results["metadata"]["end_time"] = time.perf_counter()
    results["metadata"]["total_duration_sec"] = round(results["metadata"]["end_time"] - start_time, 3)
    
    cand_dir = base_dir / f"valid_sql_{candidate_id:02d}"
    results_dir = base_dir / f"valid_result_{candidate_id:02d}"
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
        if os.path.exists(tmp_path): os.unlink(tmp_path)
        
    if results["final_sql"]:
        (cand_dir / f"{instance_id}.sql").write_text(results["final_sql"], encoding="utf-8")
        
    if results["final_status"] in ("valid", "invalid_max_turns") and context.get("df") is not None:
        df = context["df"]
        csv_path = results_dir / f"{instance_id}.csv"
        if df.empty:
            csv_path.write_text("# Empty result\n", encoding="utf-8")
        else:
            df.to_csv(str(csv_path), index=False, encoding="utf-8")
            
    logger.info(f"=== COMPLETED: {instance_id} | Status: {results['final_status']} | Verdict: {results['final_verdict']} ===")
    return results


def simple_semantic_correction(
    run_id: str, model: BaseChatModel, executor: SQLExecutor,
    tasks: Optional[Union[List[Dict[str, Any]], str]] = None,
    prompt_dir: str = "config/prompts/semantic",
    prompt_names: Optional[Dict[str, str]] = None,
    runs_root: str = "logs/runs",
    data_root: str = "data",
    input_data_root: str = "Spider2/spider2-lite",
    schema_dir: str = "final_schema",
    gen_prefix: str = "simple",
    validation_mode: str = "unified",
    max_turns: int = 2, 
    max_workers: int = 2,
    retry_config: Dict[str, float] = DEFAULT_RETRY_CONFIG, 
    **kwargs
) -> Dict[str, Any]:
    
    base_path = Path(runs_root) / run_id / "correction" / gen_prefix
    main_log = base_path / "main.log"
    main_log.parent.mkdir(parents=True, exist_ok=True)
    logger = get_logger("simple_semantic", str(main_log))
    
    if tasks is None:
        logger.info("Loading successful candidates from correction/generation manifests...")
        tasks = _load_semantic_candidates(run_id, runs_root, gen_prefix)
        logger.info(f"Loaded {len(tasks)} candidates for semantic correction.")
    else:
        logger.info(f"Starting with {len(tasks)} provided tasks.")
        
    if not tasks:
        return {"results": {}, "stats": {"total_jobs": 0}}
    
    ek_paths = _load_external_knowledge_path(tasks, input_data_root, data_root)
    
    all_results, start_pipeline = {}, time.perf_counter()
    logger.info(f"Starting parallel semantic correction ({validation_mode}) with {max_workers} workers")
    
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="SemWorker") as pool:
        future_to_task = {
            pool.submit(
                correct_semantic_single_candidate,
                instance_data=task, run_id=run_id, model=model, executor=executor,
                prompt_dir=prompt_dir, prompt_names=prompt_names, runs_root=runs_root,
                schema_dir=schema_dir, ek_path=ek_paths[task["instance_id"]], 
                gen_prefix=gen_prefix, validation_mode=validation_mode, 
                max_turns=max_turns, retry_config=retry_config
            ): (task["instance_id"], task["candidate_id"])
            for task in tasks
        }
        
        for future in as_completed(future_to_task):
            instance_id, cand_id = future_to_task[future]
            try:
                result = future.result()
                all_results[f"{instance_id}_c{cand_id}"] = result
                logger.info(f"Completed: {instance_id} (Cand {cand_id}) | Status: {result['final_status']}")
            except Exception as e:
                logger.error(f"Failed {instance_id} (Cand {cand_id}): {e}", exc_info=True)
                all_results[f"{instance_id}_c{cand_id}"] = {"instance_id": instance_id, "candidate_id": cand_id, "final_status": "crashed", "error": str(e)}
    
    total_duration = time.perf_counter() - start_pipeline
    stats = {
        "total_jobs": len(tasks),
        "valid": sum(1 for r in all_results.values() if r.get("final_status") == "valid"),
        "corrected_or_invalid": sum(1 for r in all_results.values() if r.get("final_status") == "invalid_max_turns"),
        "failed": sum(1 for r in all_results.values() if r.get("final_status") not in ("valid", "invalid_max_turns")),
        "total_duration_sec": round(total_duration, 2)
    }
    
    with open(base_path / "validation_stats.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
        
    logger.info("=== SEMANTIC CORRECTION PIPELINE FINISHED ===")
    return {"results": all_results, "stats": stats}


if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv(".env")
    
    parser = argparse.ArgumentParser(description="Semantic SQL correction pipeline")
    parser.add_argument("--run-name", required=True)
    parser.add_argument("--model-name", required=True)
    parser.add_argument("--base-url", default=None)
    parser.add_argument("--api-key", default=None)
    parser.add_argument("--temperature", type=float, default=0.2)
    
    parser.add_argument("--run-root", default="logs/runs")
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--storage-root", default="storage")
    parser.add_argument("--input-data-root", default="Spider2/spider2-lite")
    parser.add_argument("--gen-prefix", default="simple")
    parser.add_argument("--schema-dir", default="final_schema")
    parser.add_argument("--local-dbs", type=parse_dialect_path_pair, nargs="*", default=None, metavar="DIALECT:PATH")
    
    parser.add_argument("--prompt-dir", default="config/prompts/correction")
    parser.add_argument("--classify-prompt", default="semantic_classify")
    parser.add_argument("--validation-prompt", default="semantic_validation")
    parser.add_argument("--rules-prompt", default=None)
    parser.add_argument("--output-prompt", default=None)
    
    # Ключевой параметр режима
    parser.add_argument("--validation-mode", type=str, default="unified", choices=["unified", "double"], help="Режим валидации: unified или double")
    
    parser.add_argument("--max-workers", type=int, default=2)
    parser.add_argument("--max-turns", type=int, default=2)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--initial-delay", type=float, default=2.0)
    parser.add_argument("--max-delay", type=float, default=30.0)
    
    args = parser.parse_args()
    
    model = get_model(model_name=args.model_name, base_url=args.base_url, api_key=args.api_key, temperature=args.temperature)
    executor = SQLExecutor(
        input_data_root=args.input_data_root, 
        data_root=args.data_root, 
        storage_root=args.storage_root, 
        local_dbs=dict(args.local_dbs) if args.local_dbs else None
    )
    
    prompt_names = {
        "classify": args.classify_prompt, "validation": args.validation_prompt,
        "rules": args.rules_prompt, "output": args.output_prompt
    }
    retry_config = {
        "max_attempts": args.max_attempts, 
        "initial_delay": args.initial_delay, 
        "max_delay": args.max_delay, 
        "backoff_multiplier": 2.0
    }
    run_id = resolve_run_id(args.run_root, args.input_data_root, args.run_name)
    
    print(f"\nStarting semantic correction ({args.validation_mode}) for run: {run_id}")
    output = simple_semantic_correction(
        run_id=run_id, model=model, executor=executor, prompt_dir=args.prompt_dir, 
        prompt_names=prompt_names, runs_root=args.run_root, schema_dir=args.schema_dir, 
        gen_prefix=args.gen_prefix, validation_mode=args.validation_mode,
        max_turns=args.max_turns, max_workers=args.max_workers, retry_config=retry_config
    )
    
    stats = output.get("stats", {})
    print("\nРезультаты:")
    print(f"  Всего задач:     {stats.get('total_jobs', 0)}")
    print(f"  Валидных:        {stats.get('valid', 0)}")
    print(f"  Неверных/Испр.:  {stats.get('corrected_or_invalid', 0)}")
    print(f"  Ошибок:          {stats.get('failed', 0)}")
    print(f"  Время:           {stats.get('total_duration_sec', 0):.2f} сек")
