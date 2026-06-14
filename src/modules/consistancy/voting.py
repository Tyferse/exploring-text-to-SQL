import sys
sys.path.insert(0, ".")

import os
import re
import json
import time
import random
import logging
import tempfile
from pathlib import Path
from typing import Dict, Any, Literal, Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage

from src.evaluation.utils import compare_pandas_table
from src.utils.logger import get_logger
from src.utils.models import get_model
from src.utils.preprocessing import fill_prompt_template, resolve_tasks
from src.utils.run_manager import resolve_run_id
from src.utils.sql_execution import df_to_markdown


DEFAULT_RETRY_CONFIG = {
    "max_attempts": 2,
    "initial_delay": 4.0,
    "max_delay": 30.0,
    "backoff_multiplier": 2.0,
}


def _load_prompt(prompt_name: str, prompt_dir: str = "config/prompts/consistancy") -> str:
    prompt_path = Path(prompt_dir) / f"{prompt_name}.md"
    if not prompt_path.exists():
        raise FileNotFoundError(f"Prompt template {prompt_name} not found in {prompt_dir}")
    return prompt_path.read_text(encoding="utf-8")


def _normalize_sql(sql: str) -> str:
    """Приводит SQL к единому виду для сравнения."""
    sql = sql.lower().strip()
    sql = re.sub(r'\s+', ' ', sql)
    sql = re.sub(r'\s*([,;()])\s*', r'\1', sql)
    return sql


def _load_df_from_csv(csv_path: Path) -> Optional[pd.DataFrame]:
    """Загружает DataFrame из CSV."""
    try:
        content = csv_path.read_text(encoding="utf-8").strip()
        if content.startswith("# Empty result"):
            return pd.DataFrame()
        return pd.read_csv(csv_path, encoding="utf-8")
    except Exception:
        return None


def _get_stage_priority(csv_path: Path) -> int:
    """Возвращает приоритет этапа (чем больше, тем выше приоритет)."""
    path_str = str(csv_path)
    if "correction" in path_str and "valid_" in path_str:
        return 3
    elif "correction" in path_str:
        return 2
    elif "generation" in path_str:
        return 1
    return 0


def _group_by_execution(candidates: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """Группирует кандидатов по эквивалентности результатов исполнения."""
    if not candidates:
        return []
    
    groups = []
    for cand in candidates:
        placed = False
        for group in groups:
            # Сравниваем с представителем группы
            if compare_pandas_table(cand["df"], group[0]["df"], ignore_order=True):
                group.append(cand)
                placed = True
                break

        if not placed:
            groups.append([cand])
    
    return groups


def _group_by_sql(candidates: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """Групперует кандидатов по нормализованному тексту SQL."""
    groups_map: Dict[str, List[Dict[str, Any]]] = {}
    for cand in candidates:
        norm_sql = cand["normalized_sql"]
        if norm_sql not in groups_map:
            groups_map[norm_sql] = []

        groups_map[norm_sql].append(cand)

    return list(groups_map.values())


def _select_best_representative(group: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Выбирает лучшего представителя группы по времени и приоритету этапа."""
    return min(group, key=lambda c: (c["execution_time"], -c["stage_priority"]))


def parse_llm_judge_response(response_text: str) -> Tuple[str, str]:
    """Парсит ответ LLM-судьи. Возвращает 'A', 'B' или 'TIE'."""
    json_match = re.search(r'\{[\s\S]*\}', response_text)
    if json_match:
        try:
            result = json.loads(json_match.group(0))
            reason = result.get("reasoning", "").strip().upper()
            winner = result.get("winner", "").strip().upper()
            if winner in ("A", "B", "TIE"):
                return winner, reason
        except json.JSONDecodeError:
            pass
    
    # Fallback: ищем ключевые слова
    text = response_text.upper()
    if '"A"' in text or "'A'" in text or 'WINNER": "A' in text:
        return "A", ""
    if '"B"' in text or "'B'" in text or 'WINNER": "B' in text:
        return "B", ""
    if '"TIE"' in text or "'TIE'" in text or 'WINNER": "TIE' in text:
        return "TIE", ""
    
    return "UNKNOWN", ""


def _find_candidate_results(instance_id: str, candidate_id: int, run_id: str, runs_root: str, gen_prefix: str) -> Optional[Tuple[str, str]]:
    """Ищет CSV-файл кандидата по приоритету путей."""
    cand_str = f"{candidate_id:02d}"
    search_paths = [
        (Path(runs_root) / run_id / "correction" / gen_prefix, "valid_"),
        (Path(runs_root) / run_id / "correction" / gen_prefix, ""),
        (Path(runs_root) / run_id / "generation" / gen_prefix, "")   
    ]
    for path, prefix in search_paths:
        if all((path / file).exists() for file in [
            Path(f"{prefix}result_{cand_str}") / f"{instance_id}.csv", 
            Path(f"{prefix}sql_{cand_str}") / f"{instance_id}.sql", 
            Path(f"{prefix}result_{cand_str}") / f"{instance_id}_meta.json"
        ]) or all((path / file).exists() for file in [
            Path(f"{prefix}results_{cand_str}") / f"{instance_id}.csv", 
            Path(f"{prefix}sql_{cand_str}") / f"{instance_id}.sql", 
            Path(f"{prefix}manifests") / f"{instance_id}_meta.json"
        ]):
            return path, prefix
        
    return None
    

def _collect_candidates_for_instance(
    instance_id: str, run_id: str, runs_root: str, gen_prefix: str, data_root: str, input_data_root: str, logger: logging.Logger
) -> List[Dict[str, Any]]:
    """Собирает всех успешных кандидатов для instance_id из всех источников."""
    candidates = []
    seen_cand_ids = set()
    
    # Сканируем все возможные директории
    search_dirs = [
        (Path(runs_root) / run_id / "correction" / gen_prefix, "valid_result_", "semantic"),
        (Path(runs_root) / run_id / "correction" / gen_prefix, "result_", "correction"),
        (Path(runs_root) / run_id / "generation" / gen_prefix, "results_", "generation"),
    ]

    tasks = resolve_tasks(None, data_root, input_data_root)
    ek_paths = {task["instance_id"]: (Path(data_root) / input_data_root / "resource" 
                                      / "documents" / task["external_knowledge"])
                for task in tasks if task.get("external_knowledge")}
    
    for base_dir, res_dir, stage_name in search_dirs:
        if not base_dir.exists():
            continue

        for result_dir in base_dir.glob(f"{res_dir}*"):
            csv_path = result_dir / f"{instance_id}.csv"
            if not csv_path.exists():
                continue
        
            try:
                cand_id = int(result_dir.name.rsplit("_")[-1])
            except ValueError:
                continue
            
            if cand_id in seen_cand_ids:
                continue
            
            # Загружаем DataFrame
            df = _load_df_from_csv(csv_path)
            if df is None:
                logger.debug(f"Skipping {instance_id} cand {cand_id} from {stage_name}: invalid CSV")
                continue
            
            # Загружаем SQL
            find_result = _find_candidate_results(instance_id, cand_id, run_id, runs_root, gen_prefix)
            if find_result is None:
                logger.debug(f"Skipping {instance_id} cand {cand_id} from {stage_name}: incomplete artifacts")
                continue

            path, res_prefix = find_result
            sql = (path / f"{res_prefix}sql_{cand_id:02d}" / f"{instance_id}.sql").read_text(encoding="utf-8")
            if not sql:
                logger.debug(f"Skipping {instance_id} cand {cand_id} from {stage_name}: no SQL found")
                continue
            
            # Загружаем мета-данные для времени выполнения
            if stage_name != "generation":
                meta_path = path / f"{res_prefix}result_{cand_id:02d}" / f"{instance_id}_meta.json"
            else:
                meta_path = path / f"{res_prefix}manifests" / f"{instance_id}_meta.json"
            
            meta = {}
            if meta_path.exists():
                try:
                    with open(meta_path, "r", encoding="utf-8") as f:
                        meta = json.load(f)
                except Exception as e:
                    logger.warning(f"Failed to load meta for {instance_id} cand {cand_id}: {e}")

            execution_time = 0.0
            question = ""
            dialect = "sqlite"
            db_id = instance_id
            
            if meta and stage_name != "generation":
                # Пытаемся достать время из разных структур
                if "attempts" in meta and meta["attempts"]:
                    execution_time = meta["attempts"][-1].get("execution_duration_sec", 0.0)
                elif "metadata" in meta:
                    execution_time = meta["metadata"].get("total_duration_sec", 0.0)
                elif "execution" in meta:
                    execution_time = meta["execution"].get("duration_sec", 0.0)
                
                question = meta.get("question", "")
                dialect = meta.get("dialect", "sqlite")
                db_id = meta.get("db_id", instance_id)
            elif meta:    
                execution_time = meta["candidates"][f"{cand_id}:02d"].get("execution", {}).get("duration_sec", 0.0)
                question = meta.get("question", "")
                dialect = meta.get("dialect", "sqlite")
                db_id = meta.get("db_id", instance_id)    
            
            seen_cand_ids.add(cand_id)
            candidates.append({
                "candidate_id": cand_id,
                "stage": stage_name,
                "stage_priority": _get_stage_priority(csv_path),
                "csv_path": csv_path,
                "sql": sql,
                "normalized_sql": _normalize_sql(sql),
                "df": df,
                "execution_time": execution_time,
                "question": question,
                "dialect": dialect,
                "external_knowledge": ek_paths[instance_id].read_text(encoding="utf-8") if ek_paths.get(instance_id) else None,
                "db_id": db_id,
            })
            logger.info(f"Found candidate {cand_id} from {stage_name} for {instance_id} (time: {execution_time:.3f}s)")
    
    return candidates


def run_llm_pairwise_comparison(
    model: BaseChatModel,
    question: str, dialect: str, external_knowledge: str,
    sql_a: str, result_a: str, time_a: float,
    sql_b: str, result_b: str, time_b: float,
    prompt_template: str, retry_config: Dict[str, float],
    logger: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    """Сравнивает два варианта SQL через LLM. Возвращает победителя и обоснование."""
    prompt = fill_prompt_template(prompt_template, {
        "{{QUESTION}}": question,
        "{{DIALECT}}": dialect,
        "{{EXTERNAL_KNOWLEDGE}}": external_knowledge,
        "{{SQL_A}}": sql_a,
        "{{RESULT_A}}": result_a,
        "{{TIME_A}}": f"{time_a:.3f}",
        "{{SQL_B}}": sql_b,
        "{{RESULT_B}}": result_b,
        "{{TIME_B}}": f"{time_b:.3f}",
    })
    messages = [HumanMessage(content=prompt)]
    
    for llm_retry in range(retry_config["max_attempts"]):
        delay = min(retry_config["initial_delay"] * (retry_config["backoff_multiplier"] ** llm_retry), retry_config["max_delay"])
        if llm_retry > 0:
            if logger: logger.info(f"LLM Judge retry {llm_retry + 1} after {delay:.2f}s")
            time.sleep(delay)
        try:
            response = model.invoke(messages)
            response_text = response.content if hasattr(response, "content") else str(response)
            winner, reason = parse_llm_judge_response(response_text)
            return {
                "messages": messages,
                "raw_response": response_text,
                "reason": reason,
                "winner": winner,
                "success": winner != "UNKNOWN"
            }
        except Exception as e:
            if logger: logger.warning(f"LLM Judge call failed: {e}")
    
    return {"messages": messages, "raw_response": "", "reason": "", "winner": "UNKNOWN", "success": False}


def _llm_tournament(
    candidates: List[Dict[str, Any]],
    model: BaseChatModel,
    prompt_template: str,
    retry_config: Dict[str, float],
    logger: logging.Logger
) -> Tuple[Dict[str, Any], str, Optional[List[Dict[str, Any]]]]:
    """Проводит турнир между кандидатами через LLM. Возвращает (победитель, причина)."""
    if len(candidates) < 2:
        return candidates[0], "only_one_in_tournament", None
    
    tournament_log = []
    win_results = {challenger["candidate_id"]: 0 for challenger in candidates}

    for i, challenger_a in enumerate(candidates[:-1]):
        for challenger_b in candidates[i+1:]:
            logger.info(f"Tournament match: cand {challenger_a['candidate_id']} vs cand {challenger_b['candidate_id']}")
            
            result_a = df_to_markdown(challenger_a["df"])
            result_b = df_to_markdown(challenger_b["df"])
            
            llm_result = run_llm_pairwise_comparison(
                model=model,
                question=challenger_a["question"],
                dialect=challenger_a["dialect"],
                external_knowledge=challenger_a["external_knowledge"],
                sql_a=challenger_a["sql"],
                result_a=result_a,
                time_a=challenger_a["execution_time"],
                sql_b=challenger_b["sql"],
                result_b=result_b,
                time_b=challenger_b["execution_time"],
                prompt_template=prompt_template,
                retry_config=retry_config,
                logger=logger
            )
            
            tournament_log.append({
                "champion_id": challenger_a["candidate_id"],
                "challenger_id": challenger_b["candidate_id"],
                "llm_winner": llm_result["winner"],
                "messages": llm_result["messages"],
                "raw_response": llm_result["raw_response"]
            })
            
            if llm_result["winner"] == "B":
                logger.info(f"Challenger {challenger_b['candidate_id']} wins.")
                win_results[challenger_b["candidate_id"]] += 1
            elif llm_result["winner"] == "TIE":
                logger.info("TIE. Keeping current champion (or faster one).")
                if challenger_b["execution_time"] < challenger_a["execution_time"]:
                    win_results[challenger_b["candidate_id"]] += 1
                else:
                    win_results[challenger_a["candidate_id"]] += 1

            elif llm_result["winner"] == "UNKNOWN":
                logger.warning("LLM returned UNKNOWN. Fallback: keeping faster candidate.")
                if challenger_b["execution_time"] < challenger_a["execution_time"]:
                    win_results[challenger_b["candidate_id"]] += 1
                else:
                    win_results[challenger_a["candidate_id"]] += 1
            else:
                logger.info(f"Candidate {challenger_a['candidate_id']} gives a point as fallback.")
                win_results[challenger_a["candidate_id"]] += 1
    
    champion_id = max(win_results, key=win_results.get)
    champion = next(c for c in candidates if c['candidate_id'] == champion_id)
    return champion, f"tournament({len(candidates)}_candidates,{len(tournament_log)}_matches)", tournament_log


def select_final_for_instance(
    instance_id: str,
    run_id: str,
    model: BaseChatModel,
    prompt_dir: str = "config/prompts/consistancy",
    prompt_name: str = "pairwise_selection",
    runs_root: str = "logs/runs",
    gen_prefix: str = "simple",
    data_root: str = "data",
    input_data_root: str = "Spider2/spider2-lite",
    selection_mode: Literal["execution", "sql", "llm", "random"] = "execution",
    use_llm_tiebreaker: bool = True,
    retry_config: Dict[str, float] = DEFAULT_RETRY_CONFIG
) -> Dict[str, Any]:
    """Выбирает финальный SQL для одного instance_id."""
    
    # 1. Логгер
    base_dir = Path(runs_root) / run_id / "consistancy" / gen_prefix
    events_dir = base_dir / "events"
    events_dir.mkdir(parents=True, exist_ok=True)
    logger = get_logger(
        name=f"select_{instance_id}",
        log_file=str(events_dir / f"{instance_id}.log"),
        console=False
    )
    
    logger.info(f"=== START FINAL SELECTION ({selection_mode}): {instance_id} ===")
    start_time = time.perf_counter()
    
    # 2. Сбор кандидатов
    candidates = _collect_candidates_for_instance(instance_id, run_id, runs_root, gen_prefix, data_root, input_data_root, logger)
    
    results = {
        "instance_id": instance_id,
        "total_candidates_found": len(candidates),
        "selection_mode": selection_mode,
        "candidates_summary": [
            {
                "candidate_id": c["candidate_id"],
                "stage": c["stage"],
                "execution_time": c["execution_time"]
            } for c in candidates
        ],
        "selection_details": {},
        "final_status": "processing",
        "final_candidate_id": None,
        "final_sql": None,
        "final_stage": None,
        "final_execution_time": None,
        "metadata": {"start_time": start_time, "end_time": None}
    }
    
    if not candidates:
        logger.error("No successful candidates found.")
        results["final_status"] = "failed_no_candidates"
        results["metadata"]["end_time"] = time.perf_counter()
        results["metadata"]["total_duration_sec"] = round(results["metadata"]["end_time"] - start_time, 3)
        return results
    
    if len(candidates) == 1:
        logger.info("Only one candidate found. Selecting it automatically.")
        winner = candidates[0]
        results["final_status"] = "success_single_candidate"
        results["final_candidate_id"] = winner["candidate_id"]
        results["final_sql"] = winner["sql"]
        results["final_stage"] = winner["stage"]
        results["final_execution_time"] = winner["execution_time"]
        results["selection_details"]["reason"] = "only_one_candidate"
        results["metadata"]["end_time"] = time.perf_counter()
        results["metadata"]["total_duration_sec"] = round(results["metadata"]["end_time"] - start_time, 3)
        return results
    
    # 3. Загрузка промпта для LLM (если нужен)
    llm_template = None
    if selection_mode == "llm" or use_llm_tiebreaker:
        try:
            llm_template = _load_prompt(prompt_name, prompt_dir)
        except FileNotFoundError as e:
            logger.warning(f"LLM prompt not found: {e}. Falling back to execution mode.")
            if selection_mode == "llm":
                selection_mode = "execution"
    
    # 4. Режим выбора
    winner = None
    selection_reason = ""
    
    if selection_mode == "random":
        logger.info("Mode: RANDOM")
        winner = random.choice(candidates)
        selection_reason = "random_choice"
    
    elif selection_mode == "execution":
        logger.info("Mode: EXECUTION-BASED VOTING")
        groups = _group_by_execution(candidates)
        logger.info(f"Formed {len(groups)} groups by execution result")
        
        # Выбираем лучшего представителя из каждого кластера
        representatives = []
        for i, group in enumerate(groups):
            rep = _select_best_representative(group)
            representatives.append({
                "group_id": i,
                "representative": rep,
                "vote_count": len(group),
                "min_execution_time": rep["execution_time"],
                "members": [c["candidate_id"] for c in group]
            })
            logger.info(f"Group {i}: {len(group)} votes, best time: {rep['execution_time']:.3f}s, rep: cand {rep['candidate_id']}")
        
        # Сортируем по голосам (убыв.), затем по времени (возр.)
        representatives.sort(key=lambda r: (-r["vote_count"], r["min_execution_time"]))
        
        # Проверяем на ничью
        top_votes = representatives[0]["vote_count"]
        tied = [r for r in representatives if r["vote_count"] == top_votes]
        tournament_logs = None

        if len(tied) == 1:
            winner = tied[0]["representative"]
            selection_reason = f"majority_vote({top_votes}/{len(candidates)})"
        else:
            logger.info(f"Tie between {len(tied)} groups. Applying tie-breaker.")
            # Ничья по голосам - выбираем по времени
            tied.sort(key=lambda r: r["min_execution_time"])
            min_time = tied[0]["min_execution_time"]
            # time_tied = [r for r in tied if abs(r["min_execution_time"] - min_time) < 0.01]
            
            if len(tied) == 1:
                winner = tied[0]["representative"]
                selection_reason = f"fastest_execution({min_time:.3f}s)_after_vote_tie"
            elif use_llm_tiebreaker and llm_template:
                logger.info("Using LLM tie-breaker")
                winner, llm_reason, tournament_logs = _llm_tournament(
                    [r["representative"] for r in tied],
                    model, llm_template, retry_config, logger
                )
                selection_reason = f"llm_tiebreaker({llm_reason})"
            else:
                # Fallback: случайный выбор из равных
                winner = random.choice(tied)["representative"]
                selection_reason = "random_after_tie"
        
        results["selection_details"]["groups"] = [
            {"group_id": r["group_id"], "vote_count": r["vote_count"], 
             "min_time": r["min_execution_time"], "members": r["members"],
             "representative_id": r["representative"]["candidate_id"]}
            for r in representatives
        ]
    
    elif selection_mode == "sql":
        logger.info("Mode: SQL-BASED VOTING")
        groups = _group_by_sql(candidates)
        logger.info(f"Formed {len(groups)} groups by SQL text")
        
        representatives = []
        for i, group in enumerate(groups):
            rep = _select_best_representative(group)
            representatives.append({
                "group_id": i,
                "representative": rep,
                "vote_count": len(group),
                "normalized_sql": rep["normalized_sql"]
            })
        
        representatives.sort(key=lambda r: (-r["vote_count"], r["representative"]["execution_time"]))
        
        top_votes = representatives[0]["vote_count"]
        tied = [r for r in representatives if r["vote_count"] == top_votes]
        
        if len(tied) == 1:
            winner = tied[0]["representative"]
            selection_reason = f"sql_majority_vote({top_votes}/{len(candidates)})"
        elif use_llm_tiebreaker and llm_template:
            logger.info("SQL vote tie. Using LLM tie-breaker")
            winner, llm_reason, tournament_logs = _llm_tournament(
                [r["representative"] for r in tied],
                model, llm_template, retry_config, logger
            )
            selection_reason = f"llm_tiebreaker({llm_reason})"
        else:
            winner = random.choice(tied)["representative"]
            selection_reason = "random_after_sql_tie"
        
        results["selection_details"]["sql_group"] = [
            {"group_id": r["group_id"], "vote_count": r["vote_count"],
             "representative_id": r["representative"]["candidate_id"]}
            for r in representatives
        ]
    
    elif selection_mode == "llm":
        logger.info("Mode: LLM JUDGE TOURNAMENT")
        if not llm_template:
            logger.error("LLM prompt not available. Falling back to random.")
            winner = random.choice(candidates)
            selection_reason = "random_fallback_no_prompt"
        else:
            winner, llm_reason, tournament_logs = _llm_tournament(candidates, model, llm_template, retry_config, logger)
            selection_reason = f"llm_tournament({llm_reason})"
    
    results["selection_details"]["llm_tournament"] = tournament_logs

    # 5. Финализация
    if winner:
        results["final_status"] = "success"
        results["final_candidate_id"] = winner["candidate_id"]
        results["final_sql"] = winner["sql"]
        results["final_stage"] = winner["stage"]
        results["final_execution_time"] = winner["execution_time"]
        results["selection_details"]["reason"] = selection_reason
        logger.info(f"Winner: candidate {winner['candidate_id']} from {winner['stage']} (reason: {selection_reason})")
    else:
        results["final_status"] = "failed_selection"
        logger.error("Selection failed.")
    
    results["metadata"]["end_time"] = time.perf_counter()
    results["metadata"]["total_duration_sec"] = round(results["metadata"]["end_time"] - start_time, 3)
    
    # 6. Сохранение артефактов
    output_dir = base_dir / "final_choice"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    meta_path = output_dir / f"{instance_id}_meta.json"
    fd, tmp_path = tempfile.mkstemp(dir=str(output_dir), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as tmp:
            json.dump(results, tmp, ensure_ascii=False, indent=2)
        os.replace(tmp_path, str(meta_path))
    except Exception as e:
        logger.error(f"Failed to save _meta.json: {e}")
        if os.path.exists(tmp_path): os.unlink(tmp_path)
    
    if results["final_sql"]:
        sql_path = output_dir / f"{instance_id}.sql"
        sql_path.write_text(results["final_sql"], encoding="utf-8")
        
        # Копируем CSV победителя
        for cand in candidates:
            if cand["candidate_id"] == results["final_candidate_id"]:
                csv_src = cand["csv_path"]
                csv_dst = output_dir / f"{instance_id}.csv"
                csv_dst.write_text(csv_src.read_text(encoding="utf-8"), encoding="utf-8")
                break
    
    logger.info(f"=== COMPLETED SELECTION: {instance_id} | Status: {results['final_status']} ===")
    return results


def voting_selection(
    run_id: str,
    model: BaseChatModel,
    instance_ids: Optional[List[str]] = None,
    prompt_dir: str = "config/prompts/consistancy",
    prompt_name: str = "pairwise_selection",
    runs_root: str = "logs/runs",
    gen_prefix: str = "simple",
    data_root: str = "data",
    input_data_root: str = "Spider2/spider2-lite",
    selection_mode: Literal["execution", "sql", "llm", "random"] = "execution",
    use_llm_tiebreaker: bool = True,
    max_workers: int = 2,
    retry_config: Dict[str, float] = DEFAULT_RETRY_CONFIG,
    **kwargs
) -> Dict[str, Any]:
    """Запускает финальный выбор для множества instance_id."""
    
    base_path = Path(runs_root) / run_id / "consistancy" / gen_prefix
    main_log = base_path / "main.log"
    main_log.parent.mkdir(parents=True, exist_ok=True)
    logger = get_logger("final_selection", str(main_log))
    
    # Если instance_ids не переданы, собираем все уникальные из всех источников
    if instance_ids is None:
        instance_ids = set()
        for module, prefixes in [
            ("correction", ["valid_result_", "result_"]), 
            ("generation", ["results_"])
        ]:
            module_path = Path(runs_root) / run_id / module / gen_prefix
            if not module_path.exists():
                continue

            for prefix in prefixes:
                for result_dir in module_path.glob(f"{prefix}*"):
                    for csv_file in result_dir.glob("*.csv"):
                        instance_ids.add(csv_file.stem)

        instance_ids = list(instance_ids)
        logger.info(f"Discovered {len(instance_ids)} unique instance_ids")
    else:
        logger.info(f"Starting with {len(instance_ids)} provided instance_ids")
    
    if not instance_ids:
        return {"results": {}, "stats": {"total_jobs": 0}}
    
    all_results = {}
    start_pipeline = time.perf_counter()
    
    logger.info(f"Starting final selection ({selection_mode}) with {max_workers} workers")
    
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="SelWorker") as pool:
        future_to_iid = {
            pool.submit(
                select_final_for_instance,
                instance_id=iid, run_id=run_id, model=model,
                prompt_dir=prompt_dir, prompt_name=prompt_name,
                runs_root=runs_root, gen_prefix=gen_prefix,
                data_root=data_root, input_data_root=input_data_root,
                selection_mode=selection_mode, use_llm_tiebreaker=use_llm_tiebreaker,
                retry_config=retry_config
            ): iid
            for iid in instance_ids
        }
        
        for future in as_completed(future_to_iid):
            iid = future_to_iid[future]
            try:
                result = future.result()
                all_results[iid] = result
                logger.info(f"Completed: {iid} | Status: {result['final_status']}")
            except Exception as e:
                logger.error(f"Failed {iid}: {e}", exc_info=True)
                all_results[iid] = {"instance_id": iid, "final_status": "crashed", "error": str(e)}
    
    total_duration = time.perf_counter() - start_pipeline
    stats = {
        "total_jobs": len(instance_ids),
        "success": sum(1 for r in all_results.values() if r.get("final_status") in ("success", "success_single_candidate")),
        "failed": sum(1 for r in all_results.values() if r.get("final_status") not in ("success", "success_single_candidate")),
        "by_stage": {},
        "total_duration_sec": round(total_duration, 2)
    }
    
    # Подсчет по этапам
    for r in all_results.values():
        stage = r.get("final_stage", "unknown")
        stats["by_stage"][stage] = stats["by_stage"].get(stage, 0) + 1
    
    with open(base_path / "selection_stats.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    
    logger.info("=== FINAL SELECTION PIPELINE FINISHED ===")
    logger.info(f"Stats: {json.dumps(stats, indent=2)}")
    
    return {"results": all_results, "stats": stats}


if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv(".env")
    
    parser = argparse.ArgumentParser(description="Final SQL selection pipeline")
    parser.add_argument("--run-name", required=True)
    parser.add_argument("--model-name", required=True)
    parser.add_argument("--base-url", default=None)
    parser.add_argument("--api-key", default=None)
    parser.add_argument("--temperature", type=float, default=0.1)
    
    parser.add_argument("--run-root", default="logs/runs")
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--storage-root", default="storage")
    parser.add_argument("--input-data-root", default="Spider2/spider2-lite")
    parser.add_argument("--gen-prefix", default="simple")
    
    parser.add_argument("--prompt-dir", default="config/prompts/consistancy")
    parser.add_argument("--prompt-name", default="selection_pairwise")
    
    parser.add_argument("--selection-mode", type=str, default="execution", 
                        choices=["execution", "sql", "llm", "random"],
                        help="Режим выбора: execution, sql, llm, random")
    parser.add_argument("--use-llm-tiebreaker", action="store_true", 
                        help="Использовать LLM для разрешения ничьих в режимах execution/sql")
    parser.add_argument("--no-llm-tiebreaker", action="store_true",
                        help="Отключить LLM tie-breaker")
    
    parser.add_argument("--max-workers", type=int, default=2)
    parser.add_argument("--max-attempts", type=int, default=2)
    parser.add_argument("--initial-delay", type=float, default=4.0)
    parser.add_argument("--max-delay", type=float, default=30.0)
    
    args = parser.parse_args()
    
    model = get_model(
        model_name=args.model_name,
        base_url=args.base_url,
        api_key=args.api_key,
        temperature=args.temperature
    )
    
    use_llm_tb = args.use_llm_tiebreaker and not args.no_llm_tiebreaker
    
    retry_config = {
        "max_attempts": args.max_attempts,
        "initial_delay": args.initial_delay,
        "max_delay": args.max_delay,
        "backoff_multiplier": 2.0
    }
    
    run_id = resolve_run_id(args.run_root, args.input_data_root, args.run_name)
    
    print(f"\nStarting final selection ({args.selection_mode}) for run: {run_id}")
    output = voting_selection(
        run_id=run_id,
        model=model,
        prompt_dir=args.prompt_dir,
        prompt_name=args.prompt_name,
        runs_root=args.run_root,
        gen_prefix=args.gen_prefix,
        data_root=args.data_root,
        input_data_root=args.input_data_root,
        selection_mode=args.selection_mode,
        use_llm_tiebreaker=use_llm_tb,
        max_workers=args.max_workers,
        retry_config=retry_config
    )
    
    stats = output.get("stats", {})
    print("\nРезультаты финального выбора:")
    print(f"  Всего задач:     {stats.get('total_jobs', 0)}")
    print(f"  Успешно:         {stats.get('success', 0)}")
    print(f"  Ошибок:          {stats.get('failed', 0)}")
    print(f"  По этапам:       {stats.get('by_stage', {})}")
    print(f"  Время:           {stats.get('total_duration_sec', 0):.2f} сек")
