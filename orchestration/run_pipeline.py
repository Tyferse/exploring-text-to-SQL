import argparse
import json
import logging
import sys
import traceback
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from core import execute_stage, HANDLER_REGISTRY
from src.storage.docker_qdrant import ensure_qdrant_running
from src.storage.vector_manager import VectorStoreManager
from src.utils.logger import get_logger, ResourceMonitor
from src.utils.models import get_model
from src.utils.run_manager import resolve_run_id
from src.utils.sql_execution import SQLExecutor

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def load_json(path: str) -> Dict[str, Any]:
    p = Path(path).resolve()
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {p}")
    with open(p, 'r', encoding='utf-8') as f:
        return json.load(f)

def _deep_merge(base: Dict, override: Dict) -> Dict:
    """Рекурсивное слияние словарей. override имеет приоритет."""
    result = deepcopy(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = deepcopy(v)

    return result

def validate_configs(params: Dict, flow: Dict) -> List[str]:
    """Проверяет целостность и совместимость конфигов перед запуском."""
    errors = []
    
    if "general" not in params:
        errors.append("Missing 'general' section in params config")
        
    # Проверяем наличие всех хендлеров из flow в реестре
    for mod_def in flow:
        if not mod_def.get("enabled", True):
            continue
        
        if mod_def["module"] not in params:
            errors.append(f"Module '{mod_def["module"]}' does not exist in params")
            continue

        for stage_def in mod_def.get("stages", []):
            if not stage_def.get("enabled", True):
                continue
            
            if stage_def["stage"] not in params[mod_def["module"]]:
                errors.append(f"Stage '{stage_def["stage"]}' does not exist in parameters of module '{mod_def["module"]}'")
                continue

            handler_key = stage_def.get("handler", stage_def.get("stage"))
            if handler_key not in HANDLER_REGISTRY:
                errors.append(f"Handler '{handler_key}' is referenced in flow but not registered in orchestration/core.py")

    return errors

def run_pipeline(params_path: Optional[str] = None, flow_path: Optional[str] = None, *, params_config: Optional[Dict[str, Any]] = None, flow_config: Optional[List[Dict]] = None):
    # 1. Загрузка
    default_params = load_json(str(Path(PROJECT_ROOT, "config", "default_params.json")))
    params = (load_json(str(Path(PROJECT_ROOT, "config", "params", params_path))) 
              if params_path is not None 
              else params_config if params_config is not None else {})
    params = _deep_merge(default_params, params)
    flow = (load_json(str(Path(PROJECT_ROOT, "config", "flow", flow_path))) 
            if flow_path is not None 
            else flow_config if flow_config is not None else {})

    # 2. Валидация 
    validation_errors = validate_configs(params, flow)
    if validation_errors:
        for err in validation_errors:
            logging.error(f"Validation: {err}")

        raise SystemExit("Pipeline aborted due to configuration errors.")

    # 3. Инициализация запуска
    general = params.get("general", {})
    if "run_id" not in general:
        run_id = resolve_run_id(general["runs_root"], general["input_data_root"], general["run_name"])
        general["run_id"] = run_id
        (Path(PROJECT_ROOT) / general["runs_root"] / run_id).mkdir(parents=True, exist_ok=True)
    else:
        run_id = general["run_id"]

    log_dir = Path(PROJECT_ROOT) / general["runs_root"] / run_id
    logger = get_logger("text-to-sql pipeline", str(log_dir / "pipeline.log"))

    logger.info(f"Text-to-SQL pipeline starting | Run ID: {run_id}")
    logger.info(f"Params: {params_config} | Flow: {flow_config}")

    # 3.1 Определяем вспомогательные переменные
    vsm = None
    for module in flow:
        if module.get("enabled", True) and module["module"] in ("preprocessing", "schema_linking"):
            for stage in module.get("stages", []):
                if module.get("enabled", True) and stage["stage"] in (
                    "gen_column_embeddings", "retrieve_columns", "SchemaLinkingAgentPipeline"
                    ):
                    try:
                        vsm_params = params["preprocessing"].get("column_vector_db", {})
                        if vsm_params.get("location") is not None and vsm_params.get("backend") == "qdrant":
                            url, port = vsm_params.get("location").rsplit(":", 1)
                            host = url.split("://", 1)[1]
                            ensure_qdrant_running(
                                host, int(port), 
                                str(Path(general["storage_root"], general["input_data_root"], "column_vdb"))
                            )

                        vsm = VectorStoreManager(**vsm_params)
                    except Exception as e:
                        logging.error("Pipeline aborted due to vector store initialization errors")
                        raise e

    executor = SQLExecutor(general["input_data_root"], general["data_root"], general["storage_root"], general["local_dbs"], general.get("exec_timeout", 600))

    state: Dict[str, Any] = general.copy()
    state.update(dict(vsm=vsm, executor=executor))

    metadata = {
        "run_id": run_id,
        "started_at": datetime.now().isoformat(),
        "status": "running",
        "params_file": params_config,
        "flow_file": flow_config
    }

    model = None
    if general.get("model"):
        model = get_model(**mod_params["model"])

    # 4. Последовательное выполнение
    try:
        for mod_def in flow:
            mod_name = mod_def["module"]
            if not mod_def.get("enabled", True):
                logger.info(f"Skipping disabled module: {mod_name}")
                continue

            mod_params = params.get(mod_name, {})
            if model is None and "model" in mod_params:
                model = get_model(**mod_params["model"])

            for stage_def in mod_def.get("stages", []):
                stage_id = stage_def.get("stage")
                if not stage_def.get("enabled", True):
                    logger.info(f"Skipping disabled stage: {stage_id}")
                    continue

                handler_key = stage_def.get("handler", stage_id)
                logger.info(f"Executing stage: {stage_id} via handler '{handler_key}'")

                # Сборка аргументов: общие пути + параметры этапа (этап имеет приоритет)
                stage_kwargs = {**state, **mod_params.get(handler_key, {})}
                if model:
                    stage_kwargs["model"] = model
                
                result = execute_stage(handler_key, stage_kwargs)
                state[f"{stage_id}_result"] = result
                logger.info(f"Stage '{stage_id}' completed successfully.")
            
            model = model if general.get("model") else None

            # Предполагаем, что больше vsm не понадобится, и надо освободить ресурсы
            if mod_name == "schema_linking":
                vsm.close_all()

        metadata["status"] = "completed"
        metadata["completed_at"] = datetime.now().isoformat()
        logger.info("Text-to-SQL completed successfully!")

    except Exception as e:
        metadata["status"] = "failed"
        metadata["completed_at"] = datetime.now().isoformat()
        metadata["error"] = str(e)
        metadata["traceback"] = traceback.format_exc()
        logger.exception(f"Pipeline failed: {e}")
        raise SystemExit(1)
        
    finally:
        # Сохранение метаданных
        meta_path = log_dir / "metadata.json"
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        logger.info(f"Metadata saved to {meta_path}")
        if vsm:
            try: vsm.close_all()
            except: pass


if __name__ == "__main__":
    from dotenv import load_dotenv
    from src.utils.run_manager import set_global_seeds

    load_dotenv(".env")
    set_global_seeds()

    parser = argparse.ArgumentParser(description="Run Text-to-SQL pipeline from JSON configs.")
    parser.add_argument("--params", type=str, required=True, help="Path to parameters JSON file")
    parser.add_argument("--flow", type=str, required=True, help="Path to flow JSON file")
    args = parser.parse_args()

    with ResourceMonitor():
        run_pipeline(args.params, args.flow)
        