#!/usr/bin/env python3
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import os
import json
import argparse
import logging
import traceback
from pathlib import Path
from typing import Any, Dict, List
from datetime import datetime
from copy import deepcopy

from .core import register_handlers, execute_stage, HANDLER_REGISTRY


def load_json(path: str) -> Dict[str, Any]:
    p = Path(path).resolve()
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {p}")
    with open(p, 'r', encoding='utf-8') as f:
        return json.load(f)

def validate_configs(params: Dict, flow: Dict) -> List[str]:
    """Проверяет целостность и совместимость конфигов перед запуском."""
    errors = []
    
    if "general" not in params:
        errors.append("Missing 'general' section in params config")
    if "pipeline" not in flow:
        errors.append("Missing 'pipeline' array in flow config")
        
    # Проверяем наличие всех хендлеров из flow в реестре
    for mod_def in flow.get("pipeline", []):
        if not mod_def.get("enabled", True):
            continue
        for stage_def in mod_def.get("stages", []):
            if not stage_def.get("enabled", True):
                continue
            handler_key = stage_def.get("handler", stage_def.get("stage"))
            if handler_key not in HANDLER_REGISTRY:
                errors.append(f"Handler '{handler_key}' is referenced in flow but not registered in core.py")

    # Проверяем наличие параметров для включенных этапов
    for mod_def in flow.get("pipeline", []):
        if not mod_def.get("enabled", True):
            continue
        mod_name = mod_def["module"]
        if mod_name not in params:
            # Не критично, но стоит предупредить
            logging.warning(f"⚠️ Module '{mod_name}' enabled in flow but missing in params. Will use empty kwargs.")

    return errors

def setup_logging(run_id: str, verbose: bool = False) -> Path:
    log_dir = Path("logs") / run_id
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Очищаем старые хендлеры, чтобы избежать дублирования при повторных вызовах
    root = logging.getLogger()
    if root.handlers:
        for h in root.handlers:
            root.removeHandler(h)
            h.close()

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)-8s | %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_dir / "pipeline.log", encoding="utf-8")
        ]
    )
    return log_dir


def run_pipeline(params_path: str, flow_path: str, verbose: bool = False):
    # 1. Загрузка
    params = load_json(params_path)
    flow = load_json(flow_path)

    # 2. Регистрация хендлеров и валидация
    register_handlers()
    validation_errors = validate_configs(params, flow)
    if validation_errors:
        for err in validation_errors:
            logging.error(f"❌ Validation: {err}")
        raise SystemExit("Pipeline aborted due to configuration errors.")

    # 3. Инициализация запуска
    general = params.get("general", {})
    run_id = general.get("run_name", f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    log_dir = setup_logging(run_id, verbose)
    logger = logging.getLogger(__name__)

    logger.info(f"🚀 Pipeline starting | Run ID: {run_id}")
    logger.info(f"📂 Params: {params_path} | Flow: {flow_path}")

    state: Dict[str, Any] = {"general": general, "run_id": run_id}
    metadata = {
        "run_id": run_id,
        "started_at": datetime.now().isoformat(),
        "status": "running",
        "params_file": params_path,
        "flow_file": flow_path
    }

    # 4. Последовательное выполнение
    try:
        for mod_def in flow.get("pipeline", []):
            mod_name = mod_def["module"]
            if not mod_def.get("enabled", True):
                logger.info(f"⏭️ Skipping disabled module: {mod_name}")
                continue

            mod_params = params.get(mod_name, {})

            for stage_def in mod_def.get("stages", []):
                stage_id = stage_def.get("stage")
                if not stage_def.get("enabled", True):
                    logger.info(f"⏭️ Skipping disabled stage: {stage_id}")
                    continue

                handler_key = stage_def.get("handler", stage_id)
                logger.info(f"🔹 Executing stage: {stage_id} via handler '{handler_key}'")

                # Сборка аргументов: общие пути + параметры этапа (этап имеет приоритет)
                stage_kwargs = {**state["general"], **mod_params.get(handler_key, {})}
                
                # Выполнение
                result = execute_stage(handler_key, stage_kwargs)
                state[f"{stage_id}_result"] = result
                logger.info(f"✅ Stage '{stage_id}' completed successfully.")

        metadata["status"] = "completed"
        metadata["completed_at"] = datetime.now().isoformat()
        logger.info("🎉 Pipeline completed successfully!")

    except Exception as e:
        metadata["status"] = "failed"
        metadata["completed_at"] = datetime.now().isoformat()
        metadata["error"] = str(e)
        metadata["traceback"] = traceback.format_exc()
        logger.exception(f"❌ Pipeline failed: {e}")
        raise SystemExit(1)
        
    finally:
        # Сохранение метаданных
        meta_path = log_dir / "metadata.json"
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        logger.info(f"💾 Metadata saved to {meta_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Text-to-SQL pipeline from JSON configs.")
    parser.add_argument("--params", type=str, required=True, help="Path to parameters JSON")
    parser.add_argument("--flow", type=str, required=True, help="Path to flow JSON")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    run_pipeline(args.params, args.flow, args.verbose)
