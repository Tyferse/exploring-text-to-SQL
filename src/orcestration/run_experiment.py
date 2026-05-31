#!/usr/bin/env python3
import os
import sys
sys.path.insert(0, os.path.abspath("."))

import json
import inspect
import itertools
import logging
import pickle
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional
from copy import deepcopy

from .core import HANDLER_REGISTRY


def load_json(path: str) -> Dict[str, Any]:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def deep_merge(base: Dict, override: Dict) -> Dict:
    result = deepcopy(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = deep_merge(result[k], v)
        else:
            result[k] = deepcopy(v)
    return result

def get_nested_value(d: Dict, path: List[str]) -> Any:
    for k in path:
        if isinstance(d, dict) and k in d:
            d = d[k]
        else:
            return None
    return d

def set_nested_value(d: Dict, path: List[str], value: Any):
    for k in path[:-1]:
        d = d.setdefault(k, {})
    d[path[-1]] = value


def _expand_param_leaves(cfg: Dict, base_params: Dict, path: List[str] = None) -> List[Tuple[List[str], List[Any]]]:
    """Рекурсивно собирает (путь, значения) из scan_config."""
    path = path or []
    leaves = []
    for k, v in cfg.items():
        if isinstance(v, dict) and "type" in v:
            raw_vals = v.get("values", [])
            if v["type"] == "range":
                start, end, step = raw_vals
                vals = list(range(start, end + 1, step))
            else:  # enum
                vals = list(raw_vals)
            leaves.append((path + [k], vals))
        elif isinstance(v, dict):
            leaves.extend(_expand_param_leaves(v, base_params, path + [k]))
    return leaves

def _pad_with_default(vals: List[Any], default: Any, length: int) -> List[Any]:
    return vals + [default] * (length - len(vals)) if len(vals) < length else vals

def generate_param_variants(scan_cfg: Dict, base_params: Dict, is_grid: bool) -> List[Dict[str, Any]]:
    leaves = _expand_param_leaves(scan_cfg, base_params)
    if not leaves:
        return [{}]

    paths = [p for p, _ in leaves]
    value_lists = [v for _, v in leaves]
    max_len = max(len(v) for v in value_lists)

    if is_grid:
        # Cartesian product
        combos = itertools.product(*value_lists)
        variants = []
        for combo in combos:
            v_dict = {}
            for path, val in zip(paths, combo):
                set_nested_value(v_dict, path, val)
            variants.append(v_dict)
        return variants
    else:
        # Row-wise alignment
        padded = []
        for i, vals in enumerate(value_lists):
            default = get_nested_value(base_params, paths[i])
            padded.append(_pad_with_default(vals, default, max_len))
        
        variants = []
        for i in range(max_len):
            v_dict = {}
            for path, vals in zip(paths, padded):
                val = vals[i]
                if val is not None:  # None = skip override, use base default
                    set_nested_value(v_dict, path, val)
            variants.append(v_dict)
        return variants

def resolve_flow_overrides(flow_cfg: Dict, overrides_list: List[Dict], max_len: int) -> List[Dict]:
    """Применяет flow_override к каждому варианту."""
    resolved_flows = []
    for i in range(max_len):
        flow = deepcopy(flow_cfg)
        for mod_override in overrides_list:
            mod_name = mod_override["module"]
            # Поиск модуля в flow
            for mod in flow.get("pipeline", []):
                if mod.get("module") == mod_name:
                    # Уровень модуля
                    mod_enabled = mod_override.get("enabled", [])
                    if len(mod_enabled) > i:
                        mod["enabled"] = mod_enabled[i]
                    
                    # Уровень стадий
                    for stage_override in mod_override.get("stages", []):
                        for stage in mod.get("stages", []):
                            if stage.get("stage") == stage_override.get("stage"):
                                stg_enabled = stage_override.get("enabled", [])
                                if len(stg_enabled) > i:
                                    stage["enabled"] = stg_enabled[i]
        resolved_flows.append(flow)
    return resolved_flows


def execute_stage(handler_name: str, kwargs: Dict[str, Any]) -> Any:
    handler_ref = HANDLER_REGISTRY.get(handler_name)
    if handler_ref is None:
        raise ValueError(f"Handler '{handler_name}' not registered in HANDLER_REGISTRY")

    if inspect.isclass(handler_ref):
        # Это класс: создаём экземпляр и вызываем .run()
        instance = handler_ref(**kwargs)
        result = instance.run()
        
        # Автоматический вызов пост-обработки, если метод существует
        post_methods = ["extract_all_candidates", "finalize", "cleanup", "save_results"]
        for method_name in post_methods:
            if hasattr(instance, method_name):
                getattr(instance, method_name)()
        return result
    else:
        # Это функция: прямой вызов
        return handler_ref(**kwargs)


class CheckpointManager:
    def __init__(self, run_root: Path):
        self.dir = run_root / "checkpoints"
        self.dir.mkdir(parents=True, exist_ok=True)

    def save(self, stage_name: str, state: Dict):
        path = self.dir / f"{stage_name}.pkl"
        with open(path, "wb") as f:
            pickle.dump(state, f)
        logging.info(f"💾 Checkpoint saved: {path}")

    def load(self, stage_name: str) -> Optional[Dict]:
        path = self.dir / f"{stage_name}.pkl"
        if path.exists():
            with open(path, "rb") as f:
                logging.info(f"📦 Checkpoint loaded: {path}")
                return pickle.load(f)
        return None

# ─────────────────────────────────────────────────────────────
# 🚀 Главный оркестратор
# ─────────────────────────────────────────────────────────────

class ExperimentOrchestrator:
    def __init__(self, spec_path: str):
        self.spec = load_json(spec_path)
        self.base_params = load_json(self.spec["params_config"])
        self.base_flow = load_json(self.spec["flow_config"])
        self.checkpoint_stage = self.spec.get("checkpoint_after_stage")
        self.is_grid = self.spec.get("is_param_grid", False)
        
        # Генерируем варианты
        self.param_variants = generate_param_variants(
            self.spec.get("scan_config", {}), 
            self.base_params, 
            self.is_grid
        )
        
        flow_overrides = self.spec.get("flow_override", [])
        self.flow_variants = resolve_flow_overrides(
            self.base_flow, 
            flow_overrides, 
            len(self.param_variants)
        )
        
        self.ckp_mgr: Optional[CheckpointManager] = None
        logging.info(f"🧪 Generated {len(self.param_variants)} experiment variants.")

    def run(self):
        for idx, (p_overrides, f_override) in enumerate(zip(self.param_variants, self.flow_variants)):
            variant_id = f"v{idx+1}_{self.spec['name']}"
            logging.info(f"{'='*50}\n🚀 Starting variant: {variant_id}")
            
            # 1. Сборка финальных конфигов
            final_params = deep_merge(self.base_params, p_overrides)
            final_flow = f_override
            
            run_root = Path("runs") / f"{variant_id}"
            run_root.mkdir(parents=True, exist_ok=True)
            self.ckp_mgr = CheckpointManager(run_root)
            
            # 2. Инициализация состояния
            state = {"general": final_params.get("general", {}), "run_id": variant_id}
            start_from_stage = None
            
            # 3. Проверка чекпоинта
            if self.checkpoint_stage:
                saved = self.ckp_mgr.load(self.checkpoint_stage)
                if saved:
                    state.update(saved)
                    start_from_stage = self.checkpoint_stage
                    logging.info(f"⏭️ Resuming from checkpoint: {self.checkpoint_stage}")

            # 4. Выполнение пайплайна
            for module_def in final_flow.get("pipeline", []):
                module_name = module_def["module"]
                module_params = final_params.get(module_name, {})
                
                for stage_def in module_def.get("stages", []):
                    stage_name = stage_def.get("stage")
                    handler_name = stage_def.get("handler", stage_name)
                    
                    # Пропуск по flow_override
                    if not stage_def.get("enabled", True):
                        logging.info(f"⏭️ Disabled stage: {stage_name}")
                        continue
                    
                    # Пропуск до чекпоинта
                    if start_from_stage and stage_name != start_from_stage:
                        # Если мы уже загрузили чекпоинт, пропускаем все ДО него
                        if not getattr(self, "_checkpoint_reached", False):
                            logging.info(f"⏩ Skipping (before checkpoint): {stage_name}")
                            continue
                        else:
                            start_from_stage = None  # Сброс после прохождения
                    else:
                        self._checkpoint_reached = True

                    logging.info(f"🔹 Executing stage: {stage_name} ({handler_name})")
                    
                    # Сборка аргументов
                    stage_args = module_params.get(handler_name, {})
                    stage_args.update(state["general"])  # input_data_root, storage_root и т.д.
                    
                    try:
                        result = execute_stage(handler_name, stage_args)
                        state[f"{stage_name}_result"] = result
                        logging.info(f"✅ Stage {stage_name} completed.")
                        
                        # Сохранение чекпоинта, если нужно
                        if stage_name == self.checkpoint_stage:
                            self.ckp_mgr.save(stage_name, state)
                            
                    except Exception as e:
                        logging.error(f"❌ Stage {stage_name} failed: {e}", exc_info=True)
                        break

            logging.info(f"🏁 Variant {variant_id} finished.\n{'='*50}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run parameterized pipeline experiments.")
    parser.add_argument("--spec", type=str, required=True, help="Path to experiment spec JSON")
    parser.add_argument("--verbose", action="store_true", help="Debug logging")
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)-8s | %(message)s"
    )
        
    orchestrator = ExperimentOrchestrator(args.spec)
    orchestrator.run()
