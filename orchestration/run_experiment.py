import argparse
import itertools
import json
import shutil
import sys
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from orchestration.run_pipeline import run_pipeline, load_json, _deep_merge
from src.utils.logger import get_logger, ResourceMonitor
from src.utils.run_manager import resolve_run_id


def _expand_param_leaves(cfg: Dict, path: List[str] = None) -> List[Tuple[List[str], Dict]]:
    """
    Рекурсивно собирает (путь, спецификация) из scan_config.
    Возвращает: [([path, to, param], {"type": "...", "values": [...]}), ...]
    """
    path = path or []
    leaves = []
    
    for k, v in cfg.items():
        if isinstance(v, dict):
            if "type" in v and "values" in v:
                leaves.append((path + [k], v))
            else:
                leaves.extend(_expand_param_leaves(v, path + [k]))

    return leaves


def _generate_range_values(range_spec: List[int]) -> List[Any]:
    """Генерирует значения из range спецификации [start, end, step]."""
    if len(range_spec) != 3:
        raise ValueError(f"Range spec must be [start, end, step], got {range_spec}")
    
    start, end, step = range_spec
    return list(range(start, end + 1, step))


def _get_nested_value(d: Dict, path: List[str]) -> Any:
    """Получает значение из вложенного словаря по пути."""
    for k in path:
        if isinstance(d, dict) and k in d:
            d = d[k]
        else:
            return None
        
    return d


def _set_nested_value(d: Dict, path: List[str], value: Any):
    """Устанавливает значение во вложенный словарь по пути."""
    for k in path[:-1]:
        d = d.setdefault(k, {})

    d[path[-1]] = value


def generate_variants(
    scan_config: Dict, 
    base_params: Dict, 
    is_grid: bool = False
) -> List[Dict[str, Any]]:
    """
    Генерирует список вариантов параметров на основе scan_config.
    
    Args:
        scan_config: Конфигурация сканирования параметров
        base_params: Базовые параметры для заполнения дефолтов
        is_grid: True → декартово произведение (grid search)
                False → построчное выравнивание (row-wise)
    
    Returns:
        Список словарей с переопределениями параметров для каждого варианта
    """
    leaves = _expand_param_leaves(scan_config)
    
    if not leaves:
        return [{}]  # Один пустой вариант
    
    paths = [path for path, _ in leaves]
    specs = [spec for _, spec in leaves]
    
    # Генерируем списки значений
    value_lists = []
    for spec in specs:
        if spec["type"] == "range":
            values = _generate_range_values(spec["values"])
        elif spec["type"] == "enum":
            values = list(spec["values"])
        value_lists.append(values)
    
    if is_grid:
        # Декартово произведение
        variants = []
        for combo in itertools.product(*value_lists):
            variant = {}
            for path, val in zip(paths, combo):
                _set_nested_value(variant, path, val)
            variants.append(variant)

        return variants
    else:
        # Построчное выравнивание с паддингом дефолтами
        max_len = max(len(v) for v in value_lists)
        
        padded_lists = []
        for path, values in zip(paths, value_lists):
            default = _get_nested_value(base_params, path)
            padded = values + [default] * (max_len - len(values))
            padded_lists.append(padded)
        
        variants = []
        for i in range(max_len):
            variant = {}
            for path, values in zip(paths, padded_lists):
                val = values[i]
                if val is not None:  # None = не переопределять
                    _set_nested_value(variant, path, val)
            variants.append(variant)
        
        return variants


def apply_flow_overrides(
    base_flow: List[Dict], 
    overrides_list: List[Dict], 
    max_len: int
) -> List[List[Dict]]:
    """
    Применяет flow_override к каждому варианту.
    """
    result = []
    
    for i in range(max_len):
        flow = deepcopy(base_flow)
        
        for override in overrides_list:
            mod_name = override.get("module")
            
            # Module-level enabled override
            if "enabled" in override:
                enabled_list = override["enabled"] 
                if isinstance(enabled_list, list) and i < len(enabled_list):
                    enabled_val = enabled_list[i]
                else:
                    enabled_val = enabled_list

                for mod in flow:
                    if mod.get("module") == mod_name:
                        mod["enabled"] = enabled_val
                        break
                
            # Stage-level enabled override
            if "stages" in override:
                for stage_override in override["stages"]:
                    stage_name = stage_override.get("stage")
                    if "enabled" in stage_override:
                        enabled_list = stage_override["enabled"]
                        if isinstance(enabled_list, list) and i < len(enabled_list):
                            enabled_val = enabled_list[i]
                        else:
                            enabled_val = enabled_list
                    
                        for mod in flow:
                            if mod.get("module") == mod_name:
                                for stage in mod.get("stages", []):
                                    if stage.get("stage") == stage_name:
                                        stage["enabled"] = enabled_val
                                        break
                                break
        
        result.append(flow)
    
    return result


class ExperimentMetadata:
    """Управляет метаданными эксперимента: статусы вариантов, конфиги, чекпоинты."""
    
    def __init__(self, experiment_name: str, experiments_root: str = "logs/experiments"):
        self.experiments_root = Path(experiments_root)
        self.experiment_name = experiment_name
        self.exp_dir = self.experiments_root / experiment_name
        self.exp_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_path = self.exp_dir / "experiment.json"
        
        if self.metadata_path.exists():
            self.metadata = load_json(str(self.metadata_path))
        else:
            self.metadata = {
                "experiment_name": experiment_name,
                "created_at": datetime.now().isoformat(),
                "status": "running",
                "variants": {},
                "checkpoint_after_stage": None,
                "base_run_id": None
            }
    
    def save(self):
        with open(self.metadata_path, 'w', encoding='utf-8') as f:
            json.dump(self.metadata, f, indent=2, ensure_ascii=False)
    
    def mark_variant_started(self, variant_id: str, run_id: str, config: Dict, flow: List):
        self.metadata["variants"][variant_id] = {
            "run_id": run_id,
            "status": "running",
            "started_at": datetime.now().isoformat(),
            "config_snapshot": deepcopy(config),
            "flow_snapshot": deepcopy(flow)
        }
        self.save()
    
    def mark_variant_completed(self, variant_id: str, success: bool, error: Optional[str] = None):
        if variant_id in self.metadata["variants"]:
            self.metadata["variants"][variant_id].update({
                "status": "completed" if success else "failed",
                "completed_at": datetime.now().isoformat(),
                "success": success
            })
            if error:
                self.metadata["variants"][variant_id]["error"] = error

            self.save()
    
    def is_variant_completed(self, variant_id: str) -> bool:
        variant = self.metadata["variants"].get(variant_id, {})
        return variant.get("status") == "completed" and variant.get("success", False)
    
    def get_completed_variants(self) -> List[str]:
        return [
            vid for vid, data in self.metadata["variants"].items()
            if data.get("status") == "completed" and data.get("success", False)
        ]
    
    def set_checkpoint_info(self, stage: str, base_run_id: str):
        self.metadata["checkpoint_after_stage"] = stage
        self.metadata["base_run_id"] = base_run_id
        self.save()
    
    def finalize(self):
        self.metadata["status"] = "completed"
        self.metadata["completed_at"] = datetime.now().isoformat()
        self.save()


def truncate_flow_for_checkpoint(flow: List[Dict], checkpoint_stage: str) -> List[Dict]:
    """
    Обрезает flow, оставляя только этапы ДО checkpoint_stage (включительно).
    """
    truncated = []
    found_checkpoint = False
    
    for mod_def in flow:
        mod_copy = deepcopy(mod_def)
        stages_to_keep = []
        
        for stage_def in mod_def.get("stages", []):
            if found_checkpoint:
                break

            stages_to_keep.append(stage_def)
            if stage_def.get("stage") == checkpoint_stage:
                found_checkpoint = True
                break
        
        if stages_to_keep:
            mod_copy["stages"] = stages_to_keep
            truncated.append(mod_copy)
            
        if found_checkpoint:
            break
            
    return truncated

def copy_run_directory(src_run_id: str, dst_run_id: str, runs_root: str = "logs/runs"):
    """
    Копирует директорию запуска для создания идентичного начального состояния.
    """
    src_path = Path(runs_root) / src_run_id
    dst_path = Path(runs_root) / dst_run_id
    
    if not src_path.exists():
        raise FileNotFoundError(f"Checkpoint source not found: {src_path}")
    
    if dst_path.exists():
        return
        # shutil.rmtree(dst_path)
        
    shutil.copytree(src_path, dst_path)
    
    # Важно: обновить metadata.json в копии, чтобы он отражал новый run_id
    meta_path = dst_path / "metadata.json"
    if meta_path.exists():
        with open(meta_path, 'r', encoding='utf-8') as f:
            meta = json.load(f)

        meta["run_id"] = dst_run_id
        meta["status"] = "resumed_from_checkpoint"
        meta["checkpoint_source"] = src_run_id
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(meta, f, indent=2)

def run_experiment(spec_path: str, skip_completed: bool = True):
    """
    Запускает эксперимент с управлением множественными вариантами.
    
    Args:
        spec_path: Путь к спецификации эксперимента (относительно config/experiments/)
        skip_completed: Пропускать уже успешно завершённые варианты
        verbose: Включить отладочное логирование
    """
    # 1. Загрузка спецификации
    spec = load_json(Path("config") / "experiments" / spec_path)
    experiment_name = spec.get("name", "unnamed_experiment")
    experiments_root = Path(spec.get("experiments_root", "logs/experiments"))
    experiments_root.mkdir(parents=True, exist_ok=True)

    # Инициализация метаданных
    exp_meta = ExperimentMetadata(
        experiments_root=str(experiments_root),
        experiment_name=experiment_name
    )

    # Загрузка базовых конфигов
    params_config = spec.get("params_config")
    flow_config = spec.get("flow_config")
    logger = get_logger(f"exp_{experiment_name}", str(exp_meta.exp_dir / "exp.log"))
    
    if not flow_config:
        raise ValueError("Experiment spec must include 'flow_config'")
    
    # Загружаем базовые параметры
    default_params_path = PROJECT_ROOT / "config" / "default_params.json"
    default_params = load_json(str(default_params_path)) if default_params_path.exists() else {}
    
    if params_config is not None:
        base_params_path = PROJECT_ROOT / "config" / "params" / params_config
        base_params_user = load_json(str(base_params_path))
    else:
        base_params_user = {}

    base_params = _deep_merge(default_params, base_params_user)
    
    base_flow_path = PROJECT_ROOT / "config" / "flow" / flow_config
    base_flow = load_json(str(base_flow_path))
    
    # 2. Генерация вариантов
    scan_config = spec.get("scan_config", {})
    is_grid = spec.get("is_param_grid", False)
    
    param_variants = generate_variants(scan_config, base_params, is_grid)
    
    # Обработка flow_override
    flow_overrides = spec.get("flow_override", [])
    max_variants = len(param_variants)
    flow_has_lists = False
    for module in flow_overrides:
        if "enabled" in module and isinstance(module["enabled"], list):
            max_variants = max(max_variants, len(module["enabled"]))
            flow_has_lists = True

        for stage in module.get("stages", []):
            if "enabled" in stage and isinstance(stage["enabled"], list):
                max_variants = max(max_variants, len(stage["enabled"]))
                flow_has_lists = True

    if is_grid and flow_has_lists:
        raise ValueError("List arguments for flow_override are not supported if \"is_param_grid\": true, use only one value instead.")

    flow_variants = apply_flow_overrides(base_flow, flow_overrides, max_variants)
    
    logger.info(f"Experiment '{experiment_name}': generated {max_variants} variants")
    
    (exp_meta.exp_dir / "params").mkdir(parents=True, exist_ok=True)
    (exp_meta.exp_dir / "flow").mkdir(parents=True, exist_ok=True)

    # 3. Обработка чекпоинтов
    checkpoint_stage = spec.get("checkpoint_after_stage")
    base_run_name = base_params.get("general", {}).get("run_name", "exp")
    if checkpoint_stage:
        base_run_id = resolve_run_id(
            base_params["general"]["runs_root"],
            base_params["general"]["input_data_root"], 
            f"{base_run_name}_base"
        )
        if skip_completed and exp_meta.metadata.get("base_completed"):
            logger.info(f"Base checkpoint '{base_run_id}' already completed.")
        else:
            logger.info(f"Running BASE checkpoint up to '{checkpoint_stage}'...")
            
            # Обрезаем flow
            base_flow_truncated = truncate_flow_for_checkpoint(base_flow, checkpoint_stage)
            
            # Сохраняем конфиги базы в папку эксперимента
            base_params_path_exp = exp_meta.exp_dir / "params" / "base.json"
            base_flow_path_exp = exp_meta.exp_dir / "flow" / "base.json"
            
            checkpoint_params = deepcopy(base_params)
            checkpoint_params["general"]["run_name"] = f"{base_run_name}_base"
            checkpoint_params["general"]["run_id"] = base_run_id

            with open(base_params_path_exp, 'w') as f: json.dump(checkpoint_params, f, indent=2)
            with open(base_flow_path_exp, 'w') as f: json.dump(base_flow_truncated, f, indent=2)
            
            # Запускаем пайплайн
            try:
                with ResourceMonitor():
                    run_pipeline(
                        params_config=checkpoint_params,
                        flow_config=base_flow_truncated
                    )

                exp_meta.metadata["base_completed"] = True
                exp_meta.save()
                logger.info(f"Base checkpoint completed.")
            except Exception as e:
                logger.error(f"Base checkpoint failed: {e}")
                raise SystemExit("Base checkpoint failed. Aborting experiment.")

    # 4. Запуск вариантов
    for idx in range(max_variants):
        variant_id = f"v{idx + 1}"
        
        # Пропуск завершённых вариантов
        if skip_completed and exp_meta.is_variant_completed(variant_id):
            logger.info(f"Skipping completed variant: {variant_id}")
            continue
        
        # Подготовка конфигов для варианта
        param_override = param_variants[idx]
        flow_variant = flow_variants[idx]
        
        # Слияние параметров
        variant_params = _deep_merge(deepcopy(base_params), param_override)

        # Определение run_name
        variant_params.setdefault("general", {})["run_name"] = f"{base_run_name}_{variant_id}"
        variant_run_id = resolve_run_id(
            variant_params["general"]["runs_root"], 
            variant_params["general"]["input_data_root"], 
            variant_params["general"]["run_name"]
        )
        if checkpoint_stage and not Path(variant_params["general"]["runs_root"], variant_run_id).exists():
            copy_run_directory(base_run_id, variant_run_id, variant_params["general"]["runs_root"])
        else:
            dst_path = Path(variant_params["general"]["runs_root"]) / variant_run_id
        
        logger.info(f"Starting variant {variant_id}: {variant_params['general']['run_name']}")
        
        # Отмечаем начало в метаданных
        exp_meta.mark_variant_started(
            variant_id, 
            variant_params["general"]["run_name"], 
            variant_params, 
            flow_variant
        )
        
        try:
            # Сохраняем конфиг варианта
            variant_params_path = exp_meta.exp_dir / "params" / f"{variant_id}.json"
            with open(variant_params_path, 'w', encoding='utf-8') as f:
                json.dump(variant_params, f, indent=2, ensure_ascii=False)
            
            # Для flow: если есть изменения, создаём отдельный файл
            if flow_variant != base_flow:
                variant_flow_path = exp_meta.exp_dir / "flow" / f"{variant_id}.json"
                with open(variant_flow_path, 'w', encoding='utf-8') as f:
                    json.dump(flow_variant, f, indent=2, ensure_ascii=False)
                
            # Вызов run_pipeline с временными конфигами
            with ResourceMonitor():
                run_pipeline(params_config=variant_params, flow_config=flow_variant)
            
            # Отмечаем успешное завершение
            exp_meta.mark_variant_completed(variant_id, success=True)
            logger.info(f"Variant {variant_id} completed successfully")
            
        except SystemExit as e:
            if e.code != 0:
                exp_meta.mark_variant_completed(variant_id, success=False, error=f"SystemExit({e.code})")
                logger.error(f"Variant {variant_id} failed with SystemExit({e.code})")
                if not spec.get("continue_on_error", False):
                    raise
            else:
                exp_meta.mark_variant_completed(variant_id, success=True)
                
        except Exception as e:
            exp_meta.mark_variant_completed(variant_id, success=False, error=str(e))
            logger.exception(f"Variant {variant_id} failed: {e}")
            if not spec.get("continue_on_error", False):
                raise
        
    # 5. Завершение эксперимента
    exp_meta.finalize()
    logger.info(f"Experiment '{experiment_name}' completed")
    
    # Итоговый отчёт
    completed = exp_meta.get_completed_variants()
    logger.info(f"Summary: {len(completed)}/{max_variants} variants completed successfully")
    
    return exp_meta.metadata


if __name__ == "__main__":
    from dotenv import load_dotenv
    from src.utils.run_manager import set_global_seeds

    load_dotenv(".env")
    set_global_seeds()
    
    parser = argparse.ArgumentParser(
        description="Run parameterized experiments for Text-to-SQL pipeline.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--spec", type=str, required=True, 
        help="Experiment spec JSON (relative to config/experiments/ or absolute path)"
    )
    parser.add_argument(
        "--no-skip-completed", action="store_true", 
        help="Re-run variants that already completed successfully"
    )
    args = parser.parse_args()
    
    run_experiment(
        spec_path=args.spec,
        skip_completed=not args.no_skip_completed
    )
