import os
import hashlib
import json
from datetime import datetime
from typing import Optional, Dict, Any


def generate_run_id(
    input_data_root: str,
    custom_suffix: Optional[str] = None,
    timestamp: Optional[str] = None
) -> str:
    """
    Генерирует уникальный, детерминированный run_id.
    
    Формат: {timestamp}_{input_hash}_{suffix}
    
    Args:
        input_data_root: Путь к данным (влияет на хэш).
        custom_suffix: Опциональная метка (например, "harrier_test").
        timestamp: Время запуска (по умолчанию — текущее).
    """
    # Нормализуем путь для стабильного хэша
    normalized_path = input_data_root.replace("\\", "/").lower().strip("/")
    
    # Хэш от пути + суффикса
    hash_input = f"{normalized_path}:{custom_suffix or ''}"
    path_hash = hashlib.md5(hash_input.encode()).hexdigest()[:8]
    
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Финальный ID
    suffix_part = f"_{custom_suffix}" if custom_suffix else ""
    return f"{timestamp}_{path_hash}{suffix_part}"


def resolve_run_id(
    runs_root: str = "runs",
    input_data_root: Optional[str] = None,
    custom_suffix: Optional[str] = None,
    use_latest: bool = True
) -> str:
    """
    Возвращает run_id: либо новый, либо последний существующий.
    
    Args:
        runs_root: Корневая папка для запусков.
        input_data_root: Если указан + use_latest=True, ищет запуски с таким же хэшем.
        custom_suffix: Фильтр по суффиксу при поиске последнего.
        use_latest: Если True и есть подходящие запуски — возвращает последний по времени.
    """
    if not os.path.exists(runs_root):
        return generate_run_id(input_data_root or "default", custom_suffix)
    
    if use_latest and input_data_root:
        # Ищем запуски с таким же хэшем пути
        normalized_root = input_data_root.replace('\\', '/').lower().strip('/')
        target_hash = hashlib.md5(
            f"{normalized_root}:{custom_suffix or ''}".encode()
        ).hexdigest()[:8]
        
        candidates = []
        for run_dir in os.listdir(runs_root):
            if os.path.isdir(os.path.join(runs_root, run_dir)) and run_dir.split('_', 2)[1] == target_hash:
                # Парсим время из имени: {timestamp}_{hash}_{suffix}
                try:
                    parts = run_dir.split("_")
                    ts = parts[0]
                    if custom_suffix and len(parts) == 3:
                        if parts[2] != custom_suffix:
                            continue
                    candidates.append((run_dir, ts))
                except:
                    continue
        
        if candidates:
            # Сортируем по времени (предполагаем формат YYYYMMDD_HHMMSS)
            candidates.sort(key=lambda x: x[1], reverse=True)
            return candidates[0][0]
    
    # Если не нашли или use_latest=False — генерируем новый
    return generate_run_id(input_data_root or "default", custom_suffix)


def get_run_path(run_id: str, runs_root: str = "runs", stage: Optional[str] = None) -> str:
    """
    Возвращает путь к папке запуска (и опционально — к этапу внутри).
    """
    base = os.path.join(runs_root, run_id)
    if stage:
        return os.path.join(base, *stage.split("\\"))
    return base


def save_run_metadata(run_id: str, metadata: Dict[str, Any], runs_root: str = "runs") -> None:
    """Сохраняет метаданные запуска в runs/{run_id}/metadata.json."""
    path = get_run_path(run_id, runs_root)
    os.makedirs(path, exist_ok=True)
    
    meta_file = os.path.join(path, "metadata.json")
    with open(meta_file, 'w', encoding='utf-8') as f:
        json.dump({
            "run_id": run_id,
            "created_at": datetime.now().isoformat(),
            **metadata
        }, f, indent=2, ensure_ascii=False)


def load_run_metadata(run_id: str, runs_root: str = "runs") -> Optional[Dict[str, Any]]:
    """Загружает метаданные запуска."""
    meta_file = os.path.join(get_run_path(run_id, runs_root), "metadata.json")
    if os.path.exists(meta_file):
        with open(meta_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None
