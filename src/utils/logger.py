import os
import time
import threading
import logging
import psutil
from datetime import datetime
from pathlib import Path
from typing import Optional, Union, Dict, List
from torch import cuda


def get_logger(
    name: str,
    log_file: Optional[Union[str, Path]] = None,
    level: Union[int, str] = logging.INFO,
    fmt: Optional[str] = None,
    console: bool = True,
    file: bool = True,
    mode: str = 'w',
    encoding: str = 'utf-8',
    force_reconfigure: bool = False
) -> logging.Logger:
    """
    Универсальная фабрика логгеров.
    Возвращает настроенный экземпляр logging.Logger, который можно
    передавать как аргумент или хранить как атрибут класса/экземпляра.
    
    Args:
        name: Уникальное имя логгера (обычно __name__).
        log_file: Путь к файлу логов. Если None, файловый хендлер не создается.
        level: Уровень логирования (int или str: 'DEBUG', 'INFO', 'WARNING', 'ERROR').
        fmt: Формат сообщения. По умолчанию: '%(asctime)s | %(name)s | %(levelname)-8s | %(message)s'
        console: Включить вывод в консоль.
        file: Включить запись в файл (если указан log_file).
        mode: Режим открытия файла ('a' - добавление, 'w' - перезапись).
        encoding: Кодировка файла.
        force_reconfigure: Если True, очищает старые хендлеры и применяет настройки заново.
        
    Returns:
        Настроенный экземпляр logging.Logger.
    """
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)

    logger = logging.getLogger(name)

    if not force_reconfigure and logger.handlers:
        return logger

    logger.handlers.clear()
    logger.setLevel(level)
    logger.propagate = False  # Отключаем всплытие в root logger, чтобы не было дублей

    if fmt is None:
        fmt = '%(asctime)s | %(name)s | %(levelname)-8s | %(message)s'
        
    formatter = logging.Formatter(fmt, datefmt='%Y-%m-%d %H:%M:%S')

    if console:
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    if file and log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(str(log_path), mode=mode, encoding=encoding)
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


def attach_shared_file_handler(
    log_file: Union[str, Path],
    logger_names: List[str],
    level: Union[int, str] = logging.INFO,
    fmt: Optional[str] = None,
    mode: str = 'a',
    encoding: str = 'utf-8'
) -> logging.FileHandler:
    """
    Прикрепляет единый файловый обработчик к списку логгеров.
    Все логи указанных модулей будут писаться в один файл.
    """
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)

    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    if fmt is None:
        fmt = '%(asctime)s | %(name)s | %(levelname)-8s | %(message)s'

    formatter = logging.Formatter(fmt, datefmt='%Y-%m-%d %H:%M:%S')

    file_handler = logging.FileHandler(str(log_path), mode=mode, encoding=encoding)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    for name in logger_names:
        logger = logging.getLogger(name)
        # Добавляем только если такого обработчика ещё нет
        if not any(h is file_handler for h in logger.handlers):
            logger.addHandler(file_handler)
            # Убеждаемся, что логгер не отфильтрует записи раньше обработчика
            if logger.level > level:
                logger.setLevel(level)

    return file_handler


def setup_resource_logger(log_path: str) -> logging.Logger:
    """Создаёт уникальный лог-файл для каждого запуска."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_path, f"resource_monitor_{timestamp}.log")
    
    logger = logging.getLogger(f"ResourceMonitor_{timestamp}")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.FileHandler(log_file, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(handler)

    return logger


class ResourceMonitor:
    """Монитор потребления ресурсов текущим процессом (RAM, CPU, VRAM)."""
    
    def __init__(self, sample_interval: float = 1.0, log_dir: str = "logs/resources"):
        self.interval = sample_interval
        self.main_proc = psutil.Process(os.getpid())
        self.peak_ram_mb: float = 0.0
        self.peak_vram_mb: float = 0.0
        self.cpu_samples: list[float] = []
        self.num_recodrs = 0
        self.running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        os.makedirs(log_dir, exist_ok=True)
        self.logger = setup_resource_logger(log_dir)
        self.gpu_available = False
        self.nvml_handle = None
        
        # Инициализация CPU
        self.cpu_cores_logical = psutil.cpu_count(logical=True)
        self.cpu_cores_physical = psutil.cpu_count(logical=False) or self.cpu_cores_logical
        self.logger.info(f"CPU cores: {self.cpu_cores_physical} physical, {self.cpu_cores_logical} logical")
        
        # Инициализация GPU (NVIDIA)
        try:
            device_count = cuda.device_count()
            if device_count > 0:
                self.gpu_available = cuda.is_available()
                self.logger.info("GPU OK")
            else:
                self.logger.warning("No NVIDIA GPU detected.")
        except Exception as e:
            self.logger.warning(f"CUDA init failed: {e}. VRAM tracking disabled.")

    def _get_process_tree(self) -> List[psutil.Process]:
        """Возвращает главный процесс + все рекурсивные дочерние."""
        try:
            children = self.main_proc.children(recursive=True)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            children = []
        return [self.main_proc] + children

    def _monitor_loop(self) -> None:
        self.logger.info("Resource monitor thread started.")
        # "Прогрев" CPU для всех текущих процессов
        for p in self._get_process_tree():
            try: 
                p.cpu_percent(interval=None)
            except: 
                pass

        while self.running:
            try:
                time.sleep(self.interval)

                procs = self._get_process_tree()
                total_ram_mb = 0.0
                total_cpu_pct = 0.0
                active_procs = 0
                for p in procs:
                    try:
                        total_ram_mb += p.memory_info().rss / (1024**2)
                        total_cpu_pct += p.cpu_percent(interval=None)
                        active_procs += 1
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        continue

                with self._lock:
                    # 1. ОЗУ процесса
                    if self.peak_ram_mb < total_ram_mb:
                        self.peak_ram_mb = total_ram_mb

                    # 2. Использование CPU (в %)
                    self.cpu_samples.append(total_cpu_pct)

                # 3. Видеопамять (VRAM)
                vram_msg = "N/A"
                if self.gpu_available:
                    try:
                        vram_used_mb = cuda.memory_allocated() / (1024**2)
                        with self._lock:
                            if self.peak_vram_mb < vram_used_mb:
                                self.peak_vram_mb = vram_used_mb

                        vram_msg = f"{vram_used_mb:.1f}MB"
                    except Exception:
                        pass
                
                self.logger.info(f"RAM: {total_ram_mb:.1f}MB | CPU: {total_cpu_pct:.1f}% | VRAM: {vram_msg} | Procs: {active_procs}")
            except Exception as e:
                self.logger.error(f"Monitoring loop error: {e}")
                break

        self.logger.info("Resource monitoring thread stopped.")

    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
        self.logger.info("Monitor started.")

    def stop(self) -> None:
        self.running = False
        if self._thread:
            self._thread.join()

        self.logger.info("Monitor stopped and joined.")

    def get_stats(self) -> Dict:
        with self._lock:
            avg_cpu = sum(self.cpu_samples) / len(self.cpu_samples) if self.cpu_samples else 0.0
            return {
                "peak_ram_mb": round(self.peak_ram_mb, 2) if self.peak_ram_mb else None,
                "peak_vram_mb": round(self.peak_vram_mb, 2) if self.gpu_available else None,
                "avg_cpu_percent": round(avg_cpu, 2),
                "cpu_cores_logical": self.cpu_cores_logical,
                "cpu_cores_physical": self.cpu_cores_physical,
                "samples_count": len(self.cpu_samples)
            }

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    # def __del__(self):
    #     if self.gpu_available:
    #         try:
    #             pynvm
    #         except Exception:
    #             pass
