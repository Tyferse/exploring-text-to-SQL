import sys
sys.path.insert(0, '.')

import multiprocessing as mp
import time
import numpy as np
import logging
from src.utils.logger import ResourceMonitor

def worker_task():
    _ = np.random.rand(5000, 5000) 
    time.sleep(3)

if __name__ == "__main__":
    with ResourceMonitor(sample_interval=0.5) as monitor:
        print("Запуск 4 дочерних процессов...")
        processes = [mp.Process(target=worker_task) for _ in range(4)]
        for p in processes: p.start()
        for p in processes: p.join()
        
    stats = monitor.get_stats()
    print("\n📊 Итоги (Главный процесс + 4 дочерних):")
    print(f"• Пик ОЗУ: {stats['peak_ram_mb']:.2f} MB")
    print(f"• Пик VRAM: {stats['peak_vram_mb']:.2f} MB" if stats['peak_vram_mb'] else "• VRAM: N/A")
    print(f"• Средняя нагрузка CPU (сумма по всем процессам): {stats['avg_cpu_percent']:.1f}%")
    log_path = next(h.baseFilename for h in monitor.logger.handlers if isinstance(h, logging.FileHandler))
    print(f"📄 Лог: {log_path}")
