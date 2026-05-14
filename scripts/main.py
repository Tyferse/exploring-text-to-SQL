import sys
sys.path.insert(0, ".")

from src.utils.logger import ResourceMonitor
from src.utils.preprocessing import spider2preprocess


if __name__ == "__main__":
    with ResourceMonitor() as monitor:
        spider2preprocess("Spider2\spider2-lite", is_multidialect=True, max_workers=8, force_update=True)
    
    print(monitor.get_stats())
