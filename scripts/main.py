import sys
sys.path.insert(0, ".")

from src.utils.preprocessing import spider2preprocess


if __name__ == "__main__":
    spider2preprocess("Spider2\spider2-lite", is_multidialect=True, max_workers=4, force_update=True)
