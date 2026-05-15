import sys
sys.path.insert(0, ".")

from src.utils.gen_embeddings import gen_column_embeddings
from src.utils.logger import ResourceMonitor
from src.utils.preprocessing import spider2preprocess


if __name__ == "__main__":
    with ResourceMonitor() as monitor:
        preprocessing_results = spider2preprocess(
            "Spider2\spider2-lite", is_multidialect=True, max_workers=8, force_update=True
        )
        print(monitor.get_stat())

        gen_column_embeddings(
            "Spider2\spider2-lite", embedding_model="microsoft/harrier-oss-v1-270m", 
            device='cpu', max_workers=2
        )

    print(monitor.get_stats())
