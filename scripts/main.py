import sys
sys.path.insert(0, ".")

import os
from src.modules.schema_linking.retrieve_schema import retrieve_columns
from src.storage.docker_qdrant import ensure_qdrant_running
from src.utils.gen_embeddings import gen_column_embeddings
from src.utils.logger import ResourceMonitor
from src.utils.preprocessing import spider2preprocess
from src.utils.run_manager import resolve_run_id, get_run_path, load_run_metadata, save_run_metadata


if __name__ == "__main__":
    input_data_root = "Spider2\spider2-lite"
    run_name = "test"
    with ResourceMonitor() as monitor:
        preprocessing_results = spider2preprocess(
            input_data_root, is_multidialect=True, max_workers=8, # force_update=True
        )
        print(monitor.get_stats())

        # ensure_qdrant_running()
        gen_column_embeddings(
            input_data_root=input_data_root, embedding_model="microsoft/harrier-oss-v1-270m", 
            batch_size=180, device='cpu', max_workers=2
        )

        # Генерируем id запуска
        run_id = resolve_run_id(
            input_data_root=input_data_root,
            custom_suffix=run_name,
            use_latest=True
        )
        run_path = get_run_path(run_id)
        os.makedirs(run_path, exist_ok=True)

        retrieve_columns(run_name, input_data_root=input_data_root, max_workers=4)


    print(monitor.get_stats())
