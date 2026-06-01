import sys
sys.path.insert(0, ".")

import os

from dotenv import load_dotenv

from src.modules.generation.simple_generation import simple_generation
from src.modules.schema_linking.agent import SchemaLinkingAgentPipeline
from src.modules.schema_linking.generate_schema import generate_schemas
from src.modules.schema_linking.retrieve_schema import retrieve_columns
from src.storage.docker_qdrant import ensure_qdrant_running
from src.storage.vector_manager import VectorStoreManager
from src.utils.gen_embeddings import gen_column_embeddings
from src.utils.logger import ResourceMonitor
from src.utils.models import get_model
from src.utils.preprocessing import spider2preprocess
from src.utils.run_manager import resolve_run_id, get_run_path, set_global_seeds
from src.utils.sql_execution import SQLExecutor


if __name__ == "__main__":
    load_dotenv(".env")
    set_global_seeds()

    input_data_root = "Spider2/spider2-lite"
    run_name = "test"
    with ResourceMonitor() as monitor:
        preprocessing_results = spider2preprocess(
            input_data_root, is_multidialect=True, max_workers=8, # force_update=True
        )
        print(monitor.get_stats())

        ensure_qdrant_running(local_volume=os.path.join("storage", input_data_root, "column_vdb"))
        gen_column_embeddings(
            input_data_root=input_data_root, location="http://localhost:6333",
            embedding_model="microsoft/harrier-oss-v1-270m", 
            batch_size=256, device='cuda', max_workers=2
        )
        print(monitor.get_stats())

        # Генерируем id запуска
        run_id = resolve_run_id(
            input_data_root=input_data_root,
            custom_suffix=run_name,
            use_latest=True
        )
        run_path = get_run_path(run_id)
        os.makedirs(run_path, exist_ok=True)

        vsm = VectorStoreManager(
            location="http://localhost:6333", 
            embedding_model="microsoft/harrier-oss-v1-270m", 
            max_cached_sessions=2, backend="qdrant", device="cpu", 
            log_path=os.path.join("logs", "dbs", input_data_root)
        )
        retrieve_columns(run_id, vsm, input_data_root=input_data_root, topk=100, max_workers=4)
        print(monitor.get_stats())

        model = get_model("Qwen3.7-9B", "https://localhost:5433", temperature=1.0)
        executor = SQLExecutor(input_data_root, local_dbs={"sqlite": "resource/databases/spider2-localdb"})
        retry_config = {
            "max_attempts": 4,
            "initial_delay": 1.0,
            "max_delay": 30.0,
            "backoff_multiplier": 2.5,
        }
        agent_pipeline = SchemaLinkingAgentPipeline(
            run_id, model, vsm, executor, 
            input_data_root=input_data_root, 
            prompt_name="sl_explore_validation_agent", 
            max_turns=10, max_draft_calls=3, 
            additional_k=5, max_workers=2, 
            retry_config=retry_config
        )
        agent_pipeline.run()
        print(monitor.get_stats())

        generate_schemas(
            run_id, input_data_root=input_data_root, output_dir="final_schema", 
            docs_path=os.path.join("storage", input_data_root, "schema_cache"), 
            included="retrieval", target_max_tokens=80000
        )

        simple_generation(
            run_id, model, executor, input_data_root=input_data_root, 
            prompt_name="gen_basic", n_candidates=5, 
            max_workers=3, retry_config=retry_config
        )
        print(monitor.get_stats())
