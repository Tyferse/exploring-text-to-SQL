import sys
sys.path.insert(0, ".")

import os

from dotenv import load_dotenv

from src.modules.consistancy.voting import voting_selection
from src.modules.correction.semantic_correction import simple_semantic_correction
from src.modules.correction.syntax_correction import simple_correction
from src.modules.dbc_retrieval.exploratory_execution import exec_exploration
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
    tasks = "spider2-lite_wo_bq.jsonl"
    run_name = "baseline"
    with ResourceMonitor() as monitor:
        # preprocessing_results = spider2preprocess(
        #     input_data_root, is_multidialect=True, max_workers=16, force_update=True
        # )
        # print(monitor.get_stats())

        # ensure_qdrant_running(local_volume=os.path.join("storage", input_data_root, "column_vdb"))
        # gen_column_embeddings(
        #     input_data_root=input_data_root, # location="http://localhost:6333",
        #     embedding_model="microsoft/harrier-oss-v1-270m", 
        #     batch_size=512, device='cuda', max_workers=4, force_rebuild=False
        # )
        # print(monitor.get_stats())

        # Генерируем id запуска
        run_id = resolve_run_id(
            input_data_root=input_data_root,
            custom_suffix=run_name,
            use_latest=True
        )
        run_path = get_run_path(run_id)
        os.makedirs(run_path, exist_ok=True)

        # vsm = VectorStoreManager(
        #     # location="http://localhost:6333", 
        #     embedding_model="microsoft/harrier-oss-v1-270m", 
        #     max_cached_sessions=7, backend="qdrant", device="cuda", 
        #     log_path=os.path.join("logs", "dbs", input_data_root)
        # )
        # retrieve_columns(run_id, vsm, tasks=tasks, input_data_root=input_data_root, topk=100, max_workers=3, force_refresh=False)
        # print(monitor.get_stats())

        model = get_model("Qwen3.6-27B", "http://localhost:8000/v1", temperature=1.0)
        executor = SQLExecutor(input_data_root, local_dbs={"sqlite": "resource/databases/spider2-localdb"}, timeout=360)
        retry_config = {
            "max_attempts": 3,
            "initial_delay": 2.0,
            "max_delay": 30.0,
            "backoff_multiplier": 3.0,
        }
        # agent_pipeline = SchemaLinkingAgentPipeline(
        #     run_id, model, vsm, executor, tasks=tasks,
        #     input_data_root=input_data_root, 
        #     prompt_name="sl_explore_validation_agent", 
        #     max_turns=10, max_draft_calls=3, 
        #     additional_k=5, max_workers=30, 
        #     retry_config=retry_config,
        #     force_update=False
        # )
        # agent_pipeline.run()
        # agent_pipeline.extract_all_candidates()
        # vsm.close_all()
        # print(monitor.get_stats())

        generate_schemas(
            run_id, tasks=tasks, input_data_root=input_data_root, output_dir="final_schema", 
            docs_path=os.path.join("storage", input_data_root, "schema_cache"), 
            included="full", target_max_tokens=64000
        )

        # exec_exploration(
        #     run_id=run_id,
        #     model=get_model("Qwen3.6-27B", "http://localhost:8000/v1", temperature=0.6),
        #     executor=executor,
        #     tasks=tasks,
        #     max_workers=6,
        #     input_data_root=input_data_root,
        #     prompt_name="exploratory_execution",
        #     max_queries=10,
        #     max_rows=20,
        #     retry_config={
        #         "max_attempts": 3,
        #         "initial_delay": 4.0,
        #         "max_delay": 30.0,
        #         "backoff_multiplier": 3.0,
        #     }
        # )
        # print(monitor.get_stats())
        
        simple_generation(
            run_id, model, executor, tasks=tasks, input_data_root=input_data_root, 
            prompt_name="gen_basic", n_candidates=1, prefix="test_gen",
            max_workers=6, retry_config={
                "max_attempts": 4,
                "initial_delay": 2.0,
                "max_delay": 60.0,
                "backoff_multiplier": 4.0,
            }
        )
        print(monitor.get_stats())

        # simple_correction(
        #     run_id=run_id,
        #     model=model,
        #     executor=executor,
        #     tasks=tasks,
        #     prompt_names={
        #         "system": "syntax_system",
        #         "user": "syntax_user",
        #         "null": "syntax_null",
        #         "antiretry": "syntax_antiretry"
        #     },
        #     schema_dir="final_schema",
        #     gen_prefix="test_gen",
        #     max_messages=10,
        #     max_turns=5,
        #     max_workers=20,
        #     retry_config={
        #         "max_attempts": 2,
        #         "initial_delay": 10.0,
        #         "max_delay": 60.0,
        #         "backoff_multiplier": 4.0,
        #     }
        # )
        
        # simple_semantic_correction(
        #     run_id=run_id, model=model, executor=executor, 
        #     tasks=tasks,
        #     prompt_names={
        #         "classify": "semantic_classify",
        #         "validation": "semantic_validation",
        #         "rules": "semantic_rules_validation",
        #         "output": "semantic_output_validation"
        #     }, 
        #     schema_dir="final_schema", 
        #     gen_prefix="test_gen", validation_mode="double",
        #     max_turns=2, max_workers=20, retry_config=retry_config
        # )
        # print(monitor.get_stats())

        # voting_selection(
        #     tasks=tasks,
        #     run_id=run_id,
        #     model=get_model("Qwen3.6-27B", "http://localhost:8000/v1", temperature=0.05),
        #     prompt_name="pairwise_selection",
        #     gen_prefix="test_gen",
        #     input_data_root=input_data_root,
        #     selection_mode="execution",
        #     use_llm_tiebreaker=True,
        #     max_workers=20,
        #     retry_config={
        #         "max_attempts": 2,
        #         "initial_delay": 4.0,
        #         "max_delay": 30.0,
        #         "backoff_multiplier": 4.0,
        #     }
        # )
        # print(monitor.get_stats())
        