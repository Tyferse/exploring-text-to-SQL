import sys
sys.path.insert(0, '.')

import os
from src.storage.vector_manager import VectorStoreManager


if __name__ == "__main__":
    with VectorStoreManager(
        storage_root="storage",
        max_cached_sessions=2, 
        embedding_model="microsoft/harrier-oss-v1-270m",
        backend="qdrant",
        device="cpu",
        quantization=False,
        log_path="logs/dbs/Spider2/spider2-lite"
    ) as vsm:
        
        # 1. Построение индекса (один раз на датасет)
        # preprocessing_results от spider2preprocess
        vsm.build_from_preprocessing_results(
            preprocessing_results={
                tmp_db: os.path.join("storage\Spider2\spider2-lite\schema_cache", file) 
                for file in os.listdir("storage\Spider2\spider2-lite\schema_cache") 
                if file.endswith("_meta.json") and (tmp_db := file.rsplit('_', 1)[0]) in ["bigquery_cms_data", "sbowflake_STACKOVERFLOW_PLUS"]
            },
            context_id="Spider2/spider2-lite",
            max_workers=2,
            force_rebuild=True
        )
        
        # 2. Поиск: группируем вопросы по db_id
        queries_by_db = {
            "cms_data": [
                "Could you provide the percentage of participants for standard acne, atopic dermatitis, psoriasis, and vitiligo as defined by the International Classification of Diseases 10-CM (ICD-10-CM), including their subcategories? Please include all related concepts mapped to the standard ICD-10-CM codes (L70 for acne, L20 for atopic dermatitis, L40 for psoriasis, and L80 for vitiligo) by utilizing concept relationships, including descendant concepts. The percentage should be calculated based on the total number of participants, considering only the standard concepts and their related descendants.",
                "Can you tell me which healthcare provider incurs the highest combined average costs for both outpatient and inpatient services in 2014?",
            ],
            "STACKOVERFLOW_PLUS": [
                r"Identify and rank the top 10 tags from Stack Overflow questions that were referenced in Hacker News comments on or after 2014 by counting how many times each question was mentioned, then splitting the questions\u2019 tag strings by the '|' delimiter, grouping by tag",
            ]
        }
        
        # 3. Выполняем поиск
        search_results = vsm.search_batch(
            context_id="Spider2/spider2-lite",
            queries_by_db=queries_by_db,
            top_k=100,
            batch_size=32
        )
        
        # 4. Используем результаты в schema linking
        for db_id, db_results in search_results.items():
            for query, results in db_results.items():
                print(f"Query: {query}")
                for r in results[:3]:
                    print(f"  [{r.score:.3f}] {r.metadata['table_name']}.{r.metadata['column_name']}")
