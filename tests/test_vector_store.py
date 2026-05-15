import sys
sys.path.insert(0, '.')

import json
import os
from src.storage.vector_manager import VectorStoreManager
from src.utils.logger import ResourceMonitor


if __name__ == "__main__":
    with ResourceMonitor() as monitor:
        with VectorStoreManager(
            storage_root="storage",
            max_cached_sessions=2, 
            embedding_model="microsoft/harrier-oss-v1-270m",
            backend="qdrant",
            device="cpu",
            quantization=False,
            log_path="logs\dbs\Spider2\spider2-lite"
        ) as vsm:
            
            # 1. Группируем вопросы по db_id
            queries_by_db = {
                "bigquery_cms_data": [
                    # bq354
                    "Could you provide the percentage of participants for standard acne, atopic dermatitis, psoriasis, and vitiligo as defined by the International Classification of Diseases 10-CM (ICD-10-CM), including their subcategories? Please include all related concepts mapped to the standard ICD-10-CM codes (L70 for acne, L20 for atopic dermatitis, L40 for psoriasis, and L80 for vitiligo) by utilizing concept relationships, including descendant concepts. The percentage should be calculated based on the total number of participants, considering only the standard concepts and their related descendants.",
                    # bq235
                    "Can you tell me which healthcare provider incurs the highest combined average costs for both outpatient and inpatient services in 2014?",
                ],
                "snowflake_STACKOVERFLOW_PLUS": [
                    # sf_bq015
                    "Identify and rank the top 10 tags from Stack Overflow questions that were referenced in Hacker News comments on or after 2014 by counting how many times each question was mentioned, then splitting the questions\u2019 tag strings by the '|' delimiter, grouping by tag",
                ]
            }

            # 2. Построение индекса (один раз на датасет)
            # результаты предобработски spider2preprocess
            vsm.build_from_preprocessing_results(
                preprocessing_results={
                    db: os.path.join("storage\Spider2\spider2-lite\schema_cache", db + "_meta.json") 
                    for db in queries_by_db.keys()
                },
                context_id="Spider2\spider2-lite",
                max_workers=2,
                force_rebuild=True
            )
            print(monitor.get_stats())
            
            from qdrant_client.http.models import FieldCondition, Filter, MatchValue
            session = vsm._get_or_create_session("Spider2/spider2-lite")
            collection_name = session.collection_name
            for db_id in queries_by_db.keys():
                count = session.client.count(
                    collection_name=collection_name,
                    count_filter=Filter(must=[FieldCondition(key="db_id", match=MatchValue(value=db_id))])
                )
                print(f"📊 Collection '{collection_name}', db_id='{db_id}': {count.count} points indexed")
            
            # 3. Выполняем поиск
            search_results = vsm.search_batch(
                context_id="Spider2\spider2-lite",
                queries_by_db=queries_by_db,
                top_k=100,
                batch_size=32
            )
            print(monitor.get_stats())
            
            meta = {
                db: json.load(open(os.path.join("storage\Spider2\spider2-lite\schema_cache", db + '_meta.json'), encoding='utf-8'))
                for db in queries_by_db
            }

            print("\n----- From metadata: ", {db: sum(len(meta[db]['tables'][t]['columns']) for t in meta[db]['tables']) for db in meta}, '\n')

            # 4. Используем результаты в schema linking
            for db_id, db_results in search_results.items():
                for query, results in db_results.items():
                    print(f"Query: {query}")
                    for r in results[:10]:
                        print(f"  [{r.score:.3f}] {r.metadata['table_name']}.{r.metadata['column_name']}")
                        print(f"                  ({', '.join(meta[db_id]['tables'][r.metadata['table_name']]['similar_tables'])})")
