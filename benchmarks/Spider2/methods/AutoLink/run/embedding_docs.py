from sentence_transformers import SentenceTransformer
import os
import json
import numpy as np
import faiss
from tqdm import tqdm
import time
from datetime import datetime


BIGQUERY_PATH = "..\..\..\spider2-lite\\resource/databases/bigquery"
SNOWFLAKE_PATH = "..\..\..\spider2-lite\\resource/databases/snowflake"
LOCALDB_PATH = "..\..\..\spider2-lite\\resource/databases/sqlite"

DBS_PATH = [BIGQUERY_PATH, SNOWFLAKE_PATH, LOCALDB_PATH]

def embed_documents(input_file: str, embed_path: str, batch_size: int = 32):
    os.makedirs(embed_path, exist_ok=True)

    model = SentenceTransformer("BAAI/bge-large-en-v1.5")

    with open(input_file, "r", encoding="utf-8") as f:
        documents = json.load(f)

    log = {}

    log_file = open("pre_logs/emb_docs.log", 'a', encoding='utf-8')

    for db_name, tables in tqdm(documents.items()):

        db_dir = os.path.join(embed_path, db_name)
        if os.path.exists(os.path.join(db_dir, "index.faiss")):
            continue

        log_file.write(datetime.now().isoformat(sep=" ") + " | begin | " + embed_path.split('/')[1] + " | " +  db_name + "\n")
        
        os.makedirs(db_dir, exist_ok=True)

        all_descriptions = []
        metadata_mapping = []

        for table_name, table_info in tables.items():
            columns = table_info["columns"]
            column_types = table_info["column_types"]
            column_values = table_info["sample_values"]
            
            if len(columns) != len(column_types) or len(columns) != len(column_values) or len(column_types) != len(column_values):
                print(f"Warning: Length mismatch in table {table_name} of database {db_name}.")
                print(f"Columns: {len(columns)}, Column Types: {len(column_types)}, Column Values: {len(column_values)}")
            
            for (column_name, desc), column_type, column_value in zip(columns.items(), column_types, column_values):
                all_descriptions.append(desc)
                metadata_mapping.append({
                    "table": table_name,
                    "column": column_name,
                    "column_type": column_type,
                    "column_value": column_value,
                    "description": desc
                })

        log[db_name] = len(metadata_mapping)

        db_embeddings = []
        for i in tqdm(range(0, len(all_descriptions), batch_size), desc=f"Embedding {db_name}", leave=False):
            batch_descriptions = all_descriptions[i:i + batch_size]
            batch_embeddings = model.encode(batch_descriptions, convert_to_numpy=True)
            db_embeddings.extend(batch_embeddings)

        assert len(db_embeddings) == len(metadata_mapping)

        dimension = len(db_embeddings[0])
        index = faiss.IndexFlatL2(dimension)
        index.add(np.array(db_embeddings, dtype=np.float32))

        faiss.write_index(index, os.path.join(db_dir, "index.faiss"))

        with open(os.path.join(db_dir, "metadata.json"), "w", encoding="utf-8") as f_meta:
            json.dump(metadata_mapping, f_meta, ensure_ascii=False, indent=2)
        
        log_file.write(datetime.now().isoformat(sep=" ") + " | end | " + embed_path.split('/')[1] + " | " +  db_name + "\n")
    
    log_file.close()

if __name__ == "__main__":
    if not os.path.exists("run/pre_logs/emb_docs.log"):
        with open("pre_logs/emb_docs.log", 'w', encoding='utf-8'):
            pass

    for db in DBS_PATH:
        if "bigquery" in db:
            print("Embedding BigQuery documents...")
            embed_documents(os.path.join("documents", "bigquery.json"), "embeddings/bigquery", batch_size=1024)
        if "snowflake" in db:
            print("Embedding Snowflake documents...")
            embed_documents(os.path.join("documents", "snowflake.json"), "embeddings/snowflake", batch_size=1024)
        if "sqlite" in db:
            print("Embedding SQLite documents...")
            embed_documents(os.path.join("documents", "localdb.json"), "embeddings/localdb", batch_size=1024)
