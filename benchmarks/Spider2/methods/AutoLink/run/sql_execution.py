import os
import sqlite3
import pandas as pd
from tqdm import tqdm
import os
from google.cloud import bigquery
import snowflake.connector
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from func_timeout import func_timeout, FunctionTimedOut
import glob
import threading
import numpy as np
import argparse

bigquery_credential_paths = glob.glob(os.path.join("bigquery_credentials", "**", "*.json"), recursive=True)
sqlite_lock = threading.Lock()
credential_usage_count = {}
credential_lock = threading.Lock()

def get_least_used_credential():
    global credential_usage_count, bigquery_credential_paths

    with credential_lock:
        for path in bigquery_credential_paths:
            if path not in credential_usage_count:
                credential_usage_count[path] = 0

        min_usage = min(credential_usage_count.values())
        least_used_credentials = [path for path, count in credential_usage_count.items() if count == min_usage]

        selected_credential = np.random.choice(least_used_credentials)

        credential_usage_count[selected_credential] += 1

    return selected_credential

def query_database_pandas(db_path, is_save, query, id, candidate_idx, log_path, task):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)

    if df.empty:
        with open(f"{log_path}/sql_gen/{task}_execution_error_{candidate_idx}/{id}.txt", "w", encoding="utf-8") as f:
            f.write("No data found for the specified query.")
    else:
        if is_save:
            df.to_csv(f"{log_path}/sql_gen/{task}_execution_result_{candidate_idx}/{id}.csv", index=False)
    conn.close()

def snowflake_query_data(sql_query, is_save, id, candidate_idx, log_path, task):
    try:
        snowflake_credential = json.load(open('snowflake_credential.json'))
        conn = snowflake.connector.connect(
            **snowflake_credential
        )
        cursor = conn.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=columns)
        if df.empty:
            with open(f"{log_path}/sql_gen/{task}_execution_error_{candidate_idx}/{id}.txt", "w", encoding="utf-8") as f:
                f.write("No data found for the specified query.")
        else:
            if is_save:
                df.to_csv(f"{log_path}/sql_gen/{task}_execution_result_{candidate_idx}/{id}.csv", index=False)
    except Exception as e:
        with open(f"{log_path}/sql_gen/{task}_execution_error_{candidate_idx}/{id}.txt", "w", encoding="utf-8") as f:
            f.write(str(e))

def bigquery_query_data(sql_query, is_save, id, candidate_idx, log_path, task):
    used_credential = []
    bigquery_credential_path = get_least_used_credential()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = bigquery_credential_path
    used_credential.append(bigquery_credential_path)
    try:
        client = bigquery.Client()
        query_job = client.query(sql_query)
        results = query_job.result().to_dataframe(create_bqstorage_client=False)
        if results.empty:
            with open(f"{log_path}/sql_gen/{task}_execution_error_{candidate_idx}/{id}.txt", "w", encoding="utf-8") as f:
                f.write("No data found for the specified query.")
        else:
            if is_save:
                results.to_csv(f"{log_path}/sql_gen/{task}_execution_result_{candidate_idx}/{id}.csv", index=False)
    except Exception as e:
        if "Quota" in str(e):
            print(f"Quota exceeded {id}")
            remaining_credentials = [cred for cred in bigquery_credential_paths if cred not in used_credential]
            for credential_path in remaining_credentials:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential_path
                used_credential.append(credential_path)

                with credential_lock:
                    if credential_path not in credential_usage_count:
                        credential_usage_count[credential_path] = 0
                    credential_usage_count[credential_path] += 1

                client = bigquery.Client()
                try:
                    query_job = client.query(sql_query)
                    results = query_job.result().to_dataframe()
                    if results.empty:
                        with open(f"{log_path}/sql_gen/{task}_execution_error_{candidate_idx}/{id}.txt", "w", encoding="utf-8") as f:
                            f.write("No data found for the specified query.")
                        break
                    else:
                        if is_save:
                            results.to_csv(f"{log_path}/sql_gen/{task}_execution_result_{candidate_idx}/{id}.csv", index=False)
                            break
                except Exception as e:
                    if "Quota" in str(e):
                        print(f"403 Quota exceeded again, trying next credential {id}")
                        with open(f"{log_path}/sql_gen/{task}_execution_error_{candidate_idx}/{id}.txt", "w", encoding="utf-8") as f:
                            f.write(str(e))
                        continue
                    else:
                        with open(f"{log_path}/sql_gen/{task}_execution_error_{candidate_idx}/{id}.txt", "w", encoding="utf-8") as f:
                            f.write(str(e))
                        break
        else:
            with open(f"{log_path}/sql_gen/{task}_execution_error_{candidate_idx}/{id}.txt", "w", encoding="utf-8") as f:
                f.write(str(e))

def execute(sql_query, id, db, candidate_idx, log_path, task):
    is_save = True
    result_file = f"{log_path}/sql_gen/{task}_execution_result_{candidate_idx}/{id}.csv"
    error_file = f"{log_path}/sql_gen/{task}_execution_error_{candidate_idx}/{id}.txt"

    if id.startswith('bq') or id.startswith('ga'):
        try:
            func_timeout(10 * 60, bigquery_query_data, 
                         args=(sql_query, is_save, id, candidate_idx, log_path, task))
        except FunctionTimedOut:
            with open(error_file, "w", encoding="utf-8") as f:
                f.write("Execute timeout: exceed 10min")
            print(f"Timeout for {id}")
        except Exception as e:
            with open(error_file, "w", encoding="utf-8") as f:
                f.write(f"Execution error: {str(e)}")
            print(f"Error executing {id}")

    elif id.startswith('sf'):
        snowflake_query_data(sql_query, is_save, id, candidate_idx, log_path, task)
        
    elif id.startswith('local'):
        local_path = f'..\..\..\spider2-lite\\resource/databases/spider2-localdb/{db}.sqlite'
        try:
            func_timeout(10 * 60, query_database_pandas,
                         args=(local_path, is_save, sql_query, id, candidate_idx, log_path, task))
        except FunctionTimedOut:
            with open(error_file, "w", encoding="utf-8") as f:
                f.write("Execute timeout: exceed 10min")
            print(f"Timeout for {id}")
        except Exception as e:
            with open(error_file, "w", encoding="utf-8") as f:
                f.write(str(e))

def execute_sql(candidate_idx, log_path, task, data_file, max_workers=4):
    folder_path = f"{log_path}/sql_gen/{task}_sql_{candidate_idx}"
    
    os.makedirs(f"{log_path}/sql_gen/{task}_execution_result_{candidate_idx}", exist_ok=True)
    os.makedirs(f"{log_path}/sql_gen/{task}_execution_error_{candidate_idx}", exist_ok=True)

    with open(data_file, 'r', encoding='utf-8') as f:
        q_dict = json.load(f)

    tasks = []
    filenames = sorted(item for item in os.listdir(folder_path) if item.endswith('.sql'))
    
    for filename in filenames:
        id = filename.split('.sql')[0]
        file_path = os.path.join(folder_path, filename)

        result_file = f"{log_path}/sql_gen/{task}_execution_result_{candidate_idx}/{id}.csv"
        error_file = f"{log_path}/sql_gen/{task}_execution_error_{candidate_idx}/{id}.txt"
        if os.path.exists(result_file) or os.path.exists(error_file):
            print(f"Skipping {id} (already processed)")
            continue
        
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_query = f.read()
        
        db = q_dict.get(id).get('db_name', None)
        if db:
            tasks.append((sql_query, id, db))

    print(f"Found {len(tasks)} unprocessed tasks for candidate {candidate_idx}")
    
    with ThreadPoolExecutor(max_workers) as executor:
        futures = [
            executor.submit(execute, sql, id, db, candidate_idx, log_path, task)
            for sql, id, db in tasks
        ]
        for _ in tqdm(as_completed(futures), total=len(futures), desc=f"Processing candidate {candidate_idx}"):
            pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_workers", type=int, default=4)
    parser.add_argument("--num_candidates", type=int, default=5)
    parser.add_argument("--data_file", type=str, required=True)
    parser.add_argument("--log_path", type=str, required=True)
    parser.add_argument("--task", type=str, default="r1_lite")
    args = parser.parse_args()
    for candidate_idx in range(args.num_candidates):
        print(f"Processing candidate {candidate_idx} execution")
        execute_sql(candidate_idx, args.log_path, args.task, args.data_file, args.num_workers)
