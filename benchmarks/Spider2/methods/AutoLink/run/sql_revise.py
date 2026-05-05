import os
import json
import time
import threading
import argparse
import sqlite3
import pandas as pd
import numpy as np
import glob
from typing import Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from func_timeout import func_timeout, FunctionTimedOut
from openai import OpenAI
from google.cloud import bigquery
import snowflake.connector

from config import *

bigquery_credential_paths = glob.glob(os.path.join("bigquery_credentials", "**", "*.json"), recursive=True)
sqlite_lock = threading.Lock()
credential_usage_count = {}
credential_lock = threading.Lock()

class Config:
    INITIAL_RETRY_DELAY = 1
    MAX_RETRY_DELAY = 30
    REVISE_NUM_ATTEMPTS = 5


class DataLoader:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance.data = None
        return cls._instance

    def load(self, path: str) -> Dict:
        if self.data is None:
            with open(path, "r", encoding="utf-8") as f:
                self.data = json.load(f)
        return self.data

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

def query_sqlite(db_name, sql):
    conn = sqlite3.connect(f"..\..\..\spider2-lite\\resource/databases/spider2-localdb/{db_name}.sqlite")
    try:
        df = pd.read_sql_query(sql, conn)
        if df.empty:
            conn.close()
            return "empty", "No data found for the specified query."
        else:
            conn.close()
            return "success", df
    except Exception as e:
        conn.close()
        return "error", f"Error occurred while fetching data: {e}"


def query_snowflake(sql):
    cred = json.load(open("snowflake_credential/snowflake_credential.json"))
    conn = snowflake.connector.connect(**cred)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=columns)
        if df.empty:
            cursor.close()
            conn.close()
            return "empty", "No data found for the specified query."
        else:
            cursor.close()
            conn.close()
            return "success", df
    except Exception as e:
        cursor.close()
        conn.close()
        return "error", f"Error occurred while fetching data: {e}"


def query_bigquery(sql, instance_id):
    used_credential = []
    bigquery_credential_path = get_least_used_credential()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = bigquery_credential_path
    used_credential.append(bigquery_credential_path)
    client = bigquery.Client()
    try:
        query_job = client.query(sql)
        results = query_job.result().to_dataframe()
        if results.empty:
            return "empty", "No data found for the specified query."
        else:
            return "success", results
    except Exception as e:
        if "403 Quota exceeded" in str(e):
            print(f"403 Quota exceeded {instance_id}")
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
                    query_job = client.query(sql)
                    results = query_job.result().to_dataframe()
                    if results.empty:
                        return "empty", "No data found for the specified query."
                    else:
                        return "success", results
                except Exception as e:
                    if "403 Quota exceeded" in str(e):
                        print(f"403 Quota exceeded again, trying next credential {instance_id}")
                        continue
                    else:
                        return "error", f"Error occurred while fetching data: {e}"
        return "error", f"Error occurred while fetching data: {e}"


def execute_sql(instance_id, sql, db_name):
    if instance_id.startswith("bq") or instance_id.startswith("ga"):
        try:
            state, result = func_timeout(10 * 60, query_bigquery,
                         args=(sql, instance_id))
        except FunctionTimedOut:
            print(f"Execute timeout: exceed 10min  {instance_id}")
            state = "error"
            result = "Execute timeout: exceed 10min"

    elif instance_id.startswith("sf"):
        state, result = query_snowflake(sql)

    elif instance_id.startswith("local"):
        try:
            state, result = func_timeout(10 * 60, query_sqlite,
                         args=(db_name, sql))
        except FunctionTimedOut:
            print(f"Execute timeout: exceed 10min  {instance_id}")
            state = "error"
            result = "Execute timeout: exceed 10min"
    return state, result

def thread_safe_sql_execution(instance_id, sql, db_name):
    if instance_id.startswith("local"):
        with sqlite_lock:
            return execute_sql(instance_id, sql, db_name)
    else:
        return execute_sql(instance_id, sql, db_name)


class SQLReviser:
    def __init__(self):
        self.client = OpenAI(
            api_key=os.environ.get("OPENAI_API_KEY"),
            base_url=os.environ.get("OPENAI_BASE_URL"),
        )
        self.loader = DataLoader()

    def revise(self, instance_id, info, schema, sql, execution):
        question = info["question"]
        db_name = info["db_name"]

        if instance_id.startswith("bq"):
            dialect = "BigQuery"
            sql_type = BIGQUERY_DIALECT_OPTIMIZATION_SQL_GEN
        elif instance_id.startswith("sf"):
            dialect = "SnowFlake"
            sql_type = SNOWFLAKE_DIALECT_OPTIMIZATION_SQL_GEN
        else:
            dialect = "SQLite"
            sql_type = SQLITE_DIALECT_OPTIMIZATION_SQL_GEN

        prompt = (
            REVISE_ERROR
            .replace("{PROMPT}", schema[:200000])
            .replace("{QUESTION}", question)
            .replace("{SQL}", sql)
            .replace("{ERROR_MESSAGE}", execution)
            .replace("{SQL_DIALECT_OPTIMIZATION}", sql_type)
            .replace("{SQL_TYPE}", dialect)
        )

        messages = [{"role": "user", "content": prompt}]

        for attempt in range(Config.REVISE_NUM_ATTEMPTS):
            while True:
                try:
                    response = self.client.chat.completions.create(
                        model="tencent/hy3-preview:free",
                        messages=messages,
                    )
                    reasoning_content = response.choices[0].message.reasoning_content
                    model_output = response.choices[0].message.content
                    if model_output:
                        break
                except Exception as e:
                    print(f"empty output: {e}  ID: {instance_id}")
            
            messages.append({"role": "assistant", "content": model_output})
            sql_end = model_output.rfind("```")
            sql_start = model_output.rfind("```sql", 0, sql_end)
            if sql_start != -1 and sql_end != -1 and sql_end > sql_start:
                sql = model_output[sql_start + 6:sql_end].strip()
            else:
                print(f"parse sql error: {instance_id}")
                sql = model_output.strip()
            exec_status, exec_result = thread_safe_sql_execution(instance_id, sql, db_name)
            if exec_status == "success":
                return sql, exec_result, exec_status, messages
            else:
                if "Quota" in exec_result:
                    print(f"Quota exceeded for {instance_id}, skipping...")
                    break
                messages.append({"role": "user", "content": "Execution error:\n" + exec_result + "\nPlease revise the SQL again."})
                continue
        return sql, exec_result, exec_status, messages


def collect_tasks(schema_dir, log_path, task, num_candidates):
    instance_ids = [f[:-4] for f in os.listdir(schema_dir) if f.endswith(".txt")]
    tasks = []
    for cid in range(num_candidates):
        for iid in instance_ids:
            if os.path.exists(f"{log_path}/sql_gen/{task}_execution_error_{cid}/{iid}.txt"):
                out = f"{log_path}/sql_revise/{task}_sql_{cid}/{iid}.sql"
                if not os.path.exists(out):
                    tasks.append((iid, cid))
    return tasks


def run_task(reviser, data, schema_dir, log_path, task, instance_id, cid):
    with open(f"{schema_dir}/{instance_id}.txt", encoding="utf-8") as f:
        schema = f.read()

    sql_path = f"{log_path}/sql_gen/{task}_sql_{cid}/{instance_id}.sql"
    err_path = f"{log_path}/sql_gen/{task}_execution_error_{cid}/{instance_id}.txt"

    if not os.path.exists(sql_path) or not os.path.exists(err_path):
        print(f"[SKIP] missing sql or err: {instance_id} cid={cid}")
        return

    sql = open(sql_path).read()
    execution = open(err_path).read()

    new_sql, df, exec_status, messages = reviser.revise(instance_id, data[instance_id], schema, sql, execution)
    out_dir = f"{log_path}/sql_revise/{task}_sql_{cid}"
    out_dir_me = f"{log_path}/sql_revise/{task}_messages_{cid}"
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(out_dir_me, exist_ok=True)
    
    with open(f"{out_dir_me}/{instance_id}.json", "w", encoding="utf-8") as f:
        json.dump(messages, f, ensure_ascii=False, indent=2)
    
    open(f"{out_dir}/{instance_id}.sql", "w").write(new_sql)
    if exec_status == "error" or exec_status == "empty":
        with open(f"{out_dir}/{instance_id}.txt", "w", encoding="utf-8") as f:
            f.write(df)
    elif exec_status == "success":
        df.to_csv(f"{out_dir}/{instance_id}.csv", index=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_file", required=True)
    parser.add_argument("--schema_dir", required=True)
    parser.add_argument("--log_path", required=True)
    parser.add_argument("--task", default="r1_lite")
    parser.add_argument("--num_workers", type=int, default=8)
    parser.add_argument("--num_candidates", type=int, default=5)
    args = parser.parse_args()

    os.makedirs(args.log_path, exist_ok=True)

    data = DataLoader().load(args.data_file)
    reviser = SQLReviser()

    tasks = collect_tasks(args.schema_dir, args.log_path, args.task, args.num_candidates)
    print(f"Total pending tasks: {len(tasks)}")

    with ThreadPoolExecutor(max_workers=args.num_workers) as executor:
        futures = [
            executor.submit(
                run_task,
                reviser,
                data,
                args.schema_dir,
                args.log_path,
                args.task,
                iid,
                cid,
            )
            for iid, cid in tasks
        ]

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="SQL Revision",
            dynamic_ncols=True,
        ):
            try:
                future.result()
            except Exception as e:
                print("[THREAD ERROR]", e)

    print("SQL revision finished")


if __name__ == "__main__":
    main()
