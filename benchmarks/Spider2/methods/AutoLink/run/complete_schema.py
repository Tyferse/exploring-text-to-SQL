from datetime import datetime
import os
import json
import multiprocessing as mp
import time
from retrieve_topk_schema import get_next_k_results
from utils import *
import transformers
from tqdm import tqdm
from config import *
from openai import OpenAI, RateLimitError
import glob
import numpy as np
import threading
import sqlite3
from google.cloud import bigquery
import snowflake.connector
import pandas as pd
import shutil
import argparse

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"), base_url=os.environ.get("OPENAI_BASE_URL"))
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

def backup_instance_state(instance_id: str, log_path: str):
    cache_dir = os.path.join(log_path, "cache")
    status_dir = os.path.join(log_path, "status")
    backup_dir = os.path.join(log_path, "backup")
    
    os.makedirs(backup_dir, exist_ok=True)
    
    cache_file = os.path.join(cache_dir, f"{instance_id}.json")
    status_file = os.path.join(status_dir, f"{instance_id}.json")
    
    backup_cache_file = os.path.join(backup_dir, f"{instance_id}_cache.json")
    backup_status_file = os.path.join(backup_dir, f"{instance_id}_status.json")
    
    if not os.path.exists(backup_cache_file) and not os.path.exists(backup_status_file):
        shutil.copy2(cache_file, backup_cache_file)
        shutil.copy2(status_file, backup_status_file)

def restore_instance_state(instance_id: str, log_path: str):
    cache_dir = os.path.join(log_path, "cache")
    status_dir = os.path.join(log_path, "status")
    backup_dir = os.path.join(log_path, "backup")
    
    backup_cache_file = os.path.join(backup_dir, f"{instance_id}_cache.json")
    backup_status_file = os.path.join(backup_dir, f"{instance_id}_status.json")
    
    cache_file = os.path.join(cache_dir, f"{instance_id}.json")
    status_file = os.path.join(status_dir, f"{instance_id}.json")
    
    if os.path.exists(backup_cache_file):
        shutil.copy2(backup_cache_file, cache_file)
    if os.path.exists(backup_status_file):
        shutil.copy2(backup_status_file, status_file)           

def thread_safe_sql_execution(instance_id, sql, db_name):
    if instance_id.startswith("local"):
        with sqlite_lock:
            return sql_execution(instance_id, sql, db_name)
    else:
        return sql_execution(instance_id, sql, db_name)

def sql_execution(instance_id, sql, db_name):
    if instance_id.startswith("bq") or instance_id.startswith("ga"):
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
                print("403 Quota exceeded")
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
                            print("403 Quota exceeded again, trying next credential")
                            continue
                        else:
                            return "error", f"Error occurred while fetching data: {e}"
            return "error", f"Error occurred while fetching data: {e}"
    elif instance_id.startswith("sf"):
        snowflake_credential = json.load(open("..\..\..\spider2-lite\evaluation_suite\\snowflake_credential.json"))
        conn = snowflake.connector.connect(**snowflake_credential)
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(results, columns=columns)
            if df.empty:
                return "empty", "No data found for the specified query."
            else:
                return "success", df
        except Exception as e:
            return "error", f"Error occurred while fetching data: {e}"
        finally:
            cursor.close()
            conn.close()
    elif instance_id.startswith("local"):
        db_path = f"..\..\..\spider2-lite\evaluation_suite\\resource/databases/spider2-localdb/{db_name}.sqlite"
        conn = sqlite3.connect(db_path)
        try:
            df = pd.read_sql_query(sql, conn)
            if df.empty:
                return "empty", "No data found for the specified query."
            else:
                return "success", df
        except Exception as e:
            return "error", f"Error occurred while fetching data: {e}"
        finally:
            conn.close()

def remove_column_values(schema_text):
    lines = schema_text.split('\n')
    processed_lines = []
    
    for line in lines:
        if line.strip().startswith("Column name:"):
            import re
            pattern = r'(Column name:.*?Column type:.*?); Column value: \[.*?\]; (Description:.*)'
            match = re.match(pattern, line.strip())
            
            if match:
                processed_line = f"{match.group(1)}; {match.group(2)}"
                processed_lines.append(processed_line)
            else:
                if "; Column value:" in line and "; Description:" in line:
                    value_start = line.find("; Column value:")
                    desc_start = line.find("; Description:")
                    if value_start != -1 and desc_start != -1 and value_start < desc_start:
                        processed_line = line[:value_start] + line[desc_start:]
                        processed_lines.append(processed_line)
                    else:
                        processed_lines.append(line)
                else:
                    processed_lines.append(line)
        else:
            processed_lines.append(line)
            
    return '\n'.join(processed_lines)

def process_instance_batch(batch_instances, log_path):
    cache_path = os.path.join(log_path, "cache")
    status_path = os.path.join(log_path, "status")
    schema_path = os.path.join(log_path, "schema_prompts")
    model_output_path = os.path.join(log_path, "model_output")
    tool_calls_path = os.path.join(log_path, "tool_calls")
    input_path = os.path.join(log_path, "input")
    candidates_path = os.path.join(log_path, "candidates")
    error_path = os.path.join(log_path, "error")

    for instance_id, info in tqdm(batch_instances.items(), leave=False, desc=f"Thread {os.getpid()}"):
        
        restore_instance_state(instance_id, log_path)
        
        each_candidates = {}

        embed_path = determine_embedding_path(instance_id)

        if instance_id.startswith("bq") or instance_id.startswith("ga"):
            documents_path = "documents/bigquery.json"
            sql_type = BIGQUERY
            sql_optimization = BIGQUERY_DIALECT_OPTIMIZATION
        elif instance_id.startswith("sf"):
            documents_path = "documents/snowflake.json"
            sql_type = SNOWFLAKE
            sql_optimization = SNOWFLAKE_DIALECT_OPTIMIZATION
        elif instance_id.startswith("local"):
            documents_path = "documents/localdb.json"
            sql_type = SQLITE
            sql_optimization = SQLITE_DIALECT_OPTIMIZATION

        with open(documents_path, "r", encoding="utf-8") as f:
            documents = json.load(f)

        with open("spider2_data.json", "r", encoding="utf-8") as f:
            spider2_data = json.load(f)
        
        if instance_id not in spider2_data:
            raise ValueError(f"Instance ID {instance_id} not found in spider2_data.json")
        
        konwledge_name = spider2_data[instance_id].get("external_knowledge", None)
        knowledge_data = ""
        
        if konwledge_name:
            knowledge_path = os.path.join("..\..\..\spider2-lite\\resource\documents", konwledge_name)
            try:
                with open(knowledge_path, "r", encoding="utf-8") as f:
                    knowledge_data = f.read()
            except Exception as e:
                print(instance_id, e)

        question = info["question"]
        db_name = info["db_name"]

        db_documents = documents[db_name]

        with open(f"{schema_path}/{instance_id}.txt", "r", encoding="utf-8") as f:
            retrieved_schemas = f.read()
        
        all_tables = list(db_documents.keys())
        all_calls = {}
        all_model_output = ""
        all_inputs = ""

        system_prompt = SCHEMA_LINKING.format(
            SQL_TYPE=sql_type,
            SQL_OPTIMIZATION=sql_optimization,
        )
        
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": USER_INPUT.format(
                RETRIEVED_SCHEMA=json.dumps(retrieved_schemas, ensure_ascii=False),
                USER_QUESTION=question,
                EXTERNAL_KNOWLEDGE=knowledge_data,
                ALL_TABLES=json.dumps(all_tables, ensure_ascii=False),
            )}
        ]

        is_finished = False
        is_error = False

        column_candidates = []
        table_candidates = []
        column_type_candidates = []
        column_value_candidates = []
        description_candidates = []

        for i in range(10):
            
            all_inputs += f"{datetime.now().isoformat(sep=' ')} | Turn {i}\n" + str(messages) + "\n" + "=" * 50 + "\n\n"

            if is_finished or is_error:
                break

            try:
                response = client.chat.completions.create(
                    # model="deepseek-chat",
                    model="stepfun/step-3.5-flash:free",
                    messages=messages
                )
            except RateLimitError:
                time.sleep(5)
                continue

            model_output = response.choices[0].message.content

            all_model_output += (f"{datetime.now().isoformat(sep=' ')} | Turn {i}\n" +
                                 # "=============MODEL REASONING=============" +
                                 # model_reason + "\n" +
                                 # "=============MODEL OUTPUT=============" +
                                 model_output + "\n" + "=" * 50 + "\n\n")

            try:
                full_lines, tool_calls = parse_model_output(model_output)
            except Exception as e:
                full_lines = []
                tool_calls = []
                is_error = True

                with open(os.path.join(error_path, instance_id) + '.txt', "w", encoding="utf-8") as f:
                    f.write(model_output)


            func_messages = ""
            
            for line, func in zip(full_lines, tool_calls):
                
                if func["tool"] == "stop":
                    is_finished = True
                    break
                elif func["tool"] == "schema_retrieval":
                    
                    is_complete = False
                    
                    table = func["table"]
                    column = func["column"]
                    description = func["description"]

                    if table == "" and column == "" and description == "":
                        continue
                    
                    if is_complete:
                        continue

                    func_messages += f"Tool: {line} \n The tool returns the following results:"

                    retrieve_content = "column name: " + column + "\n" + \
                                       "table name: " + table + "\n" + \
                                       "description: " + description

                    semantic_results, metadata_mapping, text = get_next_k_results(
                        instance_id=instance_id,
                        question=retrieve_content,
                        db_name=db_name,
                        embed_path=embed_path,
                        top_k=3,
                        cache_dir=cache_path,
                        status_dir=status_path,
                        device="cuda:0")

                    new_results = ""
                    for result in semantic_results:
                        metadata = result["metadata"]
                        table = metadata["table"]
                        column = metadata["column"]
                        description = metadata["description"]
                        column_type = metadata["column_type"]
                        column_value = metadata["column_value"]
                        func_messages += f"{description}\n"
                        new_results += f"{description}\n"
                        column_candidates.append(column)
                        table_candidates.append(table)
                        column_type_candidates.append(column_type)
                        column_value_candidates.append(column_value)
                        description_candidates.append(description)

                    if text:
                        is_complete = True
                        func_messages += f"{text}\n"
                    func["result"] = new_results
                    func_messages += "\n"

                elif func["tool"] == "sql_execution" or func["tool"] == "sql_draft":

                    query = func["query"]

                    if query == "":
                        continue
                    func_messages += f"Tool: {line} \n The tool returns the following results:\n"
                    exec_status, results = thread_safe_sql_execution(instance_id, query, db_name)         
                    func_messages += f"{results}\n\n"
                    func["result"] = str(results)
                    
            all_calls[f"turn_{i}"] = tool_calls
            func_messages += "\nFor `@sql_execution`, if the results return column names or table names including the missing tables or columns you think, in this turn, you must use the @schema_retrieval tool to retrieve the missing tables or columns.\nBecause we will use initial schema and the results of `@sql_execution` tool as the final schema. Please do not think that the columns obtained by @sql_execution will be recalled. Only the columns obtained by `@schema_retrieval` can be considered to be recalled correctly.\nPlease also pay attention to the column name like `*id`, `*name`, `*text`, `*code` and so on. These columns are often crucial for final SQL construction, especially for joins, filtering, and output.\nYou also need to pay attention that Some important columns may exist in more than one table, but the initial schema may include only one instance. This can cause critical tables to be omitted if you're not careful. Always check whether a column name is shared across tables, and whether the other tables containing it also provide relevant context for the question."

            messages.append({"role": "assistant", "content": model_output})
            messages.append({"role": "user", "content": func_messages})

            time.sleep(4)

        each_candidates[instance_id] = {
            "question": question,
            "db_name": db_name,
            "column_candidates": column_candidates,
            "column_types": column_type_candidates,
            "column_values": column_value_candidates,
            "table_candidates": table_candidates,
            "descriptions": description_candidates,
        }

        if is_error:
            print(f"Error occurred for instance {instance_id}. Skipping...")
            continue

        with open(os.path.join(model_output_path, instance_id) + '.txt', "w", encoding="utf-8") as f:
            f.write(all_model_output)
            
        with open(os.path.join(tool_calls_path, instance_id) + '.json', "w", encoding="utf-8") as f:
            json.dump(all_calls, f, ensure_ascii=False, indent=2)

        with open(os.path.join(input_path, instance_id) + '.txt', "w", encoding="utf-8") as f:
            f.write(all_inputs)

        with open(os.path.join(candidates_path, instance_id) + '.json', "w", encoding="utf-8") as f:
            json.dump(each_candidates, f, ensure_ascii=False, indent=2)


def complete_schema(log_path, num_threads=3):
    
    status_dir = os.path.join(log_path, "status")
    
    """Complete schema using multithreading approach"""
    model_output_path = os.path.join(log_path, "model_output")
    os.makedirs(model_output_path, exist_ok=True)

    tool_calls_path = os.path.join(log_path, "tool_calls")
    os.makedirs(tool_calls_path, exist_ok=True)

    input_path = os.path.join(log_path, "input")
    os.makedirs(input_path, exist_ok=True)

    candidates_path = os.path.join(log_path, "candidates")
    os.makedirs(candidates_path, exist_ok=True)

    error_path = os.path.join(log_path, "error")
    os.makedirs(error_path, exist_ok=True)

    
    with open(os.path.join(log_path, "initial_candidates.json"), "r", encoding="utf-8") as f:
        initial_candidates = json.load(f)
    with open("spider2_data.json", "r", encoding="utf-8") as f:
        spider2_data = json.load(f)
    
    instance_ids = list(spider2_data.keys())
    
    print("Backup instance status ...")
    for instance_id in instance_ids:
        backup_instance_state(instance_id, log_path)
    
    clean_instance_ids = []
    for instance_id in instance_ids:
        if os.path.exists(os.path.join(candidates_path, instance_id) + '.json'):
            continue
        with open(os.path.join(status_dir, instance_id + ".json"), "r", encoding="utf-8") as f:
            cache_data = json.load(f)
        
        if cache_data.get("is_complete", False):
            continue
        
        clean_instance_ids.append(instance_id)
    
    print(f"Unfinished instances: {len(clean_instance_ids)}")
    
    with open("uncompleted_instances.txt", "w", encoding="utf-8") as f:
        for instance_id in clean_instance_ids:
            f.write(instance_id + "\n")
    
    instance_ids = clean_instance_ids

    batch_size = max(1, len(instance_ids) // num_threads)
    batches = []

    for i in range(0, len(instance_ids), batch_size):
        end_idx = min(i + batch_size, len(instance_ids))
        batch = {instance_id: initial_candidates[instance_id] for instance_id in instance_ids[i:end_idx]}
        batches.append(batch)

    num_threads = min(num_threads, len(batches))

    mp.set_start_method('spawn', force=True)

    # Start processes
    processes = []
    target_function = process_instance_batch
    for i in range(num_threads):
        if i < len(batches):
            p = mp.Process(
                target=target_function,
                args=(batches[i], log_path)
            )
            processes.append(p)
            p.start()

    # Wait for all processes to finish
    for p in processes:
        p.join()
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--log_path', type=str, default="log_v3_topn100")
    args = parser.parse_args()
    print("Starting schema completion...")
    complete_schema(args.log_path, num_threads=2)
    print("Schema completion finished.")
