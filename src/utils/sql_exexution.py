import glob
import os
import sqlite3
import threading

import pandas as pd
import snowflake.connector
from google.cloud import bigquery


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
        db_path = f"..\..\..\spider2-lite\\resource\databases\spider2-localdb\{db_name}.sqlite"
        conn = sqlite3.connect(db_path)
        try:
            df = pd.read_sql_query(sql, conn)
            if df.empty:
                return "empty", "No data found for the specified query."
            else:
                return "success", df
        except Exception as e:
            print(db_name, e)
            return "error", f"Error occurred while fetching data for: {e}"
        finally:
            conn.close()
