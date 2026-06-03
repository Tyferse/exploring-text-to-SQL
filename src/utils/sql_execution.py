import argparse
import glob
import json
import os
import sqlite3
import threading
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
import snowflake.connector
from google.cloud import bigquery


class SQLExecutor:
    def __init__(self, input_data_root: str = "Spider2/spider2-lite", data_root: str = "data", storage_root: str = "storage", local_dbs: Optional[Dict[str, str]] = None):
        self.data_root = data_root
        self.storage_root = storage_root
        self.input_data_root = input_data_root
        self.local_dbs = local_dbs if local_dbs is not None else {"sqlite": "resource/databases/spider2-localdb"}
        self.bigquery_credential_paths = glob.glob(os.path.join(storage_root, input_data_root, "bigquery_credential", "**", "*.json"), recursive=True)
        self.sqlite_lock = threading.Lock()
        self.credential_usage_count = {}
        self.credential_lock = threading.Lock()

    def get_least_used_credential(self):
        with self.credential_lock:
            for path in self.bigquery_credential_paths:
                if path not in self.credential_usage_count:
                    self.credential_usage_count[path] = 0
            
            min_usage = min(self.credential_usage_count.values())
            least_used_credentials = [path for path, count in self.credential_usage_count.items() if count == min_usage]
            
            selected_credential = np.random.choice(least_used_credentials)
            
            self.credential_usage_count[selected_credential] += 1
            
        return selected_credential

    def thread_safe_sql_execution(self, sql, db_name, dialect="sqlite"):
        if dialect == "sqlite":
            with self.sqlite_lock:
                return self.sql_execution(sql, db_name, dialect)
        else:
            return self.sql_execution(sql, db_name, dialect)

    def sql_execution(self, sql, db_name, dialect="sqlite") -> Tuple[str, pd.DataFrame]:
        if dialect == "bigquery":
            used_credential = []
            bigquery_credential_path = self.get_least_used_credential()
            used_credential.append(bigquery_credential_path)
            # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = bigquery_credential_path
            # client = bigquery.Client()
            client = bigquery.Client.from_service_account_json(bigquery_credential_path)
            try:
                query_job = client.query(sql)
                results = query_job.result().to_dataframe()
                if results.empty:
                    return "empty", "No data found for the specified query."
                else:
                    return "success", results
            except Exception as e:
                if "403 Quota exceeded" in str(e):
                    client.close()
                    print("403 Quota exceeded")
                    remaining_credentials = [cred for cred in self.bigquery_credential_paths if cred not in used_credential]
                    for credential_path in remaining_credentials:
                        # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential_path
                        used_credential.append(credential_path)
                        
                        with self.credential_lock:
                            if credential_path not in self.credential_usage_count:
                                self.credential_usage_count[credential_path] = 0
                            self.credential_usage_count[credential_path] += 1
                        
                        client = bigquery.Client.from_service_account_json(credential_path)
                        # client = bigquery.Client()
                        try:
                            query_job = client.query(sql)
                            results = query_job.result().to_dataframe()
                            if results.empty:
                                return "empty", "No data found for the specified query."
                            else:
                                return "success", results
                        except Exception as e:
                            client.close()
                            if "403 Quota exceeded" in str(e):
                                print("403 Quota exceeded again, trying next credential")
                                continue
                            else:
                                return "error", f"Error occurred while fetching data: {e}"
                return "error", f"Error occurred while fetching data: {e}"
        elif dialect == "snowflake":
            snowflake_credential = json.load(open(os.path.join(self.storage_root, self.input_data_root, "snowflake_credential.json")))
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
        elif dialect == "sqlite":
            db_path = os.path.join(self.data_root, self.input_data_root, self.local_dbs[dialect], f"{db_name}.sqlite")
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

def parse_dialect_path_pair(value: str) -> tuple[str, str]:
    if ':' in value:
        dialect, path = value.split(':', 1)
    elif '=' in value:
        dialect, path = value.split('=', 1)
    else:
        raise argparse.ArgumentTypeError(
            f"Invalid format '{value}'. Use 'dialect:path' or 'dialect=path'"
        )
    
    dialect = dialect.strip().lower()
    path = path.strip().rstrip('/') 
    
    if not dialect or not path:
        raise argparse.ArgumentTypeError("Both dialect and path must be non-empty")

def df_to_markdown(df: Optional[pd.DataFrame]) -> str:
    if df is None: return "*(No result available)*"
    if df.empty: return "*(Empty result set)*"
    try: 
        return df.to_markdown(index=False)
    except Exception: 
        return df.to_string(index=False)
