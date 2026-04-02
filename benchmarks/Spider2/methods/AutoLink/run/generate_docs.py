import os
import json
from utils import *
import time
from datetime import datetime

BIGQUERY_PATH = "..\..\..\spider2-lite\\resource\\databases\\bigquery"
SNOWFLAKE_PATH = "..\..\..\spider2-lite\\resource\\databases\\snowflake"
LOCALDB_PATH = "..\..\..\spider2-lite\\resource\\databases\\sqlite"

DBS_PATH = [BIGQUERY_PATH, SNOWFLAKE_PATH, LOCALDB_PATH]

def generate_documents(db: str, output_path: str = "documents"):
    if "bigquery" in db:
        output_file = "bigquery.json"
    elif "snowflake" in db:
        output_file = "snowflake.json"
    elif "sqlite" in db:
        output_file = "localdb.json"
    else:
        raise ValueError("Invalid database path")

    subdirs = get_subdir(db)
    documents = {}

    log_file = open("pre_logs/gen_docs.log", 'a', encoding='utf-8')

    for subdir in subdirs:
        log_file.write(datetime.now().isoformat(sep=" ") + " | begin | " + output_file.split('.')[0] + " | " + subdir + "\n")

        documents[subdir] = {}
        subdir_path = os.path.join(db, subdir)
        json_files = get_json_files(subdir_path)

        processed_tables = {}

        is_error = False

        for json_file in json_files:

            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            keys = list(data.keys())

            table_name = data["table_fullname"]

            if "nested_column_names" in keys:
                column_names = data["column_names"]
                column_types = data["column_types"]
                nested_column_names = data["nested_column_names"]
                if len(nested_column_names) <= len(column_names):
                    column_names = nested_column_names
                    column_types = data["nested_column_types"]
                    column_descriptions = data["description"]
                else:
                    column_descriptions = []
                    for column_name in column_names:
                        for nested_column_name, column_description in zip(nested_column_names, data["description"]):
                            if column_name == nested_column_name:
                                column_descriptions.append(column_description)
                                break
            else:
                column_names = data["column_names"]
                column_types = data["column_types"]
                column_descriptions = data["description"]
                
            current_column_set = set(column_names)
            current_column_types_dict = dict(zip(column_names, column_types))
            current_descriptions_dict = dict(zip(column_names, column_descriptions))

            sample_rows = data["sample_rows"]

            is_partition = False
            for table, info in processed_tables.items():
                table_name_similar = remove_digits(table_name) == remove_digits(table)
                
                if table_name_similar:
                    existing_column_set = set(info["columns"])
                    
                    intersection = current_column_set & existing_column_set
                    union = current_column_set | existing_column_set
                    
                    if len(union) > 0:
                        similarity = len(intersection) / len(union)
                        
                        if similarity >= 1:
                            info["similar_tables"].append(table_name)
                            merged_columns = info["columns"].copy()
                            for col in column_names:
                                if col not in merged_columns:
                                    merged_columns.append(col)
                            
                            merged_column_types = info["column_types"].copy()
                            existing_types_dict = dict(zip(info["columns"], info["column_types"]))
                            for col in merged_columns:
                                if col not in existing_types_dict:
                                    merged_column_types.append(current_column_types_dict.get(col, ""))
                            
                            merged_descriptions = info["description"].copy()
                            existing_desc_dict = dict(zip(info["columns"], info["description"]))
                            for col in merged_columns:
                                if col not in existing_desc_dict:
                                    merged_descriptions.append(current_descriptions_dict.get(col, ""))
                            
                            info["columns"] = merged_columns
                            info["column_types"] = merged_column_types
                            info["description"] = merged_descriptions
                            
                            is_partition = True
                            break

            if not is_partition:
                processed_tables[table_name] = {
                    "columns": column_names,
                    "column_types": column_types,
                    "similar_tables": [],
                    "description": column_descriptions,
                    "sample_rows": sample_rows
                }

        for table_name, table_info in processed_tables.items():
            column_names = table_info["columns"]
            similar_tables = table_info["similar_tables"]
            description = table_info["description"]
            column_types = table_info["column_types"]
            sample_rows = table_info["sample_rows"]

            documents[subdir][table_name] = {
                "similar_tables": similar_tables,
                "columns": {},
                "column_types": column_types,
                "sample_values": []
            }
            for column_name in column_names:
                column_values = []
                for sample_row in sample_rows:
                    column_values.append(str(sample_row.get(column_name, "")))
                documents[subdir][table_name]["sample_values"].append(column_values)

            for column_name, column_type, column_desc in zip(column_names, column_types, description):
                column_desc = column_desc if column_desc is not None else ""
                desc = (
                        "column name: " + column_name + "\n" +
                        "column type: " + column_type + "\n" +
                        "table name: " + table_name + "\n" +
                        "description: " + column_desc + "\n"
                )
                documents[subdir][table_name]["columns"][column_name] = desc

        if is_error:
            print(f"{subdir} has error")
        
        log_file.write(datetime.now().isoformat(sep=" ") + " | end | " + output_file.split('.')[0] + " | " + subdir + "\n")

    os.makedirs(output_path, exist_ok=True)
    log_file.close()

    with open(os.path.join(output_path, output_file), "w", encoding="utf-8") as f:
        json.dump(documents, f, indent=4, ensure_ascii=False)
    

if __name__ == "__main__":
    print("Generate documents ...")

    os.makedirs("pre_logs", exist_ok=True)
    with open("pre_logs/gen_docs.log", 'w', encoding='utf-8'):
        pass

    for db in DBS_PATH:
        if "bigquery" in db:
            print("Processing BigQuery...")
            generate_documents(db, output_path="documents")
        if "snowflake" in db:
            print("Processing Snowflake...")
            generate_documents(db, output_path="documents")
        if "sqlite" in db:
            print("Processing SQLite...")
            generate_documents(db, output_path="documents")
