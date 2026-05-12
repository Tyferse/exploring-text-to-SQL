import sqlglot
import json
import re
from sqlglot.expressions import Column, Table
import os
from utils import extract_code_blocks, clear_tb

def update_db_id(full_name, db_id):
    full_name = full_name.split(".")
    full_name[0] = clear_tb(db_id)
    full_name = ".".join(full_name)
    return full_name

class Linker:
    def __init__(self):
        pass

    def get_tables(self, sql, dialect="snowflake"):
        try:
            parsed = sqlglot.parse_one(sql, dialect=dialect)

        except Exception as e:
            # print(e)
            return set()

        def extract_real_table_name(table):
            raw_sql = table.sql(dialect=dialect)
            table_part = re.split(r"\s+AS\s+", raw_sql, flags=re.IGNORECASE)[0]
            return table_part.strip("`\"")

        tables = []
        for table in parsed.find_all(Table):
            real_name = extract_real_table_name(table)
            tables.append(clear_tb(real_name))
        return set(tables)

    def get_columns(self, sql, all_cols, dialect="snowflake"):
        try:
            parsed = sqlglot.parse_one(sql, dialect=dialect)

        except Exception as e:
            # print(e)
            return set()
        
        columns = []
        for col in parsed.find_all(Column):
            col = str(col)
            if "." in col:
                cols = col.split(".")
            else:
                cols = [col]

            cols = [clear_tb(i) for i in cols]

            for all_col in all_cols:
                gold_col = all_col.split(".")[-1]
                for gen_col in cols:
                    if gen_col == gold_col:
                        columns.append(all_col)
        return set(columns)

    def get_all_tables(self, ex_pth, db_id):
        json_paths = []
        for dirpath, _, filenames in os.walk(ex_pth):
            for filename in filenames:
                if filename.endswith(".json") and not filename.endswith("credential.json"):
                    json_paths.append(os.path.join(dirpath, filename))
        tables = []
        columns = []
        # tables2 = []
        for j in json_paths:
            with open(j) as f:
                db_json = json.load(f)

            
            full_name = update_db_id(db_json["table_fullname"], db_id)

            tables.append(clear_tb(full_name))

            tables.append(clear_tb(".".join(j.replace(".json", "").split("/")[-3:])))
            columns += [clear_tb(full_name+"."+i) for i in db_json["column_names"]]

        return set(tables), set(columns)

    def get_all_tables_bird(self, data):
        table_names = data["table_names_original"]
        column_names = data["column_names_original"]

        table_column_names = []
        for table_idx, column_name in column_names:
            if table_idx == -1:
                continue  # skip *
            table_name = table_names[table_idx]
            table_column_names.append(clear_tb(f"{table_name}.{column_name}"))

        return set([clear_tb(i) for i in table_names]), set(table_column_names)

    def add_sqls(self, sqls, CE_PTHS, example_id):
        idx = 0
        for CE_PTH in CE_PTHS:
            for ex_id in os.listdir(CE_PTH):
                if ex_id == example_id: 
                    ex_pth = os.path.join(CE_PTH, ex_id)
                    for file in os.listdir(ex_pth):
                        if file.endswith(".log"):
                            
                            with open(os.path.join(ex_pth, file)) as f:
                                log_file = f.read()
                            for sql in extract_code_blocks(log_file, "sql"):
                                if 'SELECT "COLUMN_NAME" FROM DATABASE.SCHEMA.TABLE WHERE' not in sql:
                                    sqls[f"log_{idx}"] = sql
                                    idx += 1
        return sqls

    def add_sqls_from_json(self, sqls, JS_PTHS, example_id):
        with open(JS_PTHS) as f:
            js = json.load(f)
        idx = 0
        for j in js:
            if j["example_id"] == example_id:
                for json_ce in j["parsed_json_ce"]: 
                    sqls[f"log_{idx}"] = json_ce["sql"]
                    idx += 1
                for json_vr in j["vr_json"]: 
                    sqls[f"log_{idx}"] = json_vr["sql"]
                    idx += 1
        return sqls

    def add_sqls_from_json_omni(self, sqls, responses, question_id):
        idx = 0
        # j = js[question_id]
        for res in responses: 
            for sql in extract_code_blocks(res, "sql"):
                sqls[f"log_{idx}"] = sql
                idx += 1
        return sqls

    def parse_tbs(self, parsed, sqls, all_tbs, dialect="snowflake", db_id=None):
        for k, v in sqls.items():
            if k == "gold_tb":
                parsed["gold_tb"] = self.get_tables(v, dialect=dialect)
            else:
                parsed["gen_tb"] = (parsed.get("gen_tb", set()) | self.get_tables(v, dialect=dialect)) & all_tbs
        
        if db_id is not None:
            for tb in parsed["gen_tb"]:
                tb = update_db_id(tb, db_id)
        return parsed

    def update_cols(self, used_tbs, all_cols):
        filter_cols = []
        for tb in used_tbs:
            for col in all_cols:
                if tb in col:
                    filter_cols.append(col)
        all_cols = set(filter_cols)
        return all_cols

    def parse_cols(self, sqls, parsed, all_cols, dialect="snowflake"):
        for k, v in sqls.items():
            if k == "gold_tb":
                gold_col_candidates = self.get_columns(v, all_cols, dialect=dialect) & all_cols 
                gold_cols = []
                for col in gold_col_candidates:
                    # print(".".join(col.split(".")[:-1]), parsed["gold_tb"])
                    if ".".join(col.split(".")[:-1]) in parsed["gold_tb"]:
                        gold_cols.append(col)
                parsed["gold_col"] = set(gold_cols)
            else:
                parsed["gen_col"] = (parsed.get("gen_col", set()) | self.get_columns(v, all_cols, dialect=dialect)) & all_cols

        return parsed

    def table_back(self, parsed):
        # table back
        table_back = []
        for gen in parsed["gen_col"]:
            table_back.append(".".join(gen.split(".")[:-1]))

        parsed["gen_tb"] |= set(table_back)
        return parsed

    def spider2_table_back(self, parsed):
        table_back = []
        for gen in parsed["gen_tb"]:
            for gold in parsed["gold_tb"]:
                if gen.split(".")[1:] == gold.split(".")[1:] and gen != gold:
                    table_back.append(gold)

        parsed["gen_tb"] |= set(table_back)
        return parsed        

    def compute_metrics(self, gen_set, gold_set):
        recall = len(gen_set & gold_set) / len(gold_set) if gold_set else 0
        precision = len(gen_set & gold_set) / len(gen_set) if gen_set else 0
        return recall, precision

    def update_foreign_keys_by_table(self, parsed, schema_json):
        table_names = schema_json["table_names_original"]
        column_names = schema_json["column_names_original"]
        foreign_keys = schema_json["foreign_keys"]

        matched_columns = []

        for from_idx, to_idx in foreign_keys:
            from_table_idx, from_col = column_names[from_idx]
            to_table_idx, to_col = column_names[to_idx]

            if clear_tb(table_names[from_table_idx]) in parsed["gen_tb"]:
                matched_columns.append(clear_tb(f"{table_names[from_table_idx]}.{from_col}"))
            if clear_tb(table_names[to_table_idx]) in parsed["gen_tb"]:
                matched_columns.append(clear_tb(f"{table_names[to_table_idx]}.{to_col}"))
        # print(matched_columns)
        parsed["gen_col"] |= set(matched_columns)
        return parsed
