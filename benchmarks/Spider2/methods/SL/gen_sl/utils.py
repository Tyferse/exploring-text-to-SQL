import re
import json
import numpy as np
def compute_metrics(gen_set, gold_set):
    recall = len(gen_set & gold_set) / len(gold_set) if gold_set else 0
    precision = len(gen_set & gold_set) / len(gen_set) if gen_set else 0
    return recall, precision

def parse_response(response):
    pattern = r"```sql\s*(.*?)\s*```"
    
    sql_blocks = re.findall(pattern, response, re.DOTALL)

    if sql_blocks:
        # Extract the last SQL query in the response text and remove extra whitespace characters
        last_sql = sql_blocks[-1].strip()
        return last_sql
    else:
        # print("No SQL blocks found.")
        return ""

def extract_code_blocks(text: str, tag: str):
    pattern = rf"```{tag}\s*\n(.*?)```"
    matches = re.findall(pattern, text, re.DOTALL | re.IGNORECASE)
    return [match.strip() for match in matches]

def append_json_line(path, obj):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False, indent=4) + "\n")

def clear_tb(tb):
    return tb.replace("\"", "").replace("`", "").upper()

def hard_cut(str_e, length=0):
    try:
        str_e = str(str_e)
        if length:
            if len(str_e) > length:
                str_e = str_e[:int(length)]
    except:
        pass
    return str_e

def deduplicate_by_value(input_dict):
    seen = set()
    result = {}
    for k, v in input_dict.items():
        if v not in seen:
            seen.add(v)
            result[k] = v
    return result

def strip_char(s):
    return s.strip('\n {}[]')

def parse_des(pre_col_values, nouns, debug):
    pre_col_values = pre_col_values.split("/*")[0].strip()
    if debug:
        print(pre_col_values)
    col, values = pre_col_values.split('#values:')
    _, col = col.split("#columns:")
    col = strip_char(col)
    values = strip_char(values)

    if values == '':
        values = []
    else:
        values = re.findall(r"([\"'])(.*?)\1", values)
    nouns_all = re.findall(r"([\"'])(.*?)\1", nouns)
    values_noun = set(values).union(set(nouns_all))
    values_noun = [x[1] for x in values_noun]
    return values_noun, col

def get_metrics(ce_json):
    ce_recall_tbs = []
    ce_precision_tbs = []
    ce_recall_cols = []
    ce_precision_cols = []
    for ce in ce_json:
        ce_recall_tb = ce["recall_tb"] if ce["recall_tb"] is not None else 0
        ce_precision_tb = ce["precision_tb"] if ce["precision_tb"] is not None else 0

        ce_recall_tbs.append(ce_recall_tb)
        ce_precision_tbs.append(ce_precision_tb)

        if "recall_col" in ce:
            ce_recall_col = ce["recall_col"] if ce["recall_col"] is not None else 0
            ce_precision_col = ce["precision_col"] if ce["precision_col"] is not None else 0
            ce_recall_cols.append(ce_recall_col)
            ce_precision_cols.append(ce_precision_col)

    print(f"""
P(recall_tb == 1):    {np.mean(np.array(ce_recall_tbs) == 1):.4f}
P(precision_tb == 1): {np.mean(np.array(ce_precision_tbs) == 1):.4f}
P(recall_col == 1):   {np.mean(np.array(ce_recall_cols) == 1):.4f}
P(precision_col == 1):{np.mean(np.array(ce_precision_cols) == 1):.4f}
AVG recall_tb:        {np.mean(ce_recall_tbs):.4f}
AVG precision_tb:     {np.mean(ce_precision_tbs):.4f}
AVG recall_col:       {np.mean(ce_recall_cols):.4f}
AVG precision_col:    {np.mean(ce_precision_cols):.4f}
    """)


if __name__ == "__main__":
    with open("parsed_snow_js_1m_ablation.json") as f:
        snow = json.load(f)
    # snow_new = []
    # for k, v in snow.items():
    #     snow_new.append(v)
    get_metrics(snow)