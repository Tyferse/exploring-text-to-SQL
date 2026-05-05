import argparse
import os
import pandas as pd
import json
import math
from openai import OpenAI
import itertools
from config import *
from tqdm import tqdm
import concurrent.futures
import threading

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log_path", type=str, required=True)
    parser.add_argument("--num_candidates", type=int, default=5)
    parser.add_argument("--max_rows", type=int, default=1000)
    parser.add_argument("--max_chars", type=int, default=10000)
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--task", default="r1_lite")
    return parser.parse_args()

def read_sql(path):
    return open(path, "r", encoding="utf-8").read()

def read_csv_safe(path):
    try:
        return pd.read_csv(path)
    except:
        return None

def read_txt_safe(path):
    try:
        return open(path, "r", encoding="utf-8").read()
    except:
        return None

def build_candidates(instance_id, args):
    candidates = []

    for cid in range(args.num_candidates):
        # ---------- sql_gen ----------
        sql_gen_dir = f"{args.log_path}/sql_gen"
        sql_path = f"{sql_gen_dir}/{args.task}_sql_{cid}/{instance_id}.sql"
        csv_path = f"{sql_gen_dir}/{args.task}_execution_result_{cid}/{instance_id}.csv"
        err_path = f"{sql_gen_dir}/{args.task}_execution_error_{cid}/{instance_id}.txt"

        sql = read_sql(sql_path) if os.path.exists(sql_path) else None
        df = read_csv_safe(csv_path)
        err = read_txt_safe(err_path)

        status = "success" if df is not None else "error"

        # ---------- revise  ----------
        if status == "error":
            revise_dir = f"{args.log_path}/sql_revise/{args.task}_sql_{cid}"
            r_sql = f"{revise_dir}/{instance_id}.sql"
            r_csv = f"{revise_dir}/{instance_id}.csv"
            r_err = f"{revise_dir}/{instance_id}.txt"

            if os.path.exists(r_csv):
                sql = read_sql(r_sql)
                df = read_csv_safe(r_csv)
                err = None
                status = "success"
            else:
                sql = read_sql(r_sql) if os.path.exists(r_sql) else sql
                df = None
                err = read_txt_safe(r_err) if os.path.exists(r_err) else err    
                status = "error"

        candidates.append({
            "cid": cid,
            "sql": sql,
            "status": status,
            "df": df,
            "error": err
        })

    return candidates

def compare_pandas_table(pred, gold, ignore_order=False):

    tolerance = 1e-2

    def vectors_match(v1, v2, tol=tolerance, ignore_order_=False):
        if ignore_order_:
            v1, v2 = (sorted(v1, key=lambda x: (x is None, str(x), isinstance(x, (int, float)))),
                      sorted(v2, key=lambda x: (x is None, str(x), isinstance(x, (int, float)))))
        if len(v1) != len(v2):
            return False
        for a, b in zip(v1, v2):
            if pd.isna(a) and pd.isna(b):
                continue
            elif isinstance(a, (int, float)) and isinstance(b, (int, float)):
                if not math.isclose(float(a), float(b), abs_tol=tol):
                    return False
            elif a != b:
                return False
        return True

    gold_cols = gold
    pred_cols = pred

    t_gold_list = gold_cols.transpose().values.tolist()
    t_pred_list = pred_cols.transpose().values.tolist()
    score = 1
    for _, gold in enumerate(t_gold_list):
        if not any(vectors_match(gold, pred, ignore_order_=ignore_order) for pred in t_pred_list):
            score = 0
        else:
            for j, pred in enumerate(t_pred_list):
                if vectors_match(gold, pred, ignore_order_=ignore_order):
                    break
    return score

def cluster_by_execution(candidates):
    clusters = []

    for c in candidates:
        if c["status"] != "success":
            continue

        matched = False
        for cluster in clusters:
            if compare_pandas_table(c["df"], cluster[0]["df"], ignore_order=True):
                cluster.append(c)
                matched = True
                break

        if not matched:
            clusters.append([c])

    return clusters

def select_by_consistency(clusters):
    clusters.sort(key=len, reverse=True)
    max_len = len(clusters[0])

    top_clusters = [c for c in clusters if len(c) == max_len]
    if len(top_clusters) == 1:
        return top_clusters[0][0]
    return top_clusters

def model_vote(instance_id, tied_clusters, args, schema, question):
    client = OpenAI(
        api_key=os.environ.get("OPENAI_API_KEY"),
        base_url=os.environ.get("OPENAI_BASE_URL"),
    )
    scores = {}
    candidates = [c[0] for c in tied_clusters]

    for c in candidates:
        scores[c["cid"]] = 0
    if instance_id.startswith("bq"):
        dialect = "BigQuery"
    elif instance_id.startswith("ga"):
        dialect = "BigQuery"
    elif instance_id.startswith("sf"):
        dialect = "snowflake"
    else:
        dialect = "SQLite"
        
    for a, b in itertools.combinations(candidates, 2):
        prompt = SQL_SELECTION.replace("{Database_Schema}", schema)
        prompt = prompt.replace("{Question}", question)
        prompt = prompt.replace("{dialect}", dialect)   
        prompt = prompt.replace("{sql1}", a['sql'])
        prompt = prompt.replace("{sql2}", b['sql'])
        prompt = prompt.replace("{re1}", str(a['df'].iloc[:min(len(a['df']), args.max_rows)])[:args.max_chars])
        prompt = prompt.replace("{re2}", str(b['df'].iloc[:min(len(b['df']), args.max_rows)])[:args.max_chars])

        resp = client.chat.completions.create(
            model="tencent/hy3-preview:free",
            messages=[{"role": "user", "content": prompt}]
        )

        out = resp.choices[0].message.content.lower()
        if "sql1" in out:
            scores[a["cid"]] += 1
        elif "sql2" in out:
            scores[b["cid"]] += 1

    best = max(scores.items(), key=lambda x: x[1])[0]
    return next(c for c in candidates if c["cid"] == best)

def dump_final_selection(instance_id, selected, args):
    base_dir = f"{args.log_path}/sql_selection/final/{instance_id}"
    os.makedirs(base_dir, exist_ok=True)

    with open(os.path.join(base_dir, "selected.sql"), "w", encoding="utf-8") as f:
        f.write(selected["sql"])

    if selected["status"] == "success" and selected["df"] is not None:
        selected["df"].to_csv(
            os.path.join(base_dir, "result.csv"),
            index=False,
            encoding="utf-8"
        )
    else:
        with open(os.path.join(base_dir, "error.txt"), "w", encoding="utf-8") as f:
            f.write(selected.get("error", "Unknown execution error"))

def process_instance(name, args, lock):
    instance_id = name.replace(".txt", "")
    candidates = build_candidates(instance_id, args)
    clusters = cluster_by_execution(candidates)
    if not clusters:
        fallback = min(candidates, key=lambda x: x["cid"])

        fallback["fallback"] = "all_failed"

        with lock:
            with open(f"{args.log_path}/sql_selection/selected.jsonl", "a") as f:
                f.write(json.dumps({
                    "instance_id": instance_id,
                    "candidate": fallback["cid"],
                    "status": fallback["status"],
                    "fallback": "all_failed"
                }) + "\n")

        dump_final_selection(instance_id, fallback, args)
        return

    selected = select_by_consistency(clusters)

    if isinstance(selected, list):
        with open(f"{args.log_path}/final_schema_prompts/{instance_id}.txt", "r") as f:
            schema = f.read()
        with open("spider2_data.json", "r") as f:
            spider2 = json.load(f)
        question = spider2[instance_id]["question"]
        selected = model_vote(instance_id, selected, args, schema, question)

    with lock:
        with open(f"{args.log_path}/sql_selection/selected.jsonl", "a") as f:
            f.write(json.dumps({
                "instance_id": instance_id,
                "candidate": selected["cid"],
                "status": selected["status"]
            }) + "\n")

    dump_final_selection(instance_id, selected, args)
        
def main():
    args = parse_args()
    os.makedirs(f"{args.log_path}/sql_selection", exist_ok=True)

    instance_ids = os.listdir(f"{args.log_path}/final_schema_prompts")
    lock = threading.Lock()

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(process_instance, name, args, lock) for name in instance_ids]
        with tqdm(total=len(instance_ids), desc="Processing instances") as pbar:
            for future in concurrent.futures.as_completed(futures):
                future.result()  # To raise any exceptions
                pbar.update(1)
        
if __name__ == "__main__":
    main()
