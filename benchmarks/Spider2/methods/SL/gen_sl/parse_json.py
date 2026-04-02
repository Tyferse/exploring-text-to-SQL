import json
from tqdm import tqdm
import os
import argparse
from naive_parser import Linker
from utils import clear_tb, compute_metrics


def merge_predictions(parsed, output_path):
    eid2tbs = {}
    for i in parsed:
        eid = i["example_id"]
        if eid not in eid2tbs:
            eid2tbs[eid] = i
        else:
            gen_tb_new = i["parsed_info"]["gen_tb"]
            gen_col_new = i["parsed_info"]["gen_col"]
            eid2tbs[eid]["parsed_info"]["gen_tb"] = list(
                set(eid2tbs[eid]["parsed_info"]["gen_tb"]) | set(gen_tb_new)
            )
            eid2tbs[eid]["parsed_info"]["gen_col"] = list(
                set(eid2tbs[eid]["parsed_info"]["gen_col"]) | set(gen_col_new)
            )
            eid2tbs[eid]["recall_tb"], eid2tbs[eid]["precision_tb"] = compute_metrics(
                set(eid2tbs[eid]["parsed_info"]["gen_tb"]),
                set(eid2tbs[eid]["parsed_info"]["gold_tb"]),
            )

    combined = list(eid2tbs.values())

    if os.path.exists(output_path):
        with open(output_path) as f:
            original = json.load(f)
        for i in range(len(original)):
            for j in combined:
                if original[i]["example_id"] == j["example_id"]:
                    original[i] = j
        combined = original

    with open(output_path, "w") as f:
        json.dump(combined, f, indent=4)


def main(args):
    with open(args.pred_path) as f:
        out_js = json.load(f)

    with open(args.snow_json_path) as f:
        snow_js = [json.loads(i) for i in f]

    linker = Linker()
    parsed_all = []
    for example in tqdm(out_js):
        example_id = example["output_seq"]["example_id"]
        print(example_id)

        for snow in snow_js:
            if snow["instance_id"] == example_id:
                db_id = snow["db_id"]

        all_tbs, all_cols = linker.get_all_tables(os.path.join(args.db_path, example_id), db_id=db_id)

        parsed = {}
        with open(args.gold_path) as f:
            for i in f:
                ex = json.loads(i)
                if ex["instance_id"] == example_id:
                    parsed["gold_tb"] = set(clear_tb(i) for i in ex["gold_tables"])

        if not parsed:
            print("Empty gold tb", example_id)
            parsed["gold_tb"] = set()
        if not parsed["gold_tb"] & all_tbs:
            print("Empty", example_id)
            parsed["gold_tb"] = set()       

        sqls = {}
        sqls = linker.add_sqls_from_json_omni(sqls, example["responses"], example_id)

        parsed = linker.parse_tbs(parsed, sqls, all_tbs)
        used_tbs = list(parsed["gold_tb"] | parsed["gen_tb"])
        all_cols = linker.update_cols(used_tbs, all_cols)
        parsed = linker.parse_cols(sqls, parsed, all_cols)
        parsed = linker.table_back(parsed)
        parsed = linker.spider2_table_back(parsed)

        recall, precision = linker.compute_metrics(parsed["gen_tb"], parsed["gold_tb"])
        for k, v in parsed.items():
            parsed[k] = list(v)

        parsed_all.append({
            "example_id": example_id,
            "parsed_info": parsed,
            "recall_tb": recall,
            "precision_tb": precision
        })

    merge_predictions(parsed_all, args.output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse and evaluate SQL generation outputs.")
    parser.add_argument("--pred_path", type=str, required=True, help="Path to prediction JSON file.")
    parser.add_argument("--gold_path", type=str, required=True, help="Path to gold table JSONL file.")
    parser.add_argument("--snow_json_path", type=str, required=True, help="Path to Spider2 Snow input JSONL file.")
    parser.add_argument("--output_path", type=str, required=True, help="Path to output parsed JSON file.")
    parser.add_argument("--db_path", type=str, required=True, help="Path to directory containing database table files.")

    args = parser.parse_args()
    main(args)
