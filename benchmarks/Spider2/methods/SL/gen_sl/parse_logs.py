import argparse
import json
from tqdm import tqdm
import os
from naive_parser import Linker
from utils import clear_tb

def main(args):
    CE_PTHS = args.ce_paths
    GOLD_PTH = args.gold_path
    SNOW_JSON_PTH = args.snow_json_path
    OUTPUT_DIR = args.output_path or f"parsed_gold_tb_snow_{len(CE_PTHS)}.json"
    DB_PTH = args.db_path

    with open(SNOW_JSON_PTH) as f:
        snow_json = [json.loads(i) for i in f]

    linker = Linker()
    parsed_all = []

    for example_id in tqdm(os.listdir(CE_PTHS[0])):
        print(example_id)
        db_id = None
        for ex in snow_json:
            if example_id == ex["instance_id"]:
                db_id = ex["db_id"]
                break
        assert db_id is not None, example_id

        all_tbs, all_cols = linker.get_all_tables(os.path.join(DB_PTH, example_id), db_id)

        parsed = {}
        with open(GOLD_PTH) as f:
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

        sqls = linker.add_sqls({}, CE_PTHS, example_id)

        parsed = linker.parse_tbs(parsed, sqls, all_tbs, db_id=db_id)
        used_tbs = list(parsed["gold_tb"] | parsed["gen_tb"])
        all_cols = linker.update_cols(used_tbs, all_cols)
        parsed = linker.parse_cols(sqls, parsed, all_cols)
        parsed = linker.table_back(parsed)

        recall, precision = linker.compute_metrics(parsed["gen_tb"], parsed["gold_tb"])
        for k, v in parsed.items():
            parsed[k] = list(v)

        parsed_all.append({
            "example_id": example_id,
            "parsed_info": parsed,
            "recall_tb": recall,
            "precision_tb": precision
        })

    with open(OUTPUT_DIR, "w") as f:
        json.dump(parsed_all, f, indent=4)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse generated table results and compute recall/precision.")
    parser.add_argument('--ce_paths', nargs='+', required=True, help='List of result log directories.')
    parser.add_argument('--gold_path', required=True, help='Path to the gold tables .jsonl file.')
    parser.add_argument('--snow_json_path', required=True, help='Path to the spider2-snow .jsonl file.')
    parser.add_argument('--db_path', required=True, help='Path to the database examples directory.')
    parser.add_argument('--output_path', default=None, help='Path to output JSON file.')

    args = parser.parse_args()
    main(args)
