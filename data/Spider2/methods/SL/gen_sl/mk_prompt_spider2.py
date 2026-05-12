import argparse
import os
import json
from transformers import AutoTokenizer
from tqdm import tqdm

MAX_TOKENS = 80000
MAX_LEN = 250000

PROMPT_CE = """Given the schema and task, generate up to 10 diverse Snowflake SQL queries—ranging from simple to complex—that help understand the values in relevant columns. Use the following format:

```sql
SELECT "COLUMN_NAME" FROM DATABASE.SCHEMA.TABLE WHERE ...
```

Replace "DATABASE", "SCHEMA", and "TABLE" with actual names.

For nested JSON columns, use this pattern:

```sql
SELECT t."column_name", f.value::VARIANT:"key_name"::STRING AS "alias" 
FROM DATABASE.SCHEMA.TABLE t, LATERAL FLATTEN(input => t."json_column_name") f;
```

Each query should be unique. Avoid querying schema metadata or checking data types. Only write valid SELECT statements.

DB Schema:
{schema}

The table structure information is:
{table_structure}

The DB schema offers a partial view of the detailed schema, while the table structure includes all columns. If a required column is missing from the DB schema, you can refer to the table structure instead.

Task:
{task}
"""

PROMPT = """Given the schema and task, please think step by step and generate one Snowflake SQL query by the following format:

```sql
SELECT "COLUMN_NAME" FROM DATABASE.SCHEMA.TABLE WHERE ...
```

Replace "DATABASE", "SCHEMA", and "TABLE" with actual names.

For nested JSON columns, use this pattern:

```sql
SELECT t."column_name", f.value::VARIANT:"key_name"::STRING AS "alias" 
FROM DATABASE.SCHEMA.TABLE t, LATERAL FLATTEN(input => t."json_column_name") f;
```

Avoid querying schema metadata or checking data types. Only write valid SELECT statements.

DB Schema:
{schema}

The table structure information is:
{table_structure}

The DB schema offers a partial view of the detailed schema, while the table structure includes all columns. If a required column is missing from the DB schema, you can refer to the table structure instead.

Task:
{task}
"""


def get_dictionary(db_path, task, json_path):
    task_dict = {}
    with open(json_path) as f:
        for line in f:
            line_js = json.loads(line)
            key = line_js["instance_id"]
            value = line_js["instruction"] if task == "snow" else line_js["question"]
            task_dict[key] = value

    dictionaries = [entry for entry in os.listdir(db_path) if os.path.isdir(os.path.join(db_path, entry))]
    return dictionaries, task_dict


def split_schema_with_overlap(schema: str, table_structure: str, overlap: int = 1000):
    n = 2
    while len(schema) // n + len(table_structure) > MAX_LEN:
        n += 1

    schema_len = len(schema)
    avg_len = schema_len // n
    remainder = schema_len % n

    splits = []
    start = 0
    for i in range(n):
        extra = 1 if i < remainder else 0
        end = start + avg_len + extra

        overlap_start = max(0, start - overlap // 2) if i > 0 else start
        overlap_end = min(schema_len, end + overlap // 2) if i < n - 1 else end

        schema_chunk = schema[overlap_start:overlap_end]
        full_chunk = schema_chunk + "\n" + table_structure

        if len(full_chunk) <= MAX_LEN:
            splits.append(full_chunk)
        else:
            raise ValueError(f"Split {i} exceeds max_len: {len(full_chunk)} > {MAX_LEN}")
        start = end
    return splits


def main(args):
    tokenizer = AutoTokenizer.from_pretrained(args.tokenizer_name)
    prompt_template = PROMPT_CE if args.use_prompt_ce else PROMPT
    dictionaries, task_dict = get_dictionary(args.db_path, args.task, args.snow_json_path)

    input_prompts = []
    for example in tqdm(dictionaries):
        task = task_dict.get(example, "")
        prompt_pth = os.path.join(args.db_path, example, "prompts.txt")
        if not os.path.exists(prompt_pth) or os.path.getsize(prompt_pth) < args.threshold:
            continue

        with open(prompt_pth) as f:
            schema = f.read()
        table_structure = schema[schema.find("The table structure information is"):]
        schema = schema[:schema.find("The table structure information is")]
        schema_str = schema + "\n" + table_structure

        if len(schema_str) > MAX_LEN:
            schema_splits = split_schema_with_overlap(schema, table_structure)
        else:
            schema_splits = [schema + "\n" + table_structure]

        for split in schema_splits:
            input_text = prompt_template.format(schema=split, task=task, table_structure=table_structure)
            token_ids = tokenizer.encode(input_text, add_special_tokens=False)
            if len(token_ids) > MAX_TOKENS:
                input_prompts.append({
                    "input_seq": prompt_template.format(schema=split[:len(split)//2], task=task, table_structure=table_structure),
                    "output_seq": {"example_id": example, "task": task}
                })
                input_prompts.append({
                    "input_seq": prompt_template.format(schema=split[len(split)//2:], task=task, table_structure=table_structure),
                    "output_seq": {"example_id": example, "task": task}
                })
            else:
                input_prompts.append({
                    "input_seq": input_text,
                    "output_seq": {"example_id": example, "task": task}
                })

    with open(args.output_path, "w") as f:
        json.dump(input_prompts, f, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate SQL prompts for Snowflake tasks")
    parser.add_argument("--db_path", type=str, default="../../ReFoRCE/examples_snow", help="Path to database directory containing full prompts.txt")
    parser.add_argument("--task", type=str, choices=["snow", "lite"], default="snow", help="Task type")
    parser.add_argument("--output_path", type=str, default="spider2snow_input.json", help="Path to output JSON file")
    parser.add_argument("--tokenizer_name", type=str, default="Qwen/Qwen2.5-Coder-1.5B-Instruct", help="Huggingface model name for tokenizer")
    parser.add_argument("--use_prompt_ce", action="store_true", help="Use PROMPT_CE template instead of default")
    parser.add_argument("--snow_json_path", type=str, required=True, help="Path to Spider2 Snow input JSONL file.")
    parser.add_argument("--threshold", type=int, default=0, help="Do schema linking only on prompt size > threshold.")

    args = parser.parse_args()
    main(args)
