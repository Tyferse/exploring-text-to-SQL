import argparse
import json
import os
import time
from openai import AzureOpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from utils import parse_response

def main():
    start = time.time()
    parser = argparse.ArgumentParser()
    parser.add_argument("--deployment_name", type=str, default="o3", help="Azure deployment name (not model name)")
    parser.add_argument("--api_version", type=str, default="2024-12-01-preview", help="Azure OpenAI API version")
    parser.add_argument("--input_file", type=str, default="spider2snow_input.json")
    parser.add_argument("--output_file", type=str, default="spider2snow_output.json")
    parser.add_argument("--n", type=int, default=8, help="Number of generations")
    parser.add_argument("--num_threads", type=int, default=8, help="Number of threads")
    parser.add_argument("--temperature", type=float, default=1.0)
    args = parser.parse_args()

    # Initialize Azure OpenAI client
    client = AzureOpenAI(
        azure_endpoint=os.environ.get("AZURE_ENDPOINT"),
        api_key=os.environ.get("AZURE_OPENAI_KEY"),
        api_version=args.api_version,
    )

    with open(args.input_file, "r", encoding="utf-8") as f:
        input_dataset = json.load(f)

    def process_item(item):
        messages = [{"role": "user", "content": item["input_seq"]}]
        try:
            response = client.chat.completions.create(
                model=args.deployment_name,
                messages=messages,
                temperature=args.temperature,
                n=args.n,
            )
            responses = [choice.message.content for choice in response.choices]
            sqls = [parse_response(r) for r in responses]

            item["responses"] = responses
            item["pred_sqls"] = sqls
        except Exception as e:
            print(f"[ERROR] Failed on example {item.get('output_seq', {}).get('example_id', '')}: {e}")
            item["responses"] = []
            item["pred_sqls"] = []
        return item

    results = []
    with ThreadPoolExecutor(max_workers=args.num_threads) as executor:
        futures = [executor.submit(process_item, item) for item in input_dataset]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing"):
            results.append(future.result())

    with open(args.output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(results)} examples to {args.output_file}, time: ", time.time() - start)

if __name__ == "__main__":
    main()