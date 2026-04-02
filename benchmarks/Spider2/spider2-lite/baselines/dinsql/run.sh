#!/bin/bash
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

DEV=spider2-lite
LLM=mistralai/devstral-2512:free
COMMENT=1

# step1. preprocess
cd ${script_dir}  
# python preprocessed_data/spider2_preprocess.py --dev $DEV
# python DIN-SQL.py --dev $DEV --model $LLM --n 1 --processes 8

python postprocessed_data/spider2_postprocess.py --dev $DEV 

eval_suite_dir=$(readlink -f "${script_dir}/../../evaluation_suite")
cd ${eval_suite_dir}
python evaluate.py --mode sql --result_dir ${script_dir}/postprocessed_data/${DEV}/predicted-SQL-postprocessed --gold_dir /content/ttsql/benchmarks/Spider2/spider2-lite/evaluation_suite/gold
