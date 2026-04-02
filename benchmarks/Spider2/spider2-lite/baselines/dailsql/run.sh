#!/bin/bash

# cd /media/sf_UCode/ttsql

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd ${script_dir}

# source ~/lxvenv/bin/activate

# export PYTHONPATH="/media/sf_UCode/ttsql:$PYTHONPATH"

DEV=spider2-lite
# DEV=toy
LLM=mistralai/devstral-2512:free # gpt-4o
COMMENT=1112

# step1. preprocess
# cd ${script_dir}  
python preprocessed_data/spider2_preprocess.py --dev $DEV

cd third_party/stanford-corenlp-full-2018-10-05
nohup java -mx4g -cp "*" edu.stanford.nlp.pipeline.StanfordCoreNLPServer & cd ../../

# step2. run DAIL-SQL
python data_preprocess.py --dev $DEV 
python generate_question.py --dev $DEV --model $LLM --tokenizer $LLM --prompt_repr SQL --comment $COMMENT
python ask_llm.py --model $LLM --n 1 --thinking False --question postprocessed_data/${COMMENT}_${DEV}_CTX-200 

# step3. postprocess
python postprocessed_data/spider2_postprocess.py --dev $DEV --model $LLM --comment $COMMENT

# step4. evaluate
eval_suite_dir=$(readlink -f "../../evaluation_suite")
# cd ${eval_suite_dir}
python ${eval_suite_dir}/evaluate.py --mode sql --result_dir ${script_dir}/postprocessed_data/${COMMENT}_${DEV}_CTX-200/RESULTS_MODEL-${LLM}-SQL-postprocessed --gold_dir /content/ttsql/benchmarks/Spider2/spider2-lite/evaluation_suite/gold