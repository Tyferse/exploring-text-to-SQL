#!/bin/bash
set -e

export OPENAI_API_KEY=""
export OPENAI_BASE_URL=""

LOG_PATH=log_v3_topn100
TOP_N=100
NUM_CANDIDATES=5
DATA_FILE=spider2_data.json
SCHEMA_DIR=$LOG_PATH/final_schema_prompts
TASK=r1_lite

python sql_generation.py \
  --num_workers 8 \
  --num_candidates $NUM_CANDIDATES \
  --data_file $DATA_FILE \
  --schema_dir $SCHEMA_DIR \
  --log_path $LOG_PATH \
  --task $TASK

python sql_execution.py \
  --num_workers 4 \
  --num_candidates $NUM_CANDIDATES \
  --data_file $DATA_FILE \
  --log_path $LOG_PATH \
  --task $TASK

python sql_revise.py \
  --num_workers 8 \
  --num_candidates $NUM_CANDIDATES \
  --data_file $DATA_FILE \
  --schema_dir $SCHEMA_DIR \
  --log_path $LOG_PATH \
  --task $TASK

python sql_selection.py \
  --log_path $LOG_PATH \
  --num_candidates $NUM_CANDIDATES \
  --workers 8 \
  --task $TASK
