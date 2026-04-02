@echo off
setlocal enabledelayedexpansion

for /f "tokens=1,* delims==" %%a in (..\..\..\..\.env) do (
    set %%a=%%b
)

set OPENAI_API_KEY=$OPENROUTER_API_KEY$
set OPENAI_BASE_URL=https://openrouter.ai/api/v1

set LOG_PATH=log_v3_topn100
set TOP_N=100
set NUM_CANDIDATES=5
set DATA_FILE=spider2_data.json
set SCHEMA_DIR=%LOG_PATH%\final_schema_prompts
set TASK=r1_lite

echo Запуск sql_generation.py...
python sql_generation.py ^
  --num_workers 4 ^
  --num_candidates %NUM_CANDIDATES% ^
  --data_file %DATA_FILE% ^
  --schema_dir %SCHEMA_DIR% ^
  --log_path %LOG_PATH% ^
  --task %TASK%

if %errorlevel% neq 0 (
  echo Ошибка при выполнении sql_generation.py
  exit /b %errorlevel%
)

echo Запуск sql_execution.py...
python sql_execution.py ^
  --num_workers 4 ^
  --num_candidates %NUM_CANDIDATES% ^
  --data_file %DATA_FILE% ^
  --log_path %LOG_PATH% ^
  --task %TASK%

if %errorlevel% neq 0 (
  echo Ошибка при выполнении sql_execution.py
  exit /b %errorlevel%
)

echo Запуск sql_revise.py...
python sql_revise.py ^
  --num_workers 4 ^
  --num_candidates %NUM_CANDIDATES% ^
  --data_file %DATA_FILE% ^
  --schema_dir %SCHEMA_DIR% ^
  --log_path %LOG_PATH% ^
  --task %TASK%

if %errorlevel% neq 0 (
  echo Ошибка при выполнении sql_revise.py
  exit /b %errorlevel%
)

echo Запуск sql_selection.py...
python sql_selection.py ^
  --log_path %LOG_PATH% ^
  --num_candidates %NUM_CANDIDATES% ^
  --workers 4 ^
  --task %TASK%

if %errorlevel% neq 0 (
  echo Ошибка при выполнении sql_selection.py
  exit /b %errorlevel%
)
