@echo off
setlocal enabledelayedexpansion

for /f "tokens=1,* delims==" %%a in (..\..\..\..\..\.env) do (
    set %%a=%%b
)

set OPENAI_API_KEY=%OPENROUTER_API_KEY%
set OPENAI_BASE_URL=https://openrouter.ai/api/v1

set LOG_PATH=log_v3_topn100
set TOP_N=100

echo Запуск generate_docs.py...
@REM python generate_docs.py
if %errorlevel% neq 0 (
  echo Ошибка при выполнении generate_docs.py
  exit /b %errorlevel%
)

echo Запуск embedding_docs.py...
@REM python embedding_docs.py
if %errorlevel% neq 0 (
  echo Ошибка при выполнении embedding_docs.py
  exit /b %errorlevel%
)

echo Запуск retrieve_topk_schema.py...
@REM python retrieve_topk_schema.py --log_path %LOG_PATH% --top_n %TOP_N%
if %errorlevel% neq 0 (
  echo Ошибка при выполнении retrieve_topk_schema.py
  exit /b %errorlevel%
)

echo Запуск add_id.py...
@REM python add_id.py --log_path %LOG_PATH%
if %errorlevel% neq 0 (
  echo Ошибка при выполнении add_id.py
  exit /b %errorlevel%
)

echo Запуск generate_schema.py (initial)...
@REM python generate_schema.py --log_path %LOG_PATH% --is_initial
@REM if %errorlevel% neq 0 (
@REM   echo Ошибка при выполнении generate_schema.py (initial)
@REM   exit /b %errorlevel%
@REM )

echo Запуск complete_schema.py...
python complete_schema.py --log_path %LOG_PATH%
if %errorlevel% neq 0 (
  echo Ошибка при выполнении complete_schema.py
  exit /b %errorlevel%
)

echo Запуск postprocess.py...
python postprocess.py --log_path %LOG_PATH%
if %errorlevel% neq 0 (
  echo Ошибка при выполнении postprocess.py
  exit /b %errorlevel%
)

echo Запуск generate_schema.py (final)...
python generate_schema.py --log_path %LOG_PATH%
if %errorlevel% neq 0 (
  echo Ошибка при выполнении generate_schema.py (final)
  exit /b %errorlevel%
)
