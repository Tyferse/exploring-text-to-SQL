@echo off
setlocal enabledelayedexpansion

set TIMESTAMP=%date:~6,4%%date:~3,2%%date:~0,2%-%time:~0,2%%time:~3,2%%time:~6,2%
set TIMESTAMP=%TIMESTAMP: =0%
set AZURE=false

:parse_args
if "%1"=="" goto :end_parse
set key=%1
if "%key%"=="--azure" (
    set AZURE=true
    shift
    goto :parse_args
)
if "%key%"=="--task" (
    set TASK=%2
    shift
    shift
    goto :parse_args
)
if "%key%"=="--model" (
    set API=%2
    shift
    shift
    goto :parse_args
)
shift
goto :parse_args
:end_parse

REM Set up
@REM if "%TASK%"=="lite" (
@REM     echo Downloading file from Google Drive...
@REM     python -c "import gdown; gdown.download('https://drive.google.com/uc?id=1coEVsCZq-Xvj9p2TnhBFoFTsY-UoYGmG', '../../spider2-lite/resource/local_sqlite.zip', quiet=False)"
@REM     if exist "..\..\spider2-lite\resource\databases\spider2-localdb" rmdir /s /q "..\..\spider2-lite\resource\databases\spider2-localdb"
@REM     mkdir "..\..\spider2-lite\resource\databases\spider2-localdb"
@REM     powershell -Command "Expand-Archive -Path '../../spider2-lite/resource/local_sqlite.zip' -DestinationPath '../../spider2-lite/resource/databases/spider2-localdb' -Force"
@REM )

@REM python spider_agent_setup_%TASK%.py --example_folder examples_%TASK%

REM Reconstruct data
@REM python reconstruct_data.py ^
@REM     --example_folder examples_%TASK% ^
@REM     --add_description ^
@REM     --add_sample_rows ^
@REM     --rm_digits ^
@REM     --make_folder ^
@REM     --clear_long_eg_des
@REM exit /b 1

echo Number of prompts.txt files in examples_%TASK% larger than 200KB before reducing:
powershell -Command "Get-ChildItem -Path examples_%TASK% -Recurse -Filter 'prompts.txt' | Where-Object { $_.Length -gt 200000 } | Measure-Object | Select-Object -ExpandProperty Count"

REM Run Schema linking and voting
python schema_linking.py ^
    --task %TASK% ^
    --db_path examples_%TASK% ^
    --linked_json_pth ..\..\data\linked_%TASK%_tmp0.json ^
    --reduce_col ^
    --gold_tb_pth "..\gold-tables\spider2-%TASK%-gold-tables.jsonl"
exit /b 1

echo Number of prompts.txt files in examples_%TASK% larger than 200KB before reducing:
powershell -Command "Get-ChildItem -Path examples_%TASK% -Recurse -Filter 'prompts.txt' | Where-Object { $_.Length -gt 200000 } | Measure-Object | Select-Object -ExpandProperty Count"

REM OUTPUT_PATH="output\%API%-%TASK%-log-%TIMESTAMP%"
set OUTPUT_PATH="output\%API%-%TASK%-log"
set NUM_VOTES=8
set NUM_WORKERS=2
echo AZURE mode: %AZURE%
echo Model: %API%
echo Task: %TASK%
echo Output Path: %OUTPUT_PATH%

REM Step 1: Self-refinement + Majority Voting
set CMD1=python run.py ^
    --task %TASK% ^
    --db_path examples_%TASK% ^
    --output_path %OUTPUT_PATH% ^
    --do_self_refinement ^
    --generation_model %API% ^
    --max_iter 5 ^
    --temperature 1 ^
    --early_stop ^
    --do_vote ^
    --num_votes %NUM_VOTES% ^
    --num_workers %NUM_WORKERS%

REM Step 2: Self-refinement + Majority Voting + Column Exploration + Rerun
set CMD2=python run.py ^
    --task %TASK% ^
    --db_path examples_%TASK% ^
    --output_path %OUTPUT_PATH% ^
    --do_self_refinement ^
    --generation_model %API% ^
    --do_column_exploration ^
    --column_exploration_model %API% ^
    --max_iter 5 ^
    --temperature 1 ^
    --early_stop ^
    --do_vote ^
    --num_votes %NUM_VOTES% ^
    --num_workers %NUM_WORKERS% ^
    --rerun ^
    --overwrite_unfinished

if "%AZURE%"=="true" (
    set CMD1=%CMD1% --azure
    set CMD2=%CMD2% --azure
)

@REM %CMD1%
echo Evaluation for Step 1
@REM python eval.py --log_folder %OUTPUT_PATH% --task %TASK%
@REM exit /b 1

@REM %CMD2%
echo Evaluation for Step 2
@REM python eval.py --log_folder %OUTPUT_PATH% --task %TASK%
@REM exit /b 1


REM Step 3: Random vote for tie
python run.py ^
    --task %TASK% ^
    --db_path examples_%TASK% ^
    --output_path %OUTPUT_PATH% ^
    --do_vote ^
    @REM --revote ^
    --random_vote_for_tie ^
    --num_votes %NUM_VOTES% ^
    --num_workers %NUM_WORKERS%

echo Evaluation for Step 3
@REM python eval.py --log_folder %OUTPUT_PATH% --task %TASK%
@REM exit /b 1

REM Step 4: Random vote final_choose
python run.py ^
    --task %TASK% ^
    --db_path examples_%TASK% ^
    --output_path %OUTPUT_PATH% ^
    --do_vote ^
    --revote ^
    --random_vote_for_tie ^
    --final_choose ^
    --num_votes %NUM_VOTES% ^
    --num_workers %NUM_WORKERS%

echo Evaluation for Step 4
@REM python eval.py --log_folder %OUTPUT_PATH% --task %TASK%
@REM exit /b 1

REM Final evaluation and get files for submission
python get_metadata.py --result_path %OUTPUT_PATH% --output_path output\%API%-%TASK%-csv 
@REM -%TIMESTAMP%
python get_metadata.py --result_path %OUTPUT_PATH% --output_path output\%API%-%TASK%-sql --file_type sql 
@REM -%TIMESTAMP% --file_type sql
cd ..\..\spider2-%TASK%\evaluation_suite
python evaluate.py --mode exec_result --result_dir ..\..\methods\ReFoRCE\output\%API%-%TASK%-csv --gold_dir gold
@REM -%TIMESTAMP%