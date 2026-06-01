@echo off

cd /d "%~dp0"
set pyenv=sqldev\Scripts\python.exe

:: Change prompts with different tool set
pyenv scripts\run.py --experiment sl_agent_tools.json

:: Change max agent steps
pyenv scripts\run.py --experiment sl_agent_steps.json

:: Apply different LLM-based schema linking stratagies
pyenv scripts\run.py --experiment entity_sl.json

:: Define execution exploration's influence
pyenv scripts\run.py --experiment exec_explore.json
