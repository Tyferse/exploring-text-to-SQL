@echo off

cd /d "%~dp0"
set pyenv=sqldev\Scripts\python.exe

:: Change prompts with different tool set
pyenv scripts\run.py --experiment sl_agent_tools.json

:: Change max agent steps
pyenv scripts\run.py --experiment sl_agent_steps.json

:: Apply different LLM-based schema linking stratagies
pyenv scripts\run.py --experiment entity_sl.json

:: Define execution exploration's impact
pyenv scripts\run.py --experiment exec_explore.json

:: Application of different syntax correction options
pyenv scripts\run.py --experiment syntax_corr_types.json

:: Change number of max turns per instance
pyenv scripts\run.py --experiment syntax_corr_turns.json

:: Application of semantic correction options
pyenv scripts\run.py --experiment semantic_corr.json

:: Change candidate number
pyenv scripts\run.py --experiment consistancy_cand.json

:: Change candidate selection mode
pyenv scripts\run.py --experiment consistancy_mode.json
