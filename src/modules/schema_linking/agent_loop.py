import ast
import json
import re
import time
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

# from langchain_core.rate_limiters import BaseRateLimiter
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage, SystemMessage

from .tools import TOOL_ARGUMENTS
from src.utils.logger import get_logger
from src.utils.models import serialize_messages


DEFAULT_RETRY_CONFIG = {
    "max_attempts": 3,
    "initial_delay": 2.0,
    "max_delay": 30.0,
    "backoff_multiplier": 2.0,
}


def extract_balanced_parentheses(text: str, start_idx: int) -> Optional[str]:
    """
    Находит строку внутри сбалансированных скобок, начиная с start_idx.
    Учитывает вложенные скобки и кавычки.
    """
    if start_idx >= len(text) or text[start_idx] != '(':
        return None
        
    depth = 0
    in_single_quote = False
    in_double_quote = False
    escape_next = False
    
    for i in range(start_idx, len(text)):
        char = text[i]
        
        if escape_next:
            escape_next = False
            continue
            
        if char == '\\':
            escape_next = True
            continue
            
        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            
        if not in_single_quote and not in_double_quote:
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
                if depth == 0:
                    return text[start_idx + 1 : i]
                    
    return None

def parse_tool_calls(llm_text: str) -> List[Dict[str, Any]]:
    """
    Парсит вызовы инструментов вида @name(args) из текста LLM.
    Использует schema-aware regex fallback для надежного извлечения key="value" пар.
    """
    calls = []
    pattern = re.compile(r"@(\w+)\s*\(")
    
    for match in pattern.finditer(llm_text):
        tool_name = match.group(1)
        start_idx = match.end() - 1  # Позиция '('
        
        args_str = extract_balanced_parentheses(llm_text, start_idx)
        
        if args_str is None:
            continue
            
        args_str = args_str.strip()
        parsed_args = {}
        
        # 1. Если аргументов нет (например, @stop)
        if not args_str:
            if tool_name in TOOL_ARGUMENTS and not TOOL_ARGUMENTS[tool_name]:
                calls.append({"name": tool_name, "args": {}, "raw": ""})
                continue
                
        # 2. Попытка парсинга как Python dict
        try:
            test_eval = ast.literal_eval(f"{{{args_str}}}") if not args_str.startswith('{') else ast.literal_eval(args_str)
            if isinstance(test_eval, dict):
                calls.append({"name": tool_name, "args": test_eval, "raw": args_str})
                continue
        except Exception:
            pass
            
        # 3. Попытка парсинга как JSON
        try:
            parsed_args = json.loads(args_str)
            if isinstance(parsed_args, dict):
                calls.append({"name": tool_name, "args": parsed_args, "raw": args_str})
                continue
        except json.JSONDecodeError:
            pass
            
        # Извлекаем каждый ожидаемый аргумент по отдельности
        expected_args = TOOL_ARGUMENTS.get(tool_name, [])
        if expected_args:
            for arg_name in expected_args:
                # Группа 1: значение в двойных кавычках, Группа 2: в одинарных
                pattern_arg = rf'{arg_name}\s*=\s*(?:"((?:[^"\\]|\\.)*)"|\'((?:[^\'\\]|\\.)*)\')'
                match_arg = re.search(pattern_arg, args_str, re.DOTALL)
                
                if match_arg:
                    # Берем ту группу, которая совпала (1 или 2)
                    value = match_arg.group(1) if match_arg.group(1) is not None else match_arg.group(2)
                    
                    # Очищаем от артефактов экранирования LLM
                    value = value.replace("\\'", "'").replace('\\"', '"').replace("\\n", "\n")
                    parsed_args[arg_name] = value
            
            # Если мы успешно извлекли хотя бы один ожидаемый аргумент, считаем парсинг успешным
            if parsed_args:
                calls.append({"name": tool_name, "args": parsed_args, "raw": args_str})
                continue

        # 5. Ошибка парсинга
        calls.append({
            "name": tool_name, 
            "args": {"_parse_error": True, "raw_content": args_str}, 
            "raw": args_str
        })
        
    return calls


class SchemaLinkingAgent:
    def __init__(
        self, 
        model: BaseChatModel, 
        tools: Dict[str, Any], 
        config: Dict[str, Any],
        cache_dir: Optional[Path] = None,
        retry_config: Optional[Dict[str, Any]] = None
    ):
        assert all(key in config for key in ["max_turns", "max_draft_calls", "additional_k", 
                                             "input_data_root", "vsm", "executor"])
        self.model = model
        self.tools = tools
        self.config = config
        self.max_messages = config.get("max_messages", 5)
        self.max_turns = config.get("max_turns", 10)
        self.max_draft_calls = config.get("max_draft_calls", 3)
        self.cache_dir = Path(cache_dir) if cache_dir else None
        self.retry_config = {**DEFAULT_RETRY_CONFIG, **(retry_config or {})}
        
    def _check_tool_limits(self, tool_name: str, state: Dict[str, Any]) -> Tuple[bool, str]:
        if tool_name == "sql_draft":
            if state.get("draft_count", 0) >= self.max_draft_calls:
                return False, f"[LIMIT] Max @sql_draft calls ({self.max_draft_calls}) reached."
            if state.get("draft_this_turn", False):
                return False, "[LIMIT] Only one @sql_draft call per turn allowed."
        return True, ""

    def _execute_tool(self, tool_name: str, args: Dict[str, Any], state: Dict[str, Any]) -> str:
        tool_fn = self.tools.get(tool_name)
        if not tool_fn:
            return f"[ERROR] Tool @{tool_name} is not enabled in this experiment."
        
        try:
            result = tool_fn.invoke(args) if hasattr(tool_fn, "invoke") else tool_fn(**args)
            if tool_name == "schema_retrieval" and "[RETRIEVED" in str(result):
                try:
                    if "Details: " in result:
                        details_str = result.split("Details: ", 1)[1]
                        retrieved_cols = json.loads(details_str)
                        
                        existing_ids = state.get("retrieved_column_ids", [])
                        new_meta = []
                        
                        for col in retrieved_cols:
                            col_id = col.get("column_id")
                            if col_id and col_id not in existing_ids:
                                existing_ids.append(col_id)
                                new_meta.append({
                                    "id": col_id,
                                    "table_name": col.get("table_name", ""),
                                    "column_name": col.get("column_name", "")
                                })
                        
                        state["retrieved_column_ids"] = existing_ids
                        state["retrieved_columns_meta"].extend(new_meta)
                            
                    return result.split("\n\nDetails:")[0] if "\n\nDetails:" in result else result
                except (json.JSONDecodeError, IndexError) as e:
                    if state["log"]: state["log"].info(f"Failed to parse retrieval result: {e}")
                    return str(result)
            elif tool_name == "join_discovery":
                state["validated_joins"].append({
                    "left": f"{args.get('left_table')}.{args.get('left_column')}",
                    "right": f"{args.get('right_table')}.{args.get('right_column')}",
                    "is_valid": "INVALID (Rows = 0 or Error)" not in result
                })

            return str(result)
        except Exception as e:
            return f"[TOOL ERROR] @{tool_name} failed: {str(e)}"

    def _save_artifacts(self, instance_id: str, db_id: str, state: Dict[str, Any]):
        """Сохраняет candidates, messages и tool_logs в конце работы агента."""
        if not self.cache_dir:
            return
    
        candidates_dir = self.cache_dir / "agent_candidates"
        messages_dir = self.cache_dir / "agent_messages"
        tool_log_dir = self.cache_dir / "tool_calls"
        
        for d in [candidates_dir, messages_dir, tool_log_dir]:
            d.mkdir(parents=True, exist_ok=True)
        
        # 1. Candidates
        column_triplets = set(list(zip(
            state.get("retrieved_column_ids", []),
            [m["table_name"] for m in state.get("retrieved_columns_meta", []) if m["table_name"]], 
            [m["column_name"] for m in state.get("retrieved_columns_meta", []) if m["column_name"]]
        )))
        candidates_data = {
            "instance_id": instance_id,
            "db_id": db_id,
            "column_ids": [ct[0] for ct in column_triplets],
            "tables": [ct[1] for ct in column_triplets],
            "columns": [ct[2] for ct in column_triplets],
            "joins": [pair for pair in state["validated_joins"] if pair["is_valid"]]
        }
        (candidates_dir / f"{instance_id}.json").write_text(
            json.dumps(candidates_data, indent=2, ensure_ascii=False)
        )
        
        # 2. Messages Snapshots
        (messages_dir / f"{instance_id}.json").write_text(
            json.dumps(state.get("messages_snapshots", []), indent=2, ensure_ascii=False)
        )
        
        # 3. Tool Calls Log
        (tool_log_dir / f"{instance_id}.json").write_text(
            json.dumps(state.get("tool_step_logs", []), indent=2, ensure_ascii=False)
        )

    def run(
        self, 
        system_prompt: str, 
        user_prompt: str,
        instance_id: str,
        db_name: str,
        dialect: str,
        initial_messages: Optional[List] = None
    ) -> Dict[str, Any]:
        state = {
            "messages": initial_messages or [],
            "turn": 0,
            "draft_count": 0,
            "draft_this_turn": False,
            "stopped": False,
            "retrieved_column_ids": [],
            "retrieved_columns_meta": [],
            "validated_joins": [],
            "log": None,
            "tool_step_logs": [],
            "messages_snapshots": []
        }

        if self.cache_dir:
            state["log"] = get_logger(f"schema_linking {instance_id}", str(self.cache_dir / "agent_events" / f"{instance_id}.log"))
            state["log"].info(f"Agent started for {instance_id}")
        
        initial_content = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ] + state["messages"]
        
        while state["turn"] < self.max_turns and not state["stopped"]:
            state["draft_this_turn"] = False
            
            # 1. Вызов LLM
            if state["log"]: state["log"].info(f"Turn {state['turn']} | Invoke model")
            for attempt_num in range(1, self.retry_config["max_attempts"] + 1):
                try:
                    response = self.model.invoke(initial_content + state["messages"])
                    ai_text = response.content
                    break
                except Exception as e:
                    if attempt_num == self.retry_config["max_attempts"]:
                        ai_text = "[Error] " + str(e)
                        if state["log"]: state["log"].info(f"Turn {state['turn']} | {ai_text}")
                        break
                    
                    delay = min(
                        self.retry_config["initial_delay"] * 
                        (self.retry_config["backoff_multiplier"] ** (attempt_num - 1)),
                        self.retry_config["max_delay"]
                    )
                    time.sleep(delay)

            if state["log"]: state["log"].info(f"Turn {state['turn']} | Model has been invoked")
            
            # 2. Парсинг инструментов
            tool_calls = parse_tool_calls(ai_text)
            if not tool_calls:
                state["messages"].append(AIMessage(content=ai_text))
                state["messages_snapshots"].append(serialize_messages(initial_content + state["messages"]))
                if "@stop()" in ai_text or "ready_for_sql_generation" in ai_text:
                    if state["log"]: state["log"].info(f"Turn {state['turn']} | Agent stopped")
                    break

                if "model's maximum context length" in ai_text and ai_text.startswith("[Error]"):
                    state["stopped"] = True
                    break
                
                if state["log"]: state["log"].info(f"Turn {state['turn']} | No tools. Continue")
                continue
            
            # 3. Выполнение инструментов
            tool_results = []
            tools_log = []
            for tc in tool_calls:
                name, args, raw = tc["name"], tc["args"], tc["raw"]
                if state["log"]: state["log"].info(f"Turn {state['turn']} | Execute tool: {name}")

                allowed, error_msg = self._check_tool_limits(name, state)
                if not allowed:
                    tool_results.append(ToolMessage(content=error_msg, tool_call_id=name))
                    tools_log.append({"tool": name, "raw_call": raw, "result": error_msg})
                    if state["log"]: state["log"].info(f"Turn {state['turn']} | Tool {name} is not allowed to execute")
                    continue
                
                if name == "stop":
                    state["stopped"] = True
                    res = self._execute_tool(name, {}, state)
                    tool_results.append(ToolMessage(content=res, tool_call_id=name))
                    tools_log.append({"tool": name, "raw_call": raw, "result": res})
                    if state["log"]: state["log"].info(f"Turn {state['turn']} | Agent stopped")
                    break
                
                if name == "sql_draft":
                    state["draft_count"] += 1
                    state["draft_this_turn"] = True
                
                args.update(self.config)  # добавляем общие аргументы, помимо возвращаемых моделью
                args["dialect"] = dialect
                args["db_name"] = db_name
                args["db_id"] = f"{dialect}_{db_name}"
                result = self._execute_tool(name, args, state)
                if state["log"]: state["log"].info(f"Turn {state['turn']} | Tool {name} has been executed")
                tool_results.append(ToolMessage(content=result, tool_call_id=name))
                tools_log.append({"tool": name, "raw_call": raw, "result": result})
            
            # 4. Обновление истории и снапшотов
            state["messages"].extend([AIMessage(content=ai_text)] + tool_results)
            state["messages_snapshots"].append(serialize_messages(initial_content + state["messages"]))
            
            ai_messages = sum(isinstance(message, AIMessage) for message in state["messages"])
            if ai_messages > self.max_messages:
                for j in range(1, len(state["messages"])):
                    if isinstance(state["messages"][j], AIMessage):
                        state["messages"] = state["messages"][j:]
                        break

            state["tool_step_logs"].append({"turn": state["turn"], "calls": tools_log})
            state["turn"] += 1
            if state["log"]: state["log"].info(f"Turn {state['turn'] - 1} | Finish")
        
        # При достижении лимита
        if not state["stopped"] and state["turn"] >= self.max_turns:
            state["messages"].append(AIMessage(content="@stop()\n\n[TIMEOUT]"))
            state["stopped"] = True
            if state["log"]: state["log"].info("Timeout reached")
            state["messages_snapshots"].append(serialize_messages(state["messages"]))
            
        # Финальное сохранение артефактов
        self._save_artifacts(instance_id, f"{dialect}_{db_name}", state)
        if state["log"]: state["log"].info("Artifacts has been saved")

        return {
            "final_response": state["messages"][-1].content if state["messages"] else "",
            "state": {k: v for k, v in state.items() if k != "messages"},
            "success": state["stopped"]
        }
    