import re
import json
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

from langchain_core.messages import HumanMessage, AIMessage, ToolMessage, SystemMessage, BaseMessage
from langchain_core.language_models import BaseChatModel

from src.utils.logger import get_logger


TOOL_CALL_PATTERN = re.compile(
    r"@(\w+)\s*\((.+?)\)\s*(?:\n|$)", 
    re.DOTALL
)

def parse_tool_calls(llm_text: str) -> List[Dict[str, Any]]:
    """Извлекает вызовы инструментов из текста ответа LLM."""
    calls = []
    for match in TOOL_CALL_PATTERN.finditer(llm_text):
        name, args_str = match.group(1), match.group(2).strip()
        try:
            cleaned = args_str.replace("'", '"').replace("\n", " ")
            args = json.loads(cleaned)
            calls.append({"name": name, "args": args, "raw": args_str})
        except json.JSONDecodeError:
            calls.append({"name": name, "args": {"_parse_error": True}, "raw": args_str})

    return calls

def serialize_message(msg: BaseMessage) -> Dict[str, Any]:
    """Безопасная сериализация LangChain сообщения в dict."""
    try:
        return msg.model_dump()
    except AttributeError:
        return {"type": msg.type, "content": msg.content}

def serialize_messages(messages: List[BaseMessage]) -> List[Dict[str, Any]]:
    """Сериализация списка сообщений."""
    return [serialize_message(m) for m in messages]


class SchemaLinkingAgent:
    def __init__(
        self, 
        model: BaseChatModel, 
        tools: Dict[str, Any], 
        config: Dict[str, Any],
        cache_dir: Optional[Path] = None
    ):
        self.model = model
        self.tools = tools
        self.config = config
        self.max_turns = config.get("max_turns", 10)
        self.max_draft_calls = config.get("max_draft_calls", 3)
        self.cache_dir = Path(cache_dir) if cache_dir else None
        
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
                                existing_ids.add(col_id)
                                new_meta.append({
                                    "id": col_id,
                                    "table_name": col.get("table_name", ""),
                                    "column_name": col.get("column_name", "")
                                })
                        
                        state["retrieved_column_ids"] = list(existing_ids)
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
    
        candidates_dir = self.cache_dir / "candidates"
        messages_dir = self.cache_dir / "messages"
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
            state["log"] = get_logger("schema_linking", str(self.cache_dir / "events" / f"{instance_id}.log"))
            state["log"].info(f"Agent started for {instance_id}")
        
        initial_content = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ] + state["messages"]
        
        while state["turn"] < self.max_turns and not state["stopped"]:
            state["draft_this_turn"] = False
            
            # 1. Вызов LLM
            if state["log"]: state["log"].info(f"Turn {state['turn']} | Invoke model")
            response = self.model.invoke(initial_content + state["messages"])
            ai_text = response.content
            if state["log"]: state["log"].info(f"Turn {state['turn']} | Model has been invoked")
            
            # 2. Парсинг инструментов
            tool_calls = parse_tool_calls(ai_text)
            if not tool_calls:
                if "@stop()" in ai_text or "ready_for_sql_generation" in ai_text:
                    state["messages"].append(AIMessage(content=ai_text))
                    if state["log"]: state["log"].info(f"Turn {state['turn']} | Agent stopped")
                    break

                state["messages"].append(AIMessage(content=ai_text))
                state["messages_snapshots"].append(serialize_messages(state["messages"]))
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
                result = self._execute_tool(name, args, state)
                if state["log"]: state["log"].info(f"Turn {state['turn']} | Tool {name} has been executed")
                tool_results.append(ToolMessage(content=result, tool_call_id=name))
                tools_log.append({"tool": name, "raw_call": raw, "result": result})
            
            # 4. Обновление истории и снапшотов
            state["messages"].extend([AIMessage(content=ai_text)] + tool_results)
            state["messages_snapshots"].append(serialize_messages(state["messages"]))
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
        
        return {
            "final_response": state["messages"][-1].content if state["messages"] else "",
            "state": {k: v for k, v in state.items() if k != "messages"},
            "success": state["stopped"]
        }
    