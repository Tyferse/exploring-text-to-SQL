import re
import json
from typing import List, Dict, Any, Optional
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage, SystemMessage
from langchain_core.language_models import BaseChatModel


TOOL_CALL_PATTERN = re.compile(
    r"@(\w+)\s*\((.+?)\)\s*(?:\n|$)", 
    re.DOTALL
)

def parse_tool_calls(llm_text: str) -> List[Dict[str, Any]]:
    """
    Извлекает вызовы инструментов из текста ответа LLM.
    Возвращает список {name, args_raw, args_parsed}.
    """
    calls = []
    for match in TOOL_CALL_PATTERN.finditer(llm_text):
        name, args_str = match.group(1), match.group(2).strip()
        try:
            # Попытка распарсить аргументы как JSON
            # Предварительная очистка: замена одинарных кавычек на двойные
            cleaned = args_str.replace("'", '"').replace("\n", " ")
            args = json.loads(cleaned)
            calls.append({"name": name, "args": args, "raw": args_str})
        except json.JSONDecodeError:
            # Если JSON не валиден, сохраняем как строку для отладки
            calls.append({"name": name, "args": {"_parse_error": True, "raw": args_str}, "raw": args_str})
    return calls

class SchemaLinkingAgent:
    """
    Цикл агента с поддержкой двухчастного промпта и динамического набора инструментов.
    """
    
    def __init__(
        self, 
        model: BaseChatModel, 
        tools: Dict[str, Any], 
        config: Dict[str, Any]
    ):
        self.model = model
        self.tools = tools
        self.config = config
        self.max_turns = config.get("max_turns", 10)
        self.max_draft_calls = config.get("max_draft_calls", 3)
    
    def _check_tool_limits(self, tool_name: str, state: Dict[str, Any]) -> Tuple[bool, str]:
        """Проверяет лимиты вызовов инструментов. Возвращает (можно_вызвать, сообщение_ошибки)."""
        if tool_name == "sql_draft":
            if state.get("draft_count", 0) >= self.max_draft_calls:
                return False, f"[LIMIT] Max @sql_draft calls ({self.max_draft_calls}) reached."
            if state.get("draft_this_turn", False):
                return False, "[LIMIT] Only one @sql_draft call per turn allowed."
        return True, ""
    
    def _execute_tool(self, tool_name: str, args: Dict[str, Any]) -> str:
        """Выполняет инструмент и возвращает строковый результат."""
        tool_fn = self.tools.get(tool_name)
        if not tool_fn:
            return f"[ERROR] Tool @{tool_name} is not enabled in this experiment."
        try:
            # Для LangChain tools: invoke принимает dict аргументов
            result = tool_fn.invoke(args) if hasattr(tool_fn, "invoke") else tool_fn(**args)
            return str(result)
        except Exception as e:
            return f"[TOOL ERROR] @{tool_name} failed: {str(e)}"
    
    def run(
        self, 
        system_prompt: str, 
        user_prompt: str,
        initial_messages: Optional[List] = None
    ) -> Dict[str, Any]:
        """
        Запускает цикл агента.
        
        Args:
            system_prompt: Общий системный промпт (роль, алгоритм, правила)
            user_prompt: Контекстный промпт (вопрос, схема, ограничения случая)
            initial_messages: Опциональная история диалога для multi-turn сценариев
        
        Returns:
            Dict с финальным ответом и метаданными выполнения
        """
        # Инициализация состояния
        state = {
            "messages": initial_messages or [],
            "turn": 0,
            "draft_count": 0,
            "draft_this_turn": False,
            "stopped": False,
            "tool_history": []
        }
        
        # Формируем начальные сообщения: System + User Input
        initial_content = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ] + state["messages"]
        
        while state["turn"] < self.max_turns and not state["stopped"]:
            # Сброс флага драфта на новый ход
            state["draft_this_turn"] = False
            
            # 1. Вызов LLM
            response = self.model.invoke(initial_content + state["messages"])
            ai_text = response.content
            
            # 2. Парсинг инструментов
            tool_calls = parse_tool_calls(ai_text)
            
            # Если инструментов нет — проверяем, не финальный ли это JSON
            if not tool_calls:
                if "@stop()" in ai_text or "ready_for_sql_generation" in ai_text:
                    state["messages"].append(AIMessage(content=ai_text))
                    break
                state["messages"].append(AIMessage(content=ai_text))
                continue
            
            # 3. Выполнение инструментов
            tool_results = []
            for tc in tool_calls:
                name, args = tc["name"], tc["args"]
                
                # Проверка лимитов
                allowed, error_msg = self._check_tool_limits(name, state)
                if not allowed:
                    tool_results.append(ToolMessage(content=error_msg, tool_call_id=name))
                    continue
                
                # Специальная логика для @stop
                if name == "stop":
                    state["stopped"] = True
                    tool_results.append(ToolMessage(content=self._execute_tool(name, {}), tool_call_id=name))
                    break
                
                # Специальная логика для @sql_draft (учёт лимитов)
                if name == "sql_draft":
                    state["draft_count"] += 1
                    state["draft_this_turn"] = True
                
                # Выполнение и сбор результата
                result = self._execute_tool(name, args)
                tool_results.append(ToolMessage(content=result, tool_call_id=name))
                
                # Логирование для отладки
                state["tool_history"].append({"turn": state["turn"], "tool": name, "args": args, "result": result})
            
            # Добавляем ответ LLM и результаты инструментов в историю
            state["messages"].extend([AIMessage(content=ai_text)] + tool_results)
            state["turn"] += 1
        
        # Пост-обработка: если лимит исчерпан без @stop, форсируем завершение
        if not state["stopped"] and state["turn"] >= self.max_turns:
            timeout_msg = AIMessage(content=f"@stop()\n\n[TIMEOUT] Max turns ({self.max_turns}) reached.")
            state["messages"].append(timeout_msg)
            state["stopped"] = True
        
        return {
            "final_response": state["messages"][-1].content if state["messages"] else "",
            "state": state,
            "success": state["stopped"]
        }
