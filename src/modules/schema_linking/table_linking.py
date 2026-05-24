import json
import random
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, AIMessage, BaseMessage

from src.utils.logger import get_logger


DEFAULT_PROMPT = """You are a database schema expert. 
Given a user question and a list of available tables with their columns, 
select ONLY the tables that are necessary to answer the question.

Rules:
1. Return ONLY a JSON array of table names, e.g., ["orders", "customers"]
2. Do NOT include tables that are not directly needed
3. Do NOT add explanations, comments, or markdown formatting
4. Table names must match EXACTLY as provided in the schema

User Question:
{{USER_QUESTION}}

Table schemas:
{{TABLE_SCHEMAS}}
"""

DEFAULT_RETRY_CONFIG = {
    "max_attempts": 4,
    "initial_delay": 2.0,
    "max_delay": 30.0,
    "backoff_multiplier": 2.0,
}


@dataclass
class TableSelectionAttempt:
    """Запись одной попытки отбора таблиц."""
    attempt_number: int
    prompt: str
    llm_response: str
    parsed_tables: Optional[List[str]]
    validation_errors: List[str]
    success: bool
    timestamp: float = field(default_factory=time.time)
    latency_ms: Optional[float] = None


@dataclass
class TableSelectionResult:
    """Итоговый результат отбора с полной историей."""
    instance_id: str
    selected_tables: List[str]
    success: bool
    attempts: List[TableSelectionAttempt] = field(default_factory=list)
    final_error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "selected_tables": self.selected_tables,
            "success": self.success,
            "final_error": self.final_error,
            "attempts": [asdict(a) for a in self.attempts],
            "total_attempts": len(self.attempts),
            "total_latency_ms": sum(a.latency_ms or 0 for a in self.attempts)
        }

class TableLinking:
    """
    Простой селектор таблиц через LLM с повторными попытками и валидацией.
    
    Алгоритм:
    1. Формирует промпт со списком таблиц и схемой
    2. Отправляет запрос к LLM
    3. Парсит ответ как JSON-список имён таблиц
    4. Проверяет, что все имена существуют в схеме
    5. При ошибке — повторяет с экспоненциальной задержкой
    6. Логирует все попытки в один файл, историю сообщений — в отдельный JSON
    """
    
    def __init__(
        self,
        run_id: str,
        model: BaseChatModel,
        run_root: str = "logs/runs",
        input_data_root: str = "Spider2/spider2-lite",
        storage_root: str = "storage",
        user_prompt: Optional[str] = None,
        retry_config: Optional[Dict[str, float]] = None,
        require_json_output: bool = True,
        max_workers: int = 4,
        max_tables: Optional[int] = None
    ):
        """
        Args:
            run_id: Идентификатор запуска и название папки в runs_root
            model: Инициализированная LLM-модель (ChatOpenAI и аналоги)
            run_root: Папка с запусками
            input_data_root: Папка с входными данными
            storage_root: Папка с метаданными схем баз данных
            user_prompt: Кастомный системный промпт (опционально)
            retry_config: Настройки повторных попыток
            require_json_output: Требовать строгого JSON-вывода от LLM
            max_workers: Максимальное число параллельных процессов генерации
            max_tables: Опциональное ограничение числа таблиц в результате
        """
        self.model = model
        self.input_data_root = input_data_root
        self.storage_root = storage_root
        self.user_prompt = user_prompt or self.DEFAULT_PROMPT
        self.retry_config = {**self.DEFAULT_RETRY_CONFIG, **(retry_config or {})}
        self.require_json_output = require_json_output
        self.max_workers = max_workers
        self.max_tables = max_tables

        self.log_dir = Path(run_root) / run_id / "schema_linking"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger("table_linking", self.log_dir / "table_selection.log")
    
    def select_tables(
        self,
        instance_id: str,
        user_question: str,
        available_tables: Dict[str, List[Dict[str, Any]]],
        max_tables: Optional[int] = None
    ) -> TableSelectionResult:
        """
        Основной метод: отбирает таблицы через LLM с повторными попытками.
        
        Args:
            instance_id: Уникальный идентификатор примера (для логирования)
            user_question: Вопрос пользователя
            available_tables: {table_name: [{"column": str, "type": str}, ...]}
            max_tables: Опциональный лимит на число возвращаемых таблиц
            
        Returns:
            TableSelectionResult с историей попыток и результатом
        """
        result = TableSelectionResult(instance_id=instance_id, selected_tables=[], success=False)
        
        # Подготовка контекста для промпта
        table_schemas = self._format_table_schemas(available_tables)
        messages_history: List[Dict[str, str]] = []
        
        for attempt_num in range(1, self.retry_config["max_attempts"] + 1):
            try:
                self.logger.info(f"{instance_id} | Call model")
                start_time = time.perf_counter()
                
                # 1. Формирование промпта
                prompt = self.user_prompt.replace("{{USER_QUESTION}}", user_question)
                prompt = self.user_prompt.replace("{{TABLE_SCHEMAS}}", table_schemas)
            
                # 2. Отправка запроса к LLM
                messages = [HumanMessage(content=prompt)]
                
                response = self.model.invoke(messages)
                llm_response = response.content.strip()
                
                latency_ms = (time.perf_counter() - start_time) * 1000
                self.logger.info(f"{instance_id} | Model has been called successfully")
                
                # 3. Парсинг ответа
                parsed_tables, parse_error = self._parse_llm_response(llm_response)
                
                # 4. Валидация имён таблиц
                validation_errors = []
                if parsed_tables:
                    valid_names = set(available_tables.keys())
                    for name in parsed_tables:
                        if name not in valid_names:
                            validation_errors.append(f"Table '{name}' not found in schema")
                    
                    # Применяем лимит если указан
                    if max_tables and len(parsed_tables) > max_tables:
                        parsed_tables = parsed_tables[:max_tables]
                
                # 5. Формирование записи попытки
                attempt = TableSelectionAttempt(
                    attempt_number=attempt_num,
                    prompt=prompt,
                    llm_response=llm_response,
                    parsed_tables=parsed_tables,
                    validation_errors=validation_errors,
                    success=len(validation_errors) == 0 and bool(parsed_tables),
                    latency_ms=latency_ms
                )
                result.attempts.append(attempt)
                
                # 6. Сохранение истории сообщений
                messages_history.append({
                    "attempt": attempt_num,
                    "timestamp": time.time(),
                    "user_message": prompt,
                    "llm_response": llm_response,
                    "parsed_tables": parsed_tables,
                    "validation_errors": validation_errors,
                    "success": attempt.success,
                    "latency_ms": latency_ms
                })
                
                # 7. Проверка успеха
                if attempt.success:
                    result.selected_tables = parsed_tables or []
                    result.success = True
                    self.logger.info(
                        f"{instance_id} | Attempt {attempt_num} | "
                        f"Tables: {result.selected_tables} | {latency_ms:.0f}ms"
                    )
                    break
                else:
                    self.logger.warning(
                        f"{instance_id} | Attempt {attempt_num} | "
                        f"Errors: {validation_errors or [parse_error]} | {latency_ms:.0f}ms"
                    )
                
                # 8. Экспоненциальная задержка перед следующей попыткой
                if attempt_num < self.retry_config["max_attempts"]:
                    delay = min(
                        self.retry_config["initial_delay"] * 
                        (self.retry_config["backoff_multiplier"] ** (attempt_num - 1)),
                        self.retry_config["max_delay"]
                    )
                    self.logger.info(f"{instance_id} | Waiting {delay:.1f}s before retry...")
                    time.sleep(delay)
                    
            except Exception as e:
                latency_ms = (time.perf_counter() - start_time) * 1000 if 'start_time' in locals() else None
                attempt = TableSelectionAttempt(
                    attempt_number=attempt_num,
                    prompt=prompt if 'prompt' in locals() else "",
                    llm_response=f"[ERROR] {str(e)}",
                    parsed_tables=None,
                    validation_errors=[f"Request failed: {str(e)}"],
                    success=False,
                    latency_ms=latency_ms
                )
                result.attempts.append(attempt)
                
                messages_history.append({
                    "attempt": attempt_num,
                    "timestamp": time.perf_counter(),
                    "error": str(e),
                    "success": False
                })
                
                self.logger.exception(
                    f"{instance_id} | Attempt {attempt_num} | Exception: {e}"
                )
                
                if attempt_num < self.retry_config["max_attempts"]:
                    delay = min(
                        self.retry_config["initial_delay"] * 
                        (self.retry_config["backoff_multiplier"] ** (attempt_num - 1)),
                        self.retry_config["max_delay"]
                    )
                    time.sleep(delay)
        
        # Финальная запись в лог
        if not result.success:
            result.final_error = result.attempts[-1].validation_errors[-1] if result.attempts else "Unknown error"
            self.logger.error(
                f"{instance_id} | FAILED after {len(result.attempts)} attempts | "
                f"Error: {result.final_error}"
            )
        
        # Сохранение истории сообщений
        self._save_message_history(instance_id, messages_history)
        
        return result
    
    def _format_table_list(self, tables: Dict[str, List[Dict]], max_tables: Optional[int]) -> str:
        """Формирует компактный список таблиц для промпта."""
        names = list(tables.keys())
        if max_tables and len(names) > max_tables:
            names = names[:max_tables]
        return ", ".join(f"`{name}`" for name in names)
    
    def _format_table_schemas(self, tables: Dict[str, List[Dict]]) -> str:
        """Формирует описание схем таблиц для промпта."""
        lines = []
        for table_name, columns in tables.items():
            cols_str = ", ".join(f"{c['column']}:{c.get('type', '?')}" for c in columns)
            lines.append(f"{table_name}: [{cols_str}]")
        
        random.shuffle(lines)
        return "\n".join(lines)
    
    def _parse_llm_response(self, response: str) -> Tuple[Optional[List[str]], Optional[str]]:
        """
        Парсит ответ LLM как список имён таблиц.
        
        Returns:
            (parsed_list_or_None, error_message_or_None)
        """
        if not response:
            return None, "Empty response"
        
        # Ищем паттерн ["table1", "table2"] или ['table1', 'table2']
        json_match = re.search(r'\[([^\[\]]*?)\]', response, re.DOTALL)
        if json_match:
            json_str = json_match.group(0)
            json_str = json_str.replace("'", '"')
            try:
                parsed = json.loads(json_str)
                if isinstance(parsed, list) and all(isinstance(t, str) for t in parsed):
                    return parsed, None
            except json.JSONDecodeError:
                pass
        
        # Fallback: если ответ — просто список через запятую
        if ',' in response or '\n' in response:
            # Разбиваем по запятым, переносам, точкам с запятой
            tokens = re.split(r'[,;\n]', response)
            tables = [t.strip().strip('"\'`') for t in tokens if t.strip()]
            if tables:
                return tables, None
        
        # Если ничего не получилось
        return None, f"Could not parse table list from: {response[:200]}"
    
    def _save_message_history(self, instance_id: str, history: List[Dict[str, Any]]):
        """Сохраняет историю сообщений в отдельный JSON-файл."""
        history_file = self.log_dir / "messages" / f"{instance_id}.json"
        history_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(history_file, "w", encoding="utf-8") as f:
            json.dump({
                "instance_id": instance_id,
                "timestamp": time.time(),
                "history": history
            }, f, indent=2, ensure_ascii=False)
    

