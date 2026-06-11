import json
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass, field, asdict

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage
from tqdm import tqdm

from .generate_schema import generate_single_schema
from .schema_formatter import load_schemas, load_similar_tables, format_detailed_block
from src.utils.logger import get_logger
from src.utils.models import get_model
from src.utils.preprocessing import remove_digits, resolve_tasks
from src.utils.run_manager import resolve_run_id


DEFAULT_PROMPT = """You are an expert Database Schema Analyst specializing in fine-grained schema linking for Text-to-SQL systems.

Given a user question and a set of database table schemas, identify the exact tables AND columns required to construct a valid SQL query. Assign each selected column a specific role and provide confidence hints for downstream aggregation.

# INPUT CONTEXT

## User Question
{{USER_QUESTION}}

## External Knowledge
{{EXTERNAL_KNOWLEDGE}}

## Available Database Tables and Schemas
{{TABLE_SCHEMAS}}

---

# INSTRUCTIONS

## Step 1: Table Relevance Filtering
1. Analyze the user question to identify target entities, operations, and domain concepts.
2. For each table in {{TABLE_SCHEMAS}}, evaluate relevance based on:
   - `table_name` and `description` alignment with question terms
   - Presence of columns that could satisfy SELECT, WHERE, JOIN, or aggregation needs
3. Mark tables as `relevant` or `irrelevant`. Include ONLY relevant tables in further processing.
4. If a table is structurally required as a bridge/junction to connect other relevant tables, include it even if not directly mentioned in the question.

## Step 2: Column-to-Intent Mapping
For each relevant table, decompose the question and map intent to columns:
- **Output fields**: What to SELECT → mark columns as `select`
- **Filtering conditions**: WHERE clauses (values, ranges, patterns) → mark as `filter`
- **Aggregation**: GROUP BY keys or aggregated values → mark as `group_by` or `aggregate_source`
- **Sorting**: ORDER BY criteria → mark as `order_by`
- **Joins**: Columns needed to connect tables → mark as `join_key` (PK side) or `join_foreign` (FK side)

Consider:
- Exact lexical matches (column_name contains question terms)
- Semantic alignment (description explains column purpose matching intent)
- Type compatibility (data_type supports required operations)
- Sample values (if provided, check for question literals)

## Step 3: Role Assignment and Confidence
Assign exactly ONE primary role to each selected column:
- `select` | `filter` | `join_key` | `join_foreign` | `group_by` | `order_by` | `aggregate_source`

Estimate confidence:
- `high`: Direct lexical match + clear semantic alignment + type compatibility
- `medium`: Semantic alignment only OR lexical match with ambiguous type
- `low`: Weak semantic hint OR required for structural completeness only

## Strict Constraints
- Table and column names MUST EXACTLY match the input schemas.
- NEVER invent, guess, or modify identifiers.
- Exclude tables and columns that do not directly contribute to answering the question.
- Ignore input order; relevance is determined solely by semantic and structural alignment.
- If a required operation cannot be satisfied by available schema, note it in `blocking_issues`.

---

# OUTPUT FORMAT
Return ONLY a valid JSON object matching the exact structure below. Do NOT use markdown code blocks, do NOT add explanations, and do NOT include trailing commas.

{
  "tables_selected": [
    {
      "table_name": "exact_table_name",
      "relevance_reasoning": "One-sentence justification for including this table"
    }
  ],
  "columns_mapped": [
    {
      "table_name": "exact_table_name",
      "column_name": "exact_column_name",
      "role": "select|filter|join_key|join_foreign|group_by|order_by|aggregate_source",
      "confidence": "high|medium|low",
      "reasoning": "One-sentence justification linking column to question intent",
      "literal_value": "extracted value or pattern from question"
    }
  ],
  "blocking_issues": [
    "Description of any missing column, unresolvable requirement, or ambiguous intent"
  ],
  "analysis_summary": "Brief overview of table filtering rationale, column mapping strategy, and structural assumptions"
}
"""

DEFAULT_RETRY_CONFIG = {
    "max_attempts": 4,
    "initial_delay": 2.0,
    "max_delay": 30.0,
    "backoff_multiplier": 2.0,
}

VALID_ROLES = {"select", "filter", "join_key", "join_foreign", "group_by", "order_by", "aggregate_source"}
VALID_CONFIDENCE = {"high", "medium", "low"}


@dataclass
class ColumnLinkingAttempt:
    """Запись одной попытки связывания столбцов."""
    attempt_number: int
    prompt: str
    llm_response: str
    parsed_result: Optional[Dict[str, Any]]
    validation_errors: List[str]
    success: bool
    timestamp: float = field(default_factory=time.time)
    latency_ms: Optional[float] = None


@dataclass
class ColumnLinkingResult:
    """Итоговый результат связывания с полной историей."""
    instance_id: str
    tables_selected: List[Dict[str, str]]
    columns_mapped: List[Dict[str, Any]]
    column_ids: Optional[List[int]] = None
    blocking_issues: List[str]
    success: bool
    attempts: List[ColumnLinkingAttempt] = field(default_factory=list)
    final_error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "tables_selected": self.tables_selected,
            "columns_mapped": self.columns_mapped,
            "column_ids": self.column_ids,
            "blocking_issues": self.blocking_issues,
            "success": self.success,
            "final_error": self.final_error,
            "attempts": [asdict(a) for a in self.attempts],
            "total_attempts": len(self.attempts),
            "total_latency_ms": sum(a.latency_ms or 0 for a in self.attempts)
        }


class ColumnLinking:
    """
    Алгоритм связывания столбцов через LLM с повторными попытками и валидацией.
    
    Алгоритм:
    1. Формирует промпт со схемой таблиц и вопросом
    2. Отправляет запрос к LLM
    3. Парсит ответ как структурированный JSON
    4. Валидирует имена таблиц/столбцов, роли, уверенность
    5. При ошибке — повторяет с экспоненциальной задержкой
    6. Логирует все попытки в один файл, историю — в отдельный JSON
    """
    
    def __init__(
        self,
        run_id: str,
        model: BaseChatModel,
        tasks: Optional[Union[List[Dict[str, str]], str]] = None,
        run_root: str = "logs/runs",
        input_data_root: str = "Spider2/spider2-lite",
        data_root: str = "data",
        storage_root: str = "storage",
        prompt_name: Optional[str] = None,
        prompt_dir: str = "config/prompts/schema_linking",
        max_schema_length: int = 64000,
        retry_config: Optional[Dict[str, float]] = None,
        max_workers: int = 4,
        max_columns: Optional[int] = None,
        stage: Optional[str] = "column_linking",
        **kwargs
    ):
        """
        Args:
            run_id: Идентификатор запуска
            model: Инициализированная LLM-модель
            tasks: Список задач для обработки
            run_root: Папка с запусками
            input_data_root: Папка с входными данными внутри data_root
            data_root: Корневая папка со всеми входными данными
            storage_root: Папка с метаданными схем
            prompt_name: Название .md файла (без расширения) с промптом пользователя
            prompt_dir: Папка с .md файлами промптов
            max_schema_length: максимальное оценочное число токенов для схемы
            retry_config: Настройки повторных попыток
            max_workers: Число параллельных потоков
            max_columns: Опциональный лимит на число столбцов в результате
        """
        self.model = model
        self.tasks = tasks
        self.input_data_root = input_data_root
        self.data_root = Path(data_root)
        self.storage_root = Path(storage_root)
        self.user_prompt = ((Path(prompt_dir) / f"{prompt_name}.md").read_text(encoding="utf-8") 
                            if prompt_name is not None else DEFAULT_PROMPT)
        self.max_schema_length = max_schema_length
        self.retry_config = {**DEFAULT_RETRY_CONFIG, **(retry_config or {})}
        self.max_workers = max_workers
        self.max_columns = max_columns
        self.stage = stage

        self.log_dir = Path(run_root) / run_id / "schema_linking"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger("column_linking", str(self.log_dir / f"{self.stage}.log"))
        
        self.instances = self._load_instances()

    @property
    def schemas(self):
        if not hasattr(self, '_schemas'):
            self._schemas = load_schemas(str(self.storage_root / self.input_data_root / "schema_cache"))

        return self._schemas

    @property
    def similar_tables(self):
        if not hasattr(self, '_similar_tables'):
            self._similar_tables = load_similar_tables(str(self.storage_root / self.input_data_root / "schema_cache"))

        return self._similar_tables

    def _load_instances(self) -> Dict[str, Any]:
        """Загружает инстансы из кэша или задач."""
        # Загружаем задачи
        tasks = resolve_tasks(self.tasks, self.data_root, self.input_data_root)
        ids_data = {}
        # Пробуем загрузить результаты table_linking
        if (self.log_dir / "table_linking_candidates.json").exists():
            with open(self.log_dir / "table_linking_candidates.json", "r", encoding="utf-8") as f:
                ids_data = json.load(f)
        elif (self.log_dir / "retrieved_indices.json").exists():
            with open(self.log_dir / "retrieved_indices.json", "r", encoding="utf-8") as f:
                ids_data = json.load(f)

        q_key = "question" if "question" in tasks[0] else "instruction"
        tasks_dict = {}
        for instance in tasks:
            iid = instance["instance_id"]
            # Пропускаем уже обработанные
            if (self.log_dir / f"{self.stage}_results" / f"{iid}.json").exists():
                continue

            tasks_dict[iid] = {
                "dialect": instance.get("dialect", ""),
                "db_id": instance.get("db_id", instance.get("db")),
                "question": instance.get(q_key, ""),
                "external_knowledge": str(self.data_root / self.input_data_root / "resource" / "documents" / instance["external_knowledge"])
                    if instance.get("external_knowledge") else None,
                "available_ids": ids_data.get(iid, {}).get("used_indices", [])
            }

        # Если были загружены названия таблиц, добавляем все принадлежащие им столбцы
        if self.input_data_root == "Spider2/spider2-lite":
            inst2dialect = {"sf": "snowflake", "bq": "bigquery", "ga": "bigquery", "local": "sqlite"}
            for iid in tasks_dict:
                db_id = inst2dialect[remove_digits(iid).split("_")[0]] + "_" + tasks_dict[iid]["db_id"]
                tasks_dict[iid]["db_id"] = db_id
                if "used_tables" in ids_data.get(iid, {}):
                    tasks_dict[iid]["available_ids"] = [
                        cid for tn in self.schemas[db_id] 
                        if tn in ids_data[iid].get("used_tables", [])
                        for cid in self.schemas[db_id][tn].keys()                        
                    ]   
        else:
            for iid in tasks_dict:
                db_id = tasks_dict[iid].get("dialect", "") + ("_" if tasks_dict[iid].get("dialect") else "") + tasks_dict[iid]["db_id"]
                tasks_dict[iid]["db_id"] = db_id
                if "used_tables" in ids_data.get(iid, {}):
                    tasks_dict[iid]["available_ids"] = [
                        cid for tn in self.schemas[db_id] 
                        if tn in ids_data[iid].get("used_tables", [])
                        for cid in self.schemas[db_id][tn].keys()                        
                    ]
        
        # Если не найденно индексов, добавляем все имеющиеся
        for iid in tasks_dict:
            if not tasks_dict[iid]["available_ids"]:
                tasks_dict[iid]["available_ids"] = [
                    cid for tn in self.schemas[db_id]
                    for cid in self.schemas[db_id][tn].keys()
                ]
        
        return tasks_dict
    
    def _parse_llm_response(self, response: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Парсит ответ LLM. Поддерживает:
        1. Полный JSON с tables_selected, columns_mapped, ...
        2. Упрощённый список колонок [{"table_name": ..., "column_name": ...}, ...]
        3. Простой список имён ["table.column", ...]
        
        Returns:
            (parsed_dict_or_None, error_message_or_None)
        """
        if not response:
            return None, "Empty response"
        
        # 1. Пробуем найти полный JSON-объект
        json_match = re.search(r'\{[\s\S]*\}', response)
        if json_match:
            try:
                parsed = json.loads(json_match.group(0))
                if isinstance(parsed, dict):
                    # Валидируем обязательные поля
                    if "columns_mapped" in parsed and isinstance(parsed["columns_mapped"], list):
                        # Нормализуем поля
                        for col in parsed["columns_mapped"]:
                            if not col.get("role") or col.get("role") not in VALID_ROLES:
                                col["role"] = "select"  # default
                            if not col.get("confidence") or col.get("confidence") not in VALID_CONFIDENCE:
                                col["confidence"] = "medium"  # default
                            if "literal_value" not in col:
                                col["literal_value"] = None

                        return parsed, None
            except json.JSONDecodeError:
                pass
        
        # 2. Пробуем распарсить как список колонок
        array_match = re.search(r'\[([^\[\]]*?)\]', response, re.DOTALL)
        if array_match:
            try:
                parsed = json.loads(array_match.group(0).replace("'", '"'))
                if isinstance(parsed, list):
                    # Формат: [{"table_name": "t", "column_name": "c"}, ...]
                    if all(isinstance(c, dict) and "column_name" in c for c in parsed):
                        return {
                            "tables_selected": [{"table_name": c.get("table_name"), "relevance_reasoning": "auto-selected"} 
                                                for c in parsed if c.get("table_name")],
                            "columns_mapped": [
                                {**c, "role": c.get("role", "select"), "confidence": c.get("confidence", "medium"), "literal_value": None}
                                for c in parsed
                            ],
                            "blocking_issues": [],
                            "analysis_summary": "Parsed from simplified list format"
                        }, None
                    # Формат: ["table.column", ...]
                    if all(isinstance(c, str) and "." in c for c in parsed):
                        columns = []
                        for item in parsed:
                            parts = item.split(".", 1)
                            if len(parts) == 2:
                                columns.append({
                                    "table_name": parts[0].strip(),
                                    "column_name": parts[1].strip(),
                                    "role": "select",
                                    "confidence": "medium",
                                    "reasoning": "Parsed from table.column format",
                                    "literal_value": None
                                })
                        return {
                            "tables_selected": [{"table_name": c["table_name"], "relevance_reasoning": "auto"} for c in columns],
                            "columns_mapped": columns,
                            "blocking_issues": [],
                            "analysis_summary": "Parsed from table.column list"
                        }, None
            except json.JSONDecodeError:
                pass
        
        # 3. Fallback: парсинг через запятую
        tokens = re.split(r'[,;\n]', response)
        columns = []
        for t in tokens:
            t = t.strip().strip('"\'`')
            if "." in t and len(t.split(".")) == 2:
                tn, cn = t.split(".", 1)
                columns.append({
                    "table_name": tn.strip(),
                    "column_name": cn.strip(),
                    "role": "select",
                    "confidence": "low",
                    "reasoning": "Parsed from comma-separated fallback",
                    "literal_value": None
                })
        
        if columns:
            return {
                "tables_selected": [{"table_name": c["table_name"], "relevance_reasoning": "fallback"} for c in columns],
                "columns_mapped": columns,
                "blocking_issues": [],
                "analysis_summary": "Parsed via fallback comma-split"
            }, None
        
        return None, f"Could not parse structured output from: {response[:300]}"
    
    def _validate_result(self, result: Dict[str, Any], available_columns: Dict[str, List[str]], db_id: str) -> Tuple[List[int], List[str]]:
        """Валидирует результат: имена таблиц/колонок, роли, структура."""
        
        ids = []
        errors = []
        valid_tables = set(available_columns.keys())
        
        # Валидация таблиц
        for tbl in result.get("tables_selected", []):
            tn = tbl.get("table_name")
            if tn and tn not in valid_tables:
                errors.append(f"Table '{tn}' not found in schema")
        
        # Валидация колонок
        for col in result.get("columns_mapped", []):
            tn = col.get("table_name")
            cn = col.get("column_name")
            role = col.get("role")
            conf = col.get("confidence")
            
            if tn not in valid_tables:
                errors.append(f"Column '{tn}.{cn}' references unknown table")
                continue
            
            # Проверяем существование колонки в таблице
            if cn and cn not in available_columns[tn]:
                errors.append(f"Column '{cn}' not found in table '{tn}'")
            
            all_ids = self.schemas.get(db_id, {}).get(tn, {})
            if all_ids:
                for cid, cmeta in all_ids.items():
                    if cmeta["column_name"] == cn:
                        ids.append(cid)
                        break
            
            if role and role not in VALID_ROLES:
                errors.append(f"Invalid role '{role}' for column '{tn}.{cn}'")
            
            if conf and conf not in VALID_CONFIDENCE:
                errors.append(f"Invalid confidence '{conf}' for column '{tn}.{cn}'")
        
        return ids, errors
    
    def _save_message_history(self, instance_id: str, history: List[Dict[str, Any]], result: Dict[str, Any]):
        """Сохраняет историю сообщений в отдельный JSON."""
        history_file = self.log_dir / f"{self.stage}_history" / f"{instance_id}.json"
        history_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(history_file, "w", encoding="utf-8") as f:
            json.dump({
                "instance_id": instance_id,
                "timestamp": time.time(),
                "history": history
            }, f, indent=2, ensure_ascii=False)

        result_file = self.log_dir / f"{self.stage}_results" / f"{instance_id}.json"
        result_file.parent.mkdir(parents=True, exist_ok=True)
        with open(result_file, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
    
    def extract_all_candidates(self):
        with open(self.log_dir / f"{self.stage}_candidates.json", "w", encoding="utf-8") as f:
            data = {}
            for file in (self.log_dir / f"{self.stage}_results").glob("*.json"):
                with open(file, "r", encoding="utf-8") as indf:
                    result = json.load(indf)

                iid = result["instance_id"]
                data[iid] = {
                    "db_id": self.instances[iid]["db_id"],
                    "tables": result.get("tables_selected", []),
                    "columns": [
                        {"table": c.get("table_name", ""), "column": c.get("column_name", ""), "role": c.get("role", "")}
                        for c in result.get("columns_mapped", {})
                    ],
                    "used_indices": result.get("column_ids", [])
                }
            
            json.dump(data, f, indent=2, ensure_ascii=False)
        
    def select_columns(
        self,
        instance_id: str,
        db_id: str,
        user_question: str,
        available_columns: Dict[str, List[str]],
        external_knowledge: Optional[str] = None,
        max_columns: Optional[int] = None
    ) -> ColumnLinkingResult:
        """
        Основной метод: связывает столбцы с вопросом через LLM.
        """
        result = ColumnLinkingResult(
            instance_id=instance_id,
            tables_selected=[],
            columns_mapped=[],
            blocking_issues=[],
            success=False
        )
        
        # Подготовка контекста
        table_schemas, _ = generate_single_schema(
            instance_id=instance_id,
            col_ids=self.instances[instance_id].get("available_ids", []),
            doc_data=self.schemas.get(db_id, {}),
            target_max_tokens=self.max_schema_length,
            block_formatter=format_detailed_block, 
            similar_tables=self.similar_tables.get(db_id, {}), 
            include_samples=False,
            include_descriptions=False, 
            log=self.logger
        )
        # Формирование промпта
        prompt = self.user_prompt.replace("{{USER_QUESTION}}", user_question)
        prompt = prompt.replace("{{TABLE_SCHEMAS}}", table_schemas)
        if external_knowledge and "{{EXTERNAL_KNOWLEDGE}}" in prompt:
            prompt = prompt.replace("{{EXTERNAL_KNOWLEDGE}}", external_knowledge)

        messages_history: List[Dict[str, Any]] = []
        
        for attempt_num in range(1, self.retry_config["max_attempts"] + 1):
            try:
                self.logger.info(f"{instance_id} | Invoke model (attempt {attempt_num})")
                start_time = time.perf_counter()
    
                # Запрос к LLM
                messages = [HumanMessage(content=prompt)]
                response = self.model.invoke(messages)
                llm_response = response.content.strip()
                
                latency_ms = (time.perf_counter() - start_time) * 1000
                self.logger.info(f"{instance_id} | Model invoked | {latency_ms:.0f}ms")
                
                # Парсинг
                parsed, parse_error = self._parse_llm_response(llm_response)
                
                # Валидация
                validation_errors = []
                if parsed:
                    ids, validation_errors = self._validate_result(parsed, available_columns, db_id)
                    
                    # Лимит на число колонок
                    if max_columns and len(parsed.get("columns_mapped", [])) > max_columns:
                        parsed["columns_mapped"] = parsed["columns_mapped"][:max_columns]
                        # validation_errors.append(f"Limited to {max_columns} columns")
                
                # Запись попытки
                attempt = ColumnLinkingAttempt(
                    attempt_number=attempt_num,
                    prompt=prompt,
                    llm_response=llm_response,
                    parsed_result=parsed,
                    validation_errors=validation_errors,
                    success=not validation_errors and bool(parsed and parsed.get("columns_mapped")),
                    latency_ms=latency_ms
                )
                result.attempts.append(attempt)
                
                # История сообщений
                messages_history.append({
                    "attempt": attempt_num,
                    "timestamp": time.time(),
                    "prompt": prompt,
                    "llm_response": llm_response,
                    "parsed_result": parsed,
                    "validation_errors": validation_errors,
                    "success": attempt.success,
                    "latency_ms": latency_ms
                })
                
                # Успех?
                if attempt.success:
                    result.tables_selected = parsed.get("tables_selected", [])
                    result.columns_mapped = parsed.get("columns_mapped", [])
                    result.column_ids = ids
                    result.blocking_issues = parsed.get("blocking_issues", [])
                    result.success = True
                    self.logger.info(
                        f"{instance_id} | Success | Tables: {len(result.tables_selected)} | "
                        f"Columns: {len(result.columns_mapped)} | {latency_ms:.0f}ms"
                    )
                    break
                else:
                    self.logger.warning(
                        f"{instance_id} | Attempt {attempt_num} failed | "
                        f"Errors: {validation_errors or [parse_error]} | {latency_ms:.0f}ms"
                    )
                
                # Задержка перед повтором
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
                attempt = ColumnLinkingAttempt(
                    attempt_number=attempt_num,
                    prompt=prompt,
                    llm_response=f"[ERROR] {str(e)}",
                    parsed_result=None,
                    validation_errors=[f"Request failed: {str(e)}"],
                    success=False,
                    latency_ms=latency_ms
                )
                result.attempts.append(attempt)
                messages_history.append({
                    "attempt": attempt_num,
                    "timestamp": time.time(),
                    "error": str(e),
                    "success": False
                })
                self.logger.exception(f"{instance_id} | Attempt {attempt_num} | Exception: {e}")
                
                if attempt_num < self.retry_config["max_attempts"]:
                    delay = min(
                        self.retry_config["initial_delay"] * 
                        (self.retry_config["backoff_multiplier"] ** (attempt_num - 1)),
                        self.retry_config["max_delay"]
                    )
                    time.sleep(delay)
        
        # Финал
        if not result.success:
            result.final_error = result.attempts[-1].validation_errors[-1] if result.attempts else "Unknown error"
            self.logger.error(
                f"{instance_id} | FAILED after {len(result.attempts)} attempts | "
                f"Error: {result.final_error}"
            )
        
        self._save_message_history(instance_id, messages_history, {k: v for k, v in result.to_dict().items() if k != "attempts"})
        return result
    
    def _process_single_instance(self, instance_id: str, data: Dict[str, Any]) -> ColumnLinkingResult:
        """Обработка одного примера."""
        try:
            db_id = data.get("db_id", instance_id.split("_", 1)[0])
            question = data.get("question", data.get("instruction", ""))
            external_knowledge = None
            if data.get("external_knowledge"):
                try:
                    with open(data["external_knowledge"], "r", encoding="utf-8") as f:
                        external_knowledge = f.read()
                except Exception as e:
                    self.logger.warning(f"{instance_id} | Failed to load external knowledge: {e}")
            
            # Загружаем схему БД
            available_columns = self.schemas.get(db_id, {})
            if not available_columns:
                self.logger.warning(f"{instance_id} | Schema not found for db_id: {db_id}")
                return ColumnLinkingResult(instance_id, [], [], [], False, final_error="Schema not found")
            
            available_ids = data.get("available_ids")
            if not available_ids:
                available_columns = {
                    tn: [available_columns[tn][cid]["column_name"] 
                         for cid in available_columns[tn]] 
                    for tn in available_columns
                }
            else:
                available_columns = {
                    tn: [available_columns[tn][cid]["column_name"] 
                         for cid in available_columns[tn] if cid in available_ids] 
                    for tn in available_columns
                }

            # Добавляем все похожие таблицы для получения полного списка
            if self.similar_tables:
                for tn in self.similar_tables.get(db_id, {}):
                    if tn in available_columns:
                        for stn in self.similar_tables[db_id][tn]:
                            available_columns[stn] = available_columns[tn].copy()

            return self.select_columns(
                instance_id=instance_id,
                db_id=db_id,
                user_question=question,
                available_columns=available_columns,
                external_knowledge=external_knowledge,
                max_columns=self.max_columns
            )
            
        except Exception as e:
            self.logger.exception(f"{instance_id} | Critical error")
            return ColumnLinkingResult(instance_id, [], [], [], False, final_error=str(e))
    
    def run(self) -> Dict[str, Any]:
        """Запускает параллельную обработку."""
        self.logger.info(f"Starting column linking for {len(self.instances)} instances | Workers: {self.max_workers}")
        
        results = {}
        successful = 0
        failed = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._process_single_instance, iid, data): iid 
                for iid, data in self.instances.items()
            }
            
            for future in tqdm(as_completed(futures), total=len(futures), desc="Linking Columns"):
                iid = futures[future]
                try:
                    res = future.result()
                    results[iid] = res.to_dict()
                    if res.success:
                        successful += 1
                    else:
                        failed += 1
                except Exception as e:
                    failed += 1
                    self.logger.exception(f"{iid} | Unhandled exception")
                    results[iid] = ColumnLinkingResult(iid, [], [], [], False, final_error=str(e)).to_dict()
        
        # Статистика
        stats = {
            "total": len(self.instances),
            "successful": successful,
            "failed": failed,
            "success_rate": successful / len(self.instances) if self.instances else 0.0,
            "completed_at": time.time()
        }
        
        stats_path = self.log_dir / f"{self.stage}_stats.json"
        with open(stats_path, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Column linking finished. Success: {successful}/{stats['total']}")
        return results


if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv(".env")

    parser = argparse.ArgumentParser(description="Column-level Schema Linking Pipeline")
    parser.add_argument("input_data_root", type=str, default="Spider2/spider2-lite")
    parser.add_argument("run_name", type=str, default="")
    parser.add_argument("--data-root", type=str, default="data")
    parser.add_argument("--storage-root", type=str, default="storage")
    parser.add_argument("--run-root", type=str, default="logs/runs")
    
    # Model
    parser.add_argument("--model-name", type=str, default="qwen-local")
    parser.add_argument("--base-url", type=str, default=None)
    parser.add_argument("--api-key", type=str, default=None)
    parser.add_argument("--temperature", type=float, default=0.0)
    
    # Pipeline
    parser.add_argument("--prompt-name", type=str, default="sl_column_level")
    parser.add_argument("--prompt-dir", type=str, default="config/prompts/schema_linking")
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--max-columns", type=int, default=None, help="Max columns to return per instance")
    parser.add_argument("--max-attempts", type=int, default=4)
    parser.add_argument("--initial-delay", type=float, default=2.0)
    parser.add_argument("--max-delay", type=float, default=30.0)
    
    args = parser.parse_args()
    
    run_id = resolve_run_id(input_data_root=args.input_data_root, custom_suffix=args.run_name)
    model = get_model(args.model_name, args.base_url, args.api_key, args.temperature)
    
    pipeline = ColumnLinking(
        run_id=run_id,
        model=model,
        run_root=args.run_root,
        input_data_root=args.input_data_root,
        data_root=args.data_root,
        storage_root=args.storage_root,
        prompt_name=args.prompt_name,
        prompt_dir=args.prompt_dir,
        max_workers=args.max_workers,
        max_columns=args.max_columns,
        retry_config={
            "max_attempts": args.max_attempts,
            "initial_delay": args.initial_delay,
            "max_delay": args.max_delay,
            "backoff_multiplier": 2.0
        }
    )
    pipeline.run()
    pipeline.extract_all_candidates()
