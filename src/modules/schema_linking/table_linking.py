import sys
sys.path.insert(0, ".")

import json
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
from .schema_formatter import load_schemas, load_similar_tables, format_compact_block
from src.utils.logger import get_logger
from src.utils.models import get_model
from src.utils.preprocessing import remove_digits, resolve_tasks
from src.utils.run_manager import resolve_run_id


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

External Knowledge:
{{EXTERNAL_KNOWLEDGE}}

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
class TableLinkingAttempt:
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
class TableLinkingResult:
    """Итоговый результат отбора с полной историей."""
    instance_id: str
    selected_tables: List[str]
    success: bool
    attempts: List[TableLinkingAttempt] = field(default_factory=list)
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
        tasks: Optional[Union[List[Dict[str, Any]], str]],
        run_root: str = "logs/runs",
        input_data_root: str = "Spider2/spider2-lite",
        data_root: str = "data",
        storage_root: str = "storage",
        prompt_name: Optional[str] = None,
        prompt_dir: str = "config/prompts/schema_linking",
        max_schema_length: int = 64000,
        retry_config: Optional[Dict[str, float]] = None,
        max_workers: int = 4,
        max_tables: Optional[int] = None,
        stage: Optional[str] = "table_linking",
        **kwargs
    ):
        """
        Args:
            run_id: Идентификатор запуска и название папки в runs_root
            model: Инициализированная LLM-модель (ChatOpenAI и аналоги)
            tasks: Список задач, для которых требуется найти таблицы, либо путь к .jsonl файлу с задачами, иначе загружается первый .jsonl из входных данных
            run_root: Папка с запусками
            input_data_root: Папка с текущими входными данными в data_root
            data_root: Директория со всеми входными данными
            storage_root: Папка с метаданными схем баз данных
            prompt_name: Название .md файла (без расширения) с промптом пользователя
            prompt_dir: Папка с .md файлами промптов
            max_schema_length: максимальное оценочное число токенов для схемы
            retry_config: Настройки повторных попыток
            max_workers: Максимальное число параллельных процессов генерации
            max_tables: Опциональное ограничение числа таблиц в результате
            stage: Префикс папки, в которые будут сохранены промежуточные результаты
        """
        self.model = model
        self.tasks = tasks
        self.input_data_root = input_data_root
        self.data_root = Path(data_root)
        self.storage_root = Path(storage_root)
        self.max_schema_length = max_schema_length
        self.retry_config = {**DEFAULT_RETRY_CONFIG, **(retry_config or {})}
        self.max_workers = max_workers
        self.max_tables = max_tables
        self.stage = stage
        self.user_prompt = ((Path(prompt_dir) / f"{prompt_name}.md").read_text(encoding="utf-8") 
                            if prompt_name is not None else DEFAULT_PROMPT)

        self.log_dir = Path(run_root) / run_id / "schema_linking"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger("table_linking", self.log_dir / f"{self.stage}.log")
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
        ids_data = {}
        # Если есть данные schema linking на уровне столбцов,загружаем их
        if (self.log_dir / "column_linking_candidates.json").exists():
            with open(self.log_dir / "column_candidates.json", "r", encoding="utf-8") as f:
                ids_data = json.load(f)
        
        # Иначе проверяем, если ли результаты векторного (или иного) поиска
        if not ids_data and (self.log_dir / "retrieved_indices.json").exists():
            with open(self.log_dir / "retrieved_indices.json", "r", encoding="utf-8") as f:
                ids_data = json.load(f)
        
        # Загружаем примеры
        tasks = resolve_tasks(self.tasks, self.data_root, self.input_data_root)
        q_key = "question" if "question" in tasks[0] else "instruction"
        tasks = {
            instance["instance_id"]: {
                "dialect": self.instance.get("dialect", ""),
                "db_id": instance.get("db_id", instance.get("db")), 
                "question": instance[q_key],
                "external_knowledge": (str(self.data_root / self.input_data_root / "resource" / "documents" 
                                            / instance["external_knowledge"]) 
                                        if instance.get("external_knowledge") else None),
                "available_ids": ids_data.get(instance["instance_id"], {}).get("used_indices", [])
            } 
            for instance in tasks 
            if not (self.log_dir / f"{self.stage}_results" / f"{instance['instance_id']}.json").exists()
        }
        if self.input_data_root.startswith("Spider2/"):
            inst2dialect = {"sf": "snowflake", "bq": "bigquery", "ga": "bigquery", "local": "sqlite"}
            for iid in tasks:
                tasks[iid]["db_id"] = inst2dialect[remove_digits(iid).split("_")[0]] + "_" + tasks[iid]["db_id"],
        else:
            for iid in tasks:
                tasks[iid]["db_id"] = tasks[iid].get("dialect", "") + ("_" if tasks[iid].get("dialect") else "") + tasks[iid]["db_id"]

        # Если столбцов ранее не было найдено, используем полную схему
        if not hasattr(self, "schemas"): self.schemas = self._load_schemas()
            
        for iid in tasks:
            if not tasks[iid]["available_ids"]:
                tasks[iid]["available_ids"] = [
                    cid for tn in self.schemas[tasks[iid]["db_id"]] 
                    for cid in self.schemas[tasks[iid]["db_id"]][tn].keys()
                ]

        return tasks

    def _format_table_list(self, tables: Dict[str, List[Dict]], max_tables: Optional[int]) -> str:
        """Формирует компактный список таблиц для промпта."""
        names = list(tables.keys())
        if max_tables and len(names) > max_tables:
            names = names[:max_tables]
        return ", ".join(f"`{name}`" for name in names)
        
    def _parse_llm_response(self, response: str) -> Tuple[Optional[List[str]], Optional[str]]:
        """
        Парсит ответ LLM как список имён таблиц.
        
        Returns:
            (parsed_list_or_None, error_message_or_None)
        """
        if not response:
            return None, "Empty response"
        
        json_match = re.search(r'\{[\s\S]*\}', response)
        if json_match:
            try:
                parsed = json.loads(json_match.group(0))
                # Формат: {"selected_tables": [...]}
                if isinstance(parsed, dict) and "selected_tables" in parsed:
                    tables = [
                        t.get("table_name") 
                        for t in parsed["selected_tables"] 
                        if isinstance(t, dict) and t.get("table_name")
                    ]
                    return tables if tables else None, None
                
                # формат массива
                if isinstance(parsed, list) and all(isinstance(t, str) for t in parsed):
                    return parsed, None
            except json.JSONDecodeError:
                pass
        
        # Fallback: ищем массив [...]
        array_match = re.search(r'\[([^\[\]]*?)\]', response, re.DOTALL)
        if array_match:
            try:
                parsed = json.loads(array_match.group(0).replace("'", '"'))
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
    
    def _save_message_history(self, instance_id: str, history: List[Dict[str, Any]], result: Dict[str, Any]):
        """Сохраняет историю сообщений в отдельный JSON-файл."""
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
                    "used_tables": result.get("selected_tables", [])
                }
                
            json.dump(data, f, indent=2, ensure_ascii=False)

    def select_tables(
        self,
        instance_id: str,
        db_id: str,
        user_question: str,
        available_tables: List[str],
        external_knowledge: Optional[str] = None,
        max_tables: Optional[int] = None
    ) -> TableLinkingResult:
        """
        Основной метод: отбирает таблицы через LLM с повторными попытками.
        
        Args:
            instance_id: Уникальный идентификатор примера (для логирования)
            user_question: Вопрос пользователя
            available_tables: [table_name, ...]
            max_tables: Опциональный лимит на число возвращаемых таблиц
            
        Returns:
            TableLinkingResult с историей попыток и результатом
        """
        result = TableLinkingResult(instance_id=instance_id, selected_tables=[], success=False)
        
        # Подготовка контекста для промпта
        table_schemas, _ = generate_single_schema(
            instance_id=instance_id,
            col_ids=self.instances[instance_id].get("available_ids", []),
            doc_data=self.schemas.get(db_id, {}),
            target_max_tokens=self.max_schema_length,
            block_formatter=format_compact_block,
            similar_tables=self.similar_tables[db_id], 
            include_samples=False,
            include_descriptions=False, 
            log=self.logger
        )
        # 1. Формирование промпта
        prompt = self.user_prompt.replace("{{USER_QUESTION}}", user_question)
        prompt = prompt.replace("{{TABLE_SCHEMAS}}", table_schemas)
        if external_knowledge is not None:
            prompt = prompt.replace("{{external_knowledge}}", external_knowledge)

        messages_history: List[Dict[str, str]] = []
        
        for attempt_num in range(1, self.retry_config["max_attempts"] + 1):
            try:
                self.logger.info(f"{instance_id} | Invoke model")
                start_time = time.perf_counter()
            
                # 2. Отправка запроса к LLM
                messages = [HumanMessage(content=prompt)]
                
                response = self.model.invoke(messages)
                llm_response = response.content.strip()
                
                latency_ms = (time.perf_counter() - start_time) * 1000
                self.logger.info(f"{instance_id} | Model has been invoked")
                
                # 3. Парсинг ответа
                parsed_tables, parse_error = self._parse_llm_response(llm_response)
                
                # 4. Валидация имён таблиц
                validation_errors = []
                if parsed_tables:
                    valid_names = available_tables
                    if isinstance(parsed_tables, dict):  # Если результат - JSON объект
                        parsed_tables = [tb["table_name"] for tb in parsed_tables.get("selected_tables", []) 
                                         if tb.get("table_name")]

                    validated_tables = []
                    for name in parsed_tables:
                        if name not in valid_names:
                            validation_errors.append(f"Table '{name}' not found in schema")
                        else:
                            validated_tables.append(name)

                    parsed_tables = validated_tables
                    
                    # Применяем лимит если указан
                    if max_tables and len(parsed_tables) > max_tables:
                        parsed_tables = parsed_tables[:max_tables]
                
                # 5. Формирование записи попытки
                attempt = TableLinkingAttempt(
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
                    "timestamp": time.perf_counter(),
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
                attempt = TableLinkingAttempt(
                    attempt_number=attempt_num,
                    prompt=prompt,
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
        self._save_message_history(instance_id, messages_history, {k: v for k, v in result.to_dict().items() if k != "attempts"})
        
        return result
    
    def _process_single_instance(self, instance_id: str, data: Dict[str, Any]) -> TableLinkingResult:
        try:
            db_id = data.get("db_id", instance_id.split("_", 1)[0])
            question = data.get("question", data.get("instruction", "None"))
            external_knowledge = "None"
            if data.get("external_knowledge") is not None:
                external_knowledge = open(data["external_knowledge"], "r", encoding="utf-8").read()

            available_tables = self.schemas.get(db_id, {})
            available_ids = data.get("available_ids")
            if available_ids is None:
                available_tables = list(available_tables.keys())
            else:
                available_tables = [
                    tn for tn in available_tables 
                    if any(cid in available_ids for cid, _ in available_tables[tn].items())
                ]

            # Добавляем все похожие таблицы для получения полного списка
            if self.similar_tables:
                for tn in self.similar_tables[db_id]:
                    if tn in available_tables:
                        available_tables += self.similar_tables[db_id][tn]
                
                available_tables = list(set(available_tables))

            if not available_tables:
                self.logger.warning(f"No schema for {instance_id} (db_id: {db_id})")
                return TableLinkingResult(instance_id, [], False, final_error="Schema not found")
                
            return self.select_tables(
                instance_id=instance_id,
                db_id=db_id,
                user_question=question,
                external_knowledge=external_knowledge,
                available_tables=available_tables,
                max_tables=self.max_tables
            )
        except Exception as e:
            self.logger.exception(f"Critical error for {instance_id}")
            return TableLinkingResult(instance_id, [], False, final_error=str(e))

    def run(self) -> Dict[str, Any]:
        self.logger.info(f"Starting pipeline for {len(self.instances)} instances | Workers: {self.max_workers}")
        self.schemas = self._load_schemas() if not hasattr(self, "schemas") else self.schemas
        self.instances = self._load_instances()
        results = {}
        successful = 0
        failed = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._process_single_instance, iid, data): iid for iid, data in self.instances.items()}
            
            for future in tqdm(as_completed(futures), total=len(futures), desc="Selecting Tables"):
                iid = futures[future]
                try:
                    res = future.result()
                    results[iid] = res.to_dict()
                    if res.success: successful += 1
                    else: failed += 1
                except Exception as e:
                    failed += 1
                    self.logger.exception(f"Unhandled exception for {iid}")
                    results[iid] = TableLinkingResult(iid, [], False, final_error=str(e)).to_dict()

        stats = {
            "total": len(self.instances),
            "successful": successful,
            "failed": failed,
            "success_rate": successful / len(self.instances) if self.instances else 0.0,
            "completed_at": time.time()
        }
        
        with open(self.log_dir / f"{self.stage}_stats.json", "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
            
        self.logger.info(f"Pipeline finished. Success: {successful}/{stats['total']}")
        return results


if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv(".env")

    parser = argparse.ArgumentParser(description="Parallel LLM-based Table Selection Pipeline")
    parser.add_argument(
        "input_data_root", type=str, default="Spider2/spider2-lite",
        help="Относительный путь к папке датасета внутри data_root. "
             "Используется для получения вопросов."
    )
    parser.add_argument(
        "run_name", type=str, default="", 
        help="Название запуска, использовавшегося для формирования логов в logs/runs директории."
    )
    parser.add_argument(
        "--data_root", type=str, default="data",
        help="Путь к папке с входными данными"
    )
    parser.add_argument(
        "--storage_root", type=str, default="storage",
        help="Корневая директория для кэшированных схем и векторных баз данных."
    )
    
    # Model
    parser.add_argument(
        "--model-name", type=str, default="qwen-local",
        help="Имя модели из config/llm.json (по умолчанию: qwen-local)"
    )
    parser.add_argument(
        "--base-url", type=str, default=None,
        help="Переопределение base_url API"
    )
    parser.add_argument(
        "--api-key", type=str, default=None,
        help="Переопределение API-ключа или имя env-переменной с ключом"
    )
    parser.add_argument(
        "--temperature", type=float, default=0.0,
        help="Температура семплирования (по умолчанию: 0.0 для детерменированности ответов)"
    )
    
    # Pipeline
    parser.add_argument(
        "--prompt-dir", type=str, default="config/prompts/schema_linking",
        help="Путь к директории с шаблонами промптов (по умолчанию: config/prompts/schema_linking)"
    )
    parser.add_argument(
        "--prompt-name", type=str, default="sl_table_level",
        help="Вариант промпта агента в папке prompt-dir (по умолчанию: sl_table_level)"
    )
    parser.add_argument(
        "--max-schema-length", type=str, default=64000,
        help="Максимальное оценочное число токенов для схемы (по умолчанию: 64000)"
    )
    parser.add_argument(
        "--max-workers", type=int, default=4, 
        help="Максимальное число потоков для параллельной генерации для примеров"
    )
    parser.add_argument(
        "--max-tables", type=int, default=None, 
        help="Максимальное число таблиц, возвращаемых моделью"
    )
    parser.add_argument(
        "--max-attempts", type=int, default=4, 
        help="Максимальное число попыток генерации"
    )
    parser.add_argument(
        "--initial-delay", type=float, default=1.0, 
        help="Начальная задержка до следующей попытки генерации"
    )
    parser.add_argument(
        "--max-delay", type=float, default=30.0,
        help="Максимальная задержка перед попыткой генерации"
    )
    args = parser.parse_args()
    
    run_id = resolve_run_id(input_data_root=args.input_data_root, custom_suffix=args.run_name)
    model = get_model(args.model_name, args.base_url, args.api_key, args.temperature)
    pipeline = TableLinking(
        run_id=run_id,
        model=model,
        run_root=args.run_root,
        input_data_root=args.input_data_root,
        data_root=args.data_root,
        storage_root=args.storage_root,
        prompt_name=args.prompt_name,
        prompt_dir=args.prompt_dir,
        max_schema_length=args.max_schema_length,
        max_workers=args.max_workers,
        max_tables=args.max_tables,
        retry_config={
            "max_attempts": args.max_attempts,
            "initial_delay": args.initial_delay,
            "max_delay": args.max_delay,
            "backoff_multiplier": 2.0
        }
    )
    pipeline.run()
    pipeline.extract_all_candidates()
