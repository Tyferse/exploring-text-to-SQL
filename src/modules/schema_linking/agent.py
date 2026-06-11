import sys
sys.path.insert(0, ".")

import json
import os
import random
from collections import OrderedDict
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Optional, Any, List, Union

from langchain.chat_models import BaseChatModel
from tqdm import tqdm

from .agent_postprocessor import parse_and_validate_output, format_for_downstream
from .agent_preprocessor import SchemaLinkingPreprocessor
from .agent_loop import SchemaLinkingAgent
from .tools import get_enabled_tools, TOOL_REGISTRY
from .schema_formatter import load_similar_tables
from src.storage.vector_manager import VectorStoreManager
from src.utils.logger import get_logger
from src.utils.models import get_model
from src.utils.preprocessing import resolve_tasks, remove_digits
from src.utils.run_manager import resolve_run_id
from src.utils.sql_execution import SQLExecutor, parse_dialect_path_pair


class SchemaLinkingAgentPipeline:
    def __init__(
        self,
        run_id: str,
        model: BaseChatModel,
        vsm: VectorStoreManager,
        executor: SQLExecutor,
        tasks: Optional[Union[List[Dict[str, Any]], str]] = None,
        run_root: str = "logs/runs",
        input_data_root: str = "Spider2/spider2-lite",
        data_root: str = "data",
        storage_root: str = "storage",
        prompt_name: str = "sl_explore_validation_agent",
        prompt_dir: str = "config/prompts/schema_linking",
        max_turns: int = 10,
        max_draft_calls: Optional[int] = 3,
        additional_k: int = 5,
        retry_config: Optional[Dict[str, Any]] = None,
        max_workers: int = 4,
        **kwargs
    ):
        self.run_id = run_id
        self.run_path = Path(run_root) / run_id
        self.data_root = Path(data_root)
        self.storage_root = Path(storage_root)
        self.tasks = tasks
        self.prompt_path = Path(prompt_dir)
        self.input_data_root = input_data_root
        self.max_workers = max_workers
        self.config = {
            "max_turns": max_turns, 
            "max_draft_calls": max_draft_calls, 
            "additional_k": additional_k, 
            "input_data_root": input_data_root, 
            "vsm": vsm, 
            "executor": executor
        }
              
        self.dialect_rules = self._load_dialect_rules()
        self.similar_tables = load_similar_tables(str(self.storage_root / self.input_data_root / "schema_cache"))
        self.logger = get_logger("sl_agent", str(self.run_path / "schema_linking" / "agent.log"))
        self.preprocessor = SchemaLinkingPreprocessor(
            prompt_name=prompt_name,
            base_dir=prompt_dir,
            logger=self.logger
        )
    
        enabled_tools_names = list(TOOL_REGISTRY.keys())
        if not prompt_name.startswith("sl_explore_validation_agent"):
            if prompt_name.startswith("sl_explore_agent"):
                enabled_tools_names.remove("sql_draft")
            elif prompt_name.startswith("sl_validation_agent"):
                enabled_tools_names.remove("schema_exploration")
            elif prompt_name.startswith("sl_agent"):
                enabled_tools_names.remove("schema_exploration")
                enabled_tools_names.remove("sql_draft")

        enabled_tools = get_enabled_tools(enabled_tools_names)
        self.agent = SchemaLinkingAgent(
            model=model,
            tools=enabled_tools,
            config=self.config,
            cache_dir=self.run_path / "schema_linking",
            retry_config=retry_config
        )
        self.logger.info(f"LLM Initialized: {model}")

    def _load_dialect_rules(self) -> Optional[Dict[str, str]]:
        """Загружает правила для конкретного диалекта."""
        dialects_path = self.prompt_path / "dialects"
        if dialects_path.exists():
            dialects = {str(file).split("_", 1)[0] for file in dialects_path.iterdir()}
            return {{
                    "rules": (dialects_path / f"{dialect}_rules.txt").read_text(encoding="utf-8"),
                    "specifics": (dialects_path / f"{dialect}_specifics.txt").read_text(encoding="utf-8")
                } for dialect in dialects 
                if (dialects_path / f"{dialect}_rules.txt").exists() 
                   and (dialects_path / f"{dialect}_specifics.txt").exists()
            }
        
        return None

    def _load_instances(self) -> Dict[str, Any]:
        """Загружает список примеров и их метаданные."""
        # Загружаем примеры
        tasks = resolve_tasks(self.tasks, self.data_root, self.input_data_root)
        tasks = {t["instance_id"]: {k: v for k, v in t.items() if k != "instance_id"} for t in tasks}

        # Загружаем найденные индексы
        indices_path = self.run_path / "schema_linking" / "retrieved_indices.json"
        if indices_path.exists():
            with open(indices_path, "r", encoding="utf-8") as f:
                used_indices = json.load(f)
        else:
            inst2dialect = {"sf": "snowflake", "bq": "bigquery", "ga": "bigquery", "local": "sqlite"}
            if self.input_data_root == "Spider2/spider2-lite":
                used_indices = {iid: {
                    "db_id": inst2dialect[remove_digits(tasks[iid]["instance_id"]).split("_")[0]] + "_" + tasks[iid].get("db_id", tasks[iid].get("db")),
                    "used_indices": []
                } 
                    for iid in tasks}
            else:
                used_indices = {iid: {
                    "db_id": (tasks[iid].get("dialect", "") + ("_" if tasks[iid].get("dialect") else "")
                              + tasks[iid].get("db_id", tasks[iid].get("db"))), 
                    "used_indices": []
                } 
                    for iid in tasks}

        
        # Удаляем примеры, если они уже обработаны
        for file in (self.run_path / "schema_linking" / "agent_candidates").iterdir():
            instance_id = file.rsplit(".", 1)[0]
            if instance_id in used_indices:
                del used_indices[instance_id]
        
        db_docs = {}
        docs_path = self.storage_root / self.input_data_root / "schema_cache"
        for doc_file in docs_path.glob("*_docs.json"):
            db_id = doc_file.stem.replace("_docs", "")
            with open(doc_file, "r", encoding="utf-8") as f:
                docs_data = json.load(f)
                db_docs[db_id] = {col["id"]: {key: col[key] for key in col if key != "id"} for col in docs_data}
            
        # Удаляем примеры, для которых больше не требуется добавлять столбцов
        for instance_id in list(used_indices.keys()):
            db_id = used_indices[instance_id]["db_id"]
            cand_file = self.agent.cache_dir / "agent_candidates" / f"{instance_id}.json"
            if len(used_indices[instance_id]["used_indices"]) == len(db_docs[db_id]) and not cand_file.exists():
                cand_data = {
                    "instance_id": instance_id,
                    "db_id": db_id,
                    "column_ids": [],
                    "tables": [],
                    "columns": [],
                    "joins": []
                }
                cand_file.write_text(json.dumps(cand_data, indent=2, ensure_ascii=False), encoding="utf-8")
                del used_indices[instance_id]

        # Дабавляем недостающие метаданные
        for instance_id in list(used_indices.keys()):
            db_id = used_indices[instance_id]["db_id"]
            used_indices[instance_id]["question"] = tasks[instance_id].get("question", tasks[instance_id].get("instruction"))
            
            all_tables = []
            for cid in db_docs[db_id]:
                tn = db_docs[db_id][cid]['metadata']["table_name"]
                for stn in [tn] + self.similar_tables[db_id].get(tn, []):
                    all_tables.append(stn)

            used_indices[instance_id]["all_tables"] = all_tables

            table_schemas = {
                table_name: [
                    {"column": db_docs[db_id][cid]['metadata']["column_name"], 
                     "type": db_docs[db_id][cid]['metadata']["column_type"]}
                    for cid in db_docs[db_id]
                    if db_docs[db_id][cid]['metadata']["table_name"] == table_name
                ]
                for table_name in all_tables
            }
            table_schemas = list(table_schemas.items())
            random.shuffle(table_schemas)  # Перемешиваем для предотвращения влияния порядка таблиц на ответ модели
            used_indices[instance_id]["table_schemas"] = json.dumps(
                OrderedDict(table_schemas), indent=2, ensure_ascii=False
            )
            used_indices[instance_id]["external_knowledge"] = str(
                self.data_root / self.input_data_root / "resource" / "documents" 
                / tasks[instance_id]["external_knowledge"]
            ) if tasks[instance_id]["external_knowledge"] else None
        
        return used_indices
    
    def extract_all_candidates(self):
        cand_dir = self.agent.cache_dir / "agent_candidates"
        agent_candidates = {}
        for file in cand_dir.iterdir():
            instance_id = file.stem
            with open(cand_dir / file, "r", encoding="utf-8") as f:
                cand_data = json.load(f)
            
            agent_candidates[instance_id] = {"db_id": cand_data["db_id"], "used_indices": cand_data["column_ids"]}

        with open(self.agent.cache_dir / "agent_candidates.json", "w", encoding="utf-8") as f:
            json.dump(agent_candidates, f)

    def _process_single_instance(self, instance_id: str, instance_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Обрабатывает один пример: от препроцессинга до постпроцессинга.
        """
        try:
            db_id = instance_data.get("db_id", instance_id.split("_", 1)[0])
            dialect, db_name = db_id.split("_", 1)
            external_knowledge = "None"
            if instance_data.get("external_knowledge") is not None:
                external_knowledge = open(instance_data["external_knowledge"], "r", encoding="utf-8").read()

            schema_dir = self.agent.cache_dir / "initial_schema"
            
            # 1. Предобработка
            context = {
                "USER_QUESTION": instance_data.get("question", "None"),
                # "RETRIEVED_SCHEMA": None,  # будет загружено в preprocessor.build_messages
                "ALL_TABLES": str(instance_data.get("all_tables", [])),
                "TABLE_SCHEMAS": instance_data.get("table_schemas", "None"),
                "EXTERNAL_KNOWLEDGE": external_knowledge
            }
            
            system_prompt, user_prompt = self.preprocessor.build_messages(
                instance_id=instance_id,
                context=context,
                schema_dir=schema_dir,
                dialect_rules=self.dialect_rules[dialect]
            )
            
            # 2. Исполнение
            result = self.agent.run(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                instance_id=instance_id,
                db_name=db_name,
                dialect=dialect
            )
            
            # 3. Постобработка
            if result["success"]:
                try:
                    parsed_output = parse_and_validate_output(result["final_response"])
                    final_output = format_for_downstream(parsed_output, result)
                    return {
                        "instance_id": instance_id,
                        "status": "success",
                        "data": final_output,
                        "metadata": result["state"]
                    }
                except Exception as e:
                    self.logger.warning(f"Postprocessing failed for {instance_id}: {e}")
                    return {
                        "instance_id": instance_id,
                        "status": "parse_error",
                        "error": str(e),
                        "raw_response": result["final_response"]
                    }
            else:
                return {
                    "instance_id": instance_id,
                    "status": "agent_failed",
                    "error": "Agent did not stop successfully or timeout reached",
                    "metadata": result["state"]
                }
                
        except Exception as e:
            self.logger.exception(f"Critical error processing {instance_id}")
            return {
                "instance_id": instance_id,
                "status": "critical_error",
                "error": str(e)
            }

    def run(self):
        """Запускает параллельную обработку всех примеров."""
        instances = self._load_instances()
        total = len(instances)
        
        self.logger.info(f"Starting pipeline for {total} instances with {self.max_workers} workers.")
        
        results = {}
        successful = 0
        failed = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_id = {
                executor.submit(self._process_single_instance, iid, idata): iid 
                for iid, idata in instances.items()
            }
            
            for future in tqdm(as_completed(future_to_id), total=total, desc="Processing Instances"):
                iid = future_to_id[future]
                try:
                    result = future.result()
                    results[iid] = result
                    
                    if result["status"] == "success":
                        successful += 1
                    else:
                        failed += 1
                        self.logger.warning(f"Instance {iid} failed with status: {result['status']}")
                        
                except Exception as e:
                    failed += 1
                    self.logger.exception(f"Unhandled exception for {iid}")
                    results[iid] = {
                        "instance_id": iid,
                        "status": "exception",
                        "error": str(e)
                    }

        # Сохранение сводной статистики
        stats = {
            "total": total,
            "successful": successful,
            "failed": failed,
            "success_rate": successful / total if total > 0 else 0
        }
        
        stats_path = self.run_path / "schema_linking" / "agent_pipeline_stats.json"
        stats_path.parent.mkdir(parents=True, exist_ok=True)
        with open(stats_path, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)

        self.logger.info(f"Pipeline finished. Success: {successful}/{total}")
        return results
    

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(".env")

    import argparse

    parser = argparse.ArgumentParser(description="Запуск schema linking агента для дополнения итоговой схемы БД")
    parser.add_argument(
        "input-data-root", type=str, default="Spider2/spider2-lite",
        help="Относительный путь к папке датасета внутри data_root. "
             "Используется для получения вопросов."
    )
    parser.add_argument(
        "run-name", type=str, default="", 
        help="Название запуска, использовавшегося для формирования логов в logs/runs директории."
    )
    parser.add_argument(
        "--data-root", type=str, default="data",
        help="Путь к папке с входными данными"
    )
    parser.add_argument(
        "--storage-root", type=str, default="storage",
        help="Корневая директория для кэшированных схем и векторных баз данных."
    )

    # Параметры агента
    parser.add_argument(
        "--prompt-dir", type=str, default="config/prompts/schema_linking",
        help="Путь к директории с шаблонами промптов (по умолчанию: config/prompts/schema_linking)"
    )
    parser.add_argument(
        "--prompt-name", type=str, default="sl_explore_validation_agent",
        help="Вариант промпта агента в папке prompt-dir (по умолчанию: sl_explore_validation_agent)"
    )
    parser.add_argument(
        "--max-turns", type=int, default=10,
        help="Максимальное количество ходов рассуждения агента (по умолчанию: 10)"
    )
    parser.add_argument(
        "--max-draft-calls", type=int, default=3,
        help="Максимальное число вызовов инструмента @sql_draft за сессию (по умолчанию: 3)"
    )
    parser.add_argument(
        "--additional-k", type=int, default=5,
        help="Число столбцов для возврата через инструмент @schema_retrieval (по умолчанию: 5)"
    )
    parser.add_argument(
        "--max-workers", type=int, default=4,
        help="Количество потоков для параллельной обработки примеров (по умолчанию: 4)"
    )
    parser.add_argument(
        "--max-attempts", type=int, default=3, 
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

    # Параметры модели
    parser.add_argument(
        "--model-name", type=str, default="qwen-local",
        help="Имя модели из configs/llm.json (по умолчанию: qwen-local)"
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
        "--temperature", type=float, default=1.0,
        help="Температура семплирования (по умолчанию: 1.0)"
    )

    # Параметры исполнения запросов
    parser.add_argument(
        "--local-dbs",
        type=parse_dialect_path_pair,
        nargs="*",  # Принимает 0 или более значений
        default=None,  # None означает "использовать дефолты из SQLExecutor"
        metavar="DIALECT:PATH",
        help="Пути к папкам локальных БД относительно data_root/input_data_root. "
            "Формат: 'dialect:path' (можно указать несколько через пробел). "
            "Пример: --local-dbs sqlite:databases snowflake:sf_data bigquery:local_bq"
    )
    parser.add_argument(
        "--exec-timeout", type=float, default=600, 
        help="Максимальное время ожидания исполнения SQL в секундах"
    )

    # Параметны векторной БД
    parser.add_argument(
        "--location", type=str, default=None,
        help="URL локального сервера с векторной базой данных."
    )
    parser.add_argument(
        "--embedding_model", type=str, default="microsoft/harrier-oss-v1-270m",
        help="Идентификатор HuggingFace модели или локальный путь для создания эмбеддингов. "
             "Поддерживает модели с prompt-based кодированием (Harrier, Qwen3 и др.)."
    )
    parser.add_argument(
        "--device", type=str, default="cpu",  # choices=["cpu", "cuda", "cuda:0", "mps"],
        help="Устройство для инференса модели эмбеддингов. "
             "'cuda' использует доступный GPU, 'cpu' — процессор, 'mps' — Apple Silicon."
    )
    parser.add_argument(
        "--max_cached_sessions", type=int, default=2,
        help="Максимальное число сессий векторного хранилища (разных датасетов) в RAM. "
             "Использует LRU-вытеснение для предотвращения OOM при работе с несколькими контекстами."
    )
    parser.add_argument(
        "--quantization", action="store_true",
        help="Включить int8 скалярное квантование векторов. Сокращает потребление RAM/диска в ~4 раза "
             "с минимальным влиянием на точность поиска. Рекомендуется для датасетов >50k столбцов."
    )
    parser.add_argument(
        "--backend", type=str, default="qdrant", choices=["qdrant"],
        help="Движок векторной базы данных. На текущий момент поддерживается только Qdrant."
    )
    args = parser.parse_args()

    run_id = resolve_run_id(input_data_root=args.input_data_root, custom_suffix=args.run_name)
    model = get_model(args.model_name, args.base_url, args.api_key, args.temperature)
    vsm = VectorStoreManager(
        args.storage_root, args.location, args.max_cached_sessions, 
        args.embedding_model, backend=args.backend, device=args.device, 
        quantization=args.quantization, log_path=os.path.join("logs", "dbs", args.input_data_root)
    )
    executor = SQLExecutor(args.input_data_root, args.data_root, args.storage_root, 
                           dict(args.local_dbs) if args.local_dbs else None, args.exec_timeout)

    pipeline = SchemaLinkingAgentPipeline(
        run_id, model, vsm, executor, 
        input_data_root=args.input_data_root, data_root=args.data_root, storage_root=args.storage_root,
        prompt_name=args.prompt_name, prompt_dir=args.prompt_dir, max_turns=args.max_turns, 
        max_draft_calls=args.max_draft_calls, additional_k=args.additional_k, max_workers=args.max_workers,
        retry_config={
            "max_attempts": args.max_attempts,
            "initial_delay": args.initial_delay,
            "max_delay": args.max_delay,
            "backoff_multiplier": 2.0
        }
    )
    pipeline.run()
    pipeline.extract_all_candidates()
