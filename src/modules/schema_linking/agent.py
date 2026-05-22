import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Optional, Any

from langchain_openai import ChatOpenAI
from tqdm import tqdm

from .agent_preprocessor import SchemaLinkingPreprocessor
from .agent_loop import SchemaLinkingAgent
from .agent_postprocessor import parse_and_validate_output, format_for_downstream
from .tools import get_enabled_tools, TOOL_REGISTRY
from src.storage.vector_manager import VectorStoreManager
from src.utils.sql_exeсution import SQLExecutor 
from src.utils.logger import get_logger


class SchemaLinkingAgentPipeline:
    def __init__(
        self,
        run_id: str,
        vsm: VectorStoreManager,
        executor: SQLExecutor,
        run_root: str = "logs/runs",
        input_data_root: str = "Spider2/spider2-lite",
        data_root: str = "data",
        storage_root: str = "storage",
        prompt_name: str = "sl_explore_validation_agent",
        prompt_path: str = "config/prompts/schema_linking",
        max_turns: int = 10,
        max_draft_calls: Optional[int] = 3,
        additional_k: int = 5,
        max_workers: int = 4
    ):
        self.run_id = run_id
        self.run_path = Path(run_root) / run_id
        self.storage_root = Path(storage_root)
        self.prompt_path = Path(prompt_path)
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
        self.logger = get_logger("sl_agent", str(self.run_path / "schema_linking" / "agent.log"))
        self.preprocessor = SchemaLinkingPreprocessor(
            prompt_name=prompt_name,
            base_dir=prompt_path,
            logger=self.logger
        )
        agent_config = {
            **self.config,
            "vsm": self.vsm,
            "executor": self.executor,
            "input_data_root": self.input_data_root
        }

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
            model=self._get_model(),
            tools=enabled_tools,
            config=agent_config,
            cache_dir=self.run_path / "schema_linking" / "retrieval_cache"
        )

    def _load_dialect_rules(self) -> Optional[Dict[str, str]]:
        """Загружает правила для конкретного диалекта."""
        dialects_path = self.prompt_path / "dialects"
        if dialects_path.exists():
            dialects = {file.split("_", 1)[0] for file in dialects_path.iterdir()}
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
        indices_path = self.run_path / "schema_linking" / "retrieval_cache" / "used_indices.json"
        if not indices_path.exists():
            raise FileNotFoundError(f"Indices file not found: {indices_path}")
        
        with open(indices_path, "r", encoding="utf-8") as f:
            used_indices = json.load(f)
        
        # Удаляем примеры, если они уже обработаны
        for file in (self.run_path / "schema_linking" / "candidates").iterdir():
            instance_id = file.rsplit(".", 1)[0]
            if instance_id in used_indices:
                del used_indices[instance_id]
        
        # Удаляем примеры, для которых больше не требуется добавлять столбцов
        # TODO

        # TODO Дабавляем недостающие метаданные

        return used_indices

    def _process_single_instance(self, instance_id: str, instance_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Обрабатывает один пример: от препроцессинга до постпроцессинга.
        """
        try:
            db_id = instance_data.get("db_id", instance_id.split("_", 1)[0])
            dialect, db_name = db_id.split("_", 1)
            schema_dir = self.run_path / "schema_linking" / "initial_schema"
            # cache_dir = self.run_path / "schema_linking" / "retrieval_cache"
            
            # 1. Предобработка
            context = {
                "USER_QUESTION": instance_data.get("question", ""),
                "RETRIEVED_SCHEMA": None,
                "ALL_TABLES": instance_data.get("all_tables", []),
                "TABLE_SCHEMAS": None,
                "EXTERNAL_KNOWLEDGE": open(instance_data["external_knowledge"], encoding="utf-8").read() 
                                      if instance_data.get("external_knowledge") else None
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

    def _load_llm_config(self, model_name: str) -> Dict[str, Any]:
        """
        Загружает конфигурацию модели из configs/llm.json.
        """
        config_path = Path("configs/llm.json")
        if not config_path.exists():
            # raise FileNotFoundError(f"LLM config not found at {config_path}")
            return {}
        
        with open(config_path, "r", encoding="utf-8") as f:
            full_config = json.load(f)
            
        models_cfg = full_config.get("models", {})
        if model_name not in models_cfg:
            raise ValueError(f"Model '{model_name}' not found in configs/llm.json. Available: {list(models_cfg.keys())}")
        
        return models_cfg[model_name]

    def _get_model(
        self, 
        model_name: str = "qwen-local", 
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        temperature: float = 1.0
    ) -> ChatOpenAI:
        """
        Инициализирует LLM через стандартный OpenAI API клиент.
        
        Args:
            model_name: Ключ модели в configs/llm.json
            base_url: Если передан, заменяет URL из конфига
            api_key: Если передан, заменяет API Key из конфига
            temperature: Если передан, заменяет температуру
            
        Returns:
            Экземпляр ChatOpenAI
        """
        cfg = self._load_llm_config(model_name)
        final_base_url = base_url or cfg.get("base_url")
        final_api_key = api_key or cfg.get("api_key", None)

        if not final_base_url:
            raise ValueError(f"base_url is not specified for model '{model_name}' and not provided via override.")

        if final_api_key is not None:
            normalized_api = final_api_key.replace("_", "")
            if normalized_api.isupper() and normalized_api.isalnum():
                final_api_key = os.environ.get(final_api_key)

        llm = ChatOpenAI(
            model=model_name,
            base_url=final_base_url,
            api_key=final_api_key,
            temperature=temperature,
            disable_streaming=True 
        )
        self.logger.info(f"LLM Initialized: {model_name} | {final_base_url}")
        return llm

    def run(self):
        """Запускает параллельную обработку всех примеров."""
        instances = self._load_instances()
        total = len(instances)
        
        self.logger.info(f"Starting pipeline for {total} instances with {self.max_workers} workers.")
        
        results = []
        successful = 0
        failed = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_id = {
                executor.submit(self._process_single_instance, iid, idata): iid 
                for iid, idata in instances.items()
            }
            
            for future in tqdm(as_completed(future_to_id), total=total, desc="Processing Instances"):
                instance_id = future_to_id[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result["status"] == "success":
                        successful += 1
                    else:
                        failed += 1
                        self.logger.warning(f"Instance {instance_id} failed with status: {result['status']}")
                        
                except Exception as e:
                    failed += 1
                    self.logger.exception(f"Unhandled exception for {instance_id}")
                    results.append({
                        "instance_id": instance_id,
                        "status": "exception",
                        "error": str(e)
                    })

        # Сохранение сводной статистики
        stats = {
            "total": total,
            "successful": successful,
            "failed": failed,
            "success_rate": successful / total if total > 0 else 0
        }
        
        stats_path = self.run_path / "schema_linking" / "pipeline_stats.json"
        stats_path.parent.mkdir(parents=True, exist_ok=True)
        with open(stats_path, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
            
        self.logger.info(f"Pipeline finished. Success: {successful}/{total}")
        return results
    