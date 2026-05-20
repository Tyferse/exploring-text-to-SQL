import re
import json
from pathlib import Path
from typing import Dict, Any, Optional

class SchemaLinkingPreprocessor:
    """
    Загружает двухчастный промпт + предвычисленную схему из initial_schema/.
    """
    
    def __init__(self, prompt_name: str, base_dir: str = "config/prompts/schema_linking"):
        self.base_dir = Path(base_dir)
        self.prompt_name = prompt_name
        self.system_path = self.base_dir / f"{prompt_name}.md"
        self.input_path = self.base_dir / f"{prompt_name}_input.md"
    
    def load_template(self, path: Path) -> str:
        if not path.exists():
            raise FileNotFoundError(f"Prompt template not found: {path}")
        return path.read_text(encoding="utf-8")
    
    def load_initial_schema(self, instance_id: str, schema_dir: Path) -> Optional[str]:
        """Загружает предвычисленную схему или возвращает None, если файл не найден."""
        schema_file = schema_dir / f"{instance_id}.txt"
        if schema_file.exists():
            return schema_file.read_text(encoding="utf-8")
        return None
    
    def inject_user_context(
        self, 
        template: str, 
        context: Dict[str, Any],
        initial_schema: Optional[str] = None
    ) -> str:
        """
        Заменяет {{PLACEHOLDER}} в user_input промпте.
        Особая логика для RETRIEVED_SCHEMA: если есть initial_schema, подставляем её.
        """
        result = template
        
        for key, value in context.items():
            placeholder = f"{{{{{key}}}}}"
            
            # Ключевое изменение: подставляем предвычисленную схему вместо динамической
            if key == "RETRIEVED_SCHEMA" and initial_schema is not None:
                # Форматируем как код-блок для лучшей читаемости в промпте
                serialized = f"\n{initial_schema}\n"
            elif isinstance(value, (dict, list)):
                serialized = json.dumps(value, separators=(',', ':'), ensure_ascii=False)
            else:
                serialized = str(value)
            
            result = result.replace(placeholder, serialized)
        
        return result
    
    def inject_config_constraints(self, template: str, config: Dict[str, Any]) -> str:
        """Заменяет конфигурационные плейсхолдеры."""
        result = template
        if "enabled_tools" in config:
            tools_str = ", ".join(f"`@{t}`" for t in config["enabled_tools"])
            result = result.replace("{{ENABLED_TOOLS}}", tools_str)
        for key in ["MAX_TURNS", "MAX_DRAFT_CALLS"]:
            if key.lower() in config:
                result = result.replace(f"{{{{{key}}}}}", str(config[key.lower()]))
        return result
    
    def build_messages(
        self, 
        instance_id: str,
        schema_dir: Path,
        context: Dict[str, Any], 
        config: Dict[str, Any],
        dialect_rules: Optional[str] = None
    ) -> tuple[str, str]:
        """
        Returns: (system_prompt, user_prompt) с подстановкой предвычисленной схемы.
        """
        system_template = self.load_template(self.system_path)
        input_template = self.load_template(self.input_path)
        
        # Загрузка предвычисленной схемы
        initial_schema = self.load_initial_schema(instance_id, schema_dir)
        if initial_schema is None:
            # Fallback: логгируем предупреждение, агент получит пустую схему
            # В продакшене можно поднять исключение или запустить динамическую сборку
            print(f"⚠️ Warning: initial schema not found for {instance_id}")
        
        # Инжекция контекста (с приоритетом initial_schema)
        user_prompt = self.inject_user_context(input_template, context, initial_schema)
        user_prompt = self.inject_config_constraints(user_prompt, config)
        
        # Инжекция диалектных правил в system промпт
        if dialect_rules:
            if "# DIALECT OPTIMIZATION RULES" in system_template:
                system_prompt = system_template.replace(
                    "# DIALECT OPTIMIZATION RULES",
                    f"# DIALECT OPTIMIZATION RULES\n{dialect_rules}"
                )
            else:
                system_prompt = system_template + f"\n\n# DIALECT OPTIMIZATION RULES\n{dialect_rules}"
        else:
            system_prompt = system_template
        
        return system_prompt, user_prompt
    