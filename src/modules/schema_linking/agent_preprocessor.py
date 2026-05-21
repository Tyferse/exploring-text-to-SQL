import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional

class SchemaLinkingPreprocessor:
    """
    Загружает двухчастный промпт.
    """
    
    def __init__(self, prompt_name: str, base_dir: str = "config/prompts/schema_linking", logger: Optional[logging.Logger] = None):
        self.base_dir = Path(base_dir)
        self.prompt_name = prompt_name
        self.system_path = self.base_dir / f"{prompt_name}.md"
        self.input_path = self.base_dir / f"{prompt_name}_input.md"
        self.log = logger
    
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
    
    def inject_context(
        self, 
        template: str, 
        context: Dict[str, Any]
    ) -> str:
        """
        Заменяет {{PLACEHOLDER}} в user_input промпте.
        """
        result = template
        
        for key, value in context.items():
            placeholder = f"{{{{{key}}}}}"
            if isinstance(value, (dict, list)):
                serialized = json.dumps(value, separators=(',', ':'), ensure_ascii=False)
            else:
                serialized = str(value)
            
            result = result.replace(placeholder, serialized)
        
        return result
    
    def build_messages(
        self, 
        instance_id: str,
        context: Dict[str, Any], 
        schema_dir: Path,
        dialect_rules: Optional[Dict[str, str]] = None
    ) -> tuple[str, str]:
        """
        Returns: (system_prompt, user_prompt) с подстановкой предвычисленной схемы.
        """
        system_template = self.load_template(self.system_path)
        input_template = self.load_template(self.input_path)
        
        # Загрузка предвычисленной схемы
        if "RETRIEVED_SCHEMA" not in context:
            context["RETRIEVED_SCHEMA"] = self.load_initial_schema(instance_id, schema_dir)
        
        if context["RETRIEVED_SCHEMA"] is None:
            if self.log:
                self.log.warning(f"initial schema not found for {instance_id}")
            else:
                print(f"Warning: initial schema not found for {instance_id}")
        
        user_prompt = self.inject_context(input_template, context)
        
        # Вставка диалектных правил в system промпт
        if dialect_rules:
            if "{{SQL_OPTIMIZATION}}" in system_template:
                system_prompt = system_template.replace(
                    "{{DIALECT_SPECIFICS}}",
                    dialect_rules['specifics']
                )
                system_prompt = system_template.replace(
                    "{{SQL_OPTIMIZATION}}",
                    dialect_rules['rules']
                )
        else:
            system_prompt = system_template
        
        return system_prompt, user_prompt
    