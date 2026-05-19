import re
import json
from pathlib import Path
from typing import Dict, Any, Optional


class SchemaLinkingPreprocessor:
    """
    Загружает и собирает двухчастный промпт:
    - system_prompt.md: роль, алгоритм, определения инструментов, общие правила
    - {name}_input.md: контекст случая (вопрос, схема, ограничения, формат вывода)
    """
    
    def __init__(self, prompt_name: str, base_dir: str = "config/prompts/schema_linking"):
        self.base_dir = Path(base_dir)
        self.prompt_name = prompt_name  # e.g., "sl_explore_validation_agent"
        self.system_path = self.base_dir / f"{prompt_name}.md"
        self.input_path = self.base_dir / f"{prompt_name}_input.md"
    
    def load_template(self, path: Path) -> str:
        if not path.exists():
            raise FileNotFoundError(f"Prompt template not found: {path}")
        return path.read_text(encoding="utf-8")
    
    def inject_user_context(self, template: str, context: Dict[str, Any]) -> str:
        """
        Заменяет {{PLACEHOLDER}} в user_input промпте на сериализованные данные.
        Поддерживает вложенные структуры через json.dumps.
        """
        result = template
        for key, value in context.items():
            placeholder = f"{{{{{key}}}}}"  # {{KEY}}
            if isinstance(value, (dict, list)):
                # Компактный JSON без пробелов для экономии токенов
                serialized = json.dumps(value, separators=(',', ':'), ensure_ascii=False)
            else:
                serialized = str(value)
            result = result.replace(placeholder, serialized)
        return result
    
    def inject_config_constraints(self, template: str, config: Dict[str, Any]) -> str:
        """
        Заменяет конфигурационные плейсхолдеры: {{MAX_TURNS}}, {{ENABLED_TOOLS}}, etc.
        """
        result = template
        # Список инструментов как строка для вставки в текст
        if "enabled_tools" in config:
            tools_str = ", ".join(f"`@{t}`" for t in config["enabled_tools"])
            result = result.replace("{{ENABLED_TOOLS}}", tools_str)
        # Числовые лимиты
        for key in ["MAX_TURNS", "MAX_DRAFT_CALLS"]:
            if key.lower() in config:
                result = result.replace(f"{{{{{key}}}}}", str(config[key.lower()]))
        return result
    
    def build_messages(
        self, 
        context: Dict[str, Any], 
        config: Dict[str, Any],
        dialect_rules: Optional[str] = None
    ) -> tuple[str, str]:
        """
        Returns: (system_prompt, user_prompt) готовых к передаче в LLM.
        """
        # 1. Загрузка шаблонов
        system_template = self.load_template(self.system_path)
        input_template = self.load_template(self.input_path)
        
        # 2. Инжекция контекста в user_input промпт
        user_prompt = self.inject_user_context(input_template, context)
        user_prompt = self.inject_config_constraints(user_prompt, config)
        
        # 3. Инжекция диалектных правил в system промпт (если переданы)
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
