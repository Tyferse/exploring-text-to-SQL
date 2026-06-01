
import inspect
import logging
from typing import Dict, Any, Optional

from src.modules.dbc_retrieval.exploratory_execution import exec_exploration
from src.modules.generation.simple_generation import simple_generation
from src.modules.schema_linking.agent import SchemaLinkingAgentPipeline
from src.modules.schema_linking.column_linking import ColumnLinking
from src.modules.schema_linking.double_linking import ColumnTableLinking, TableColumnLinking, BidirectionalLinking
from src.modules.schema_linking.generate_schema import generate_schemas
from src.modules.schema_linking.retrieve_schema import retrieve_columns
from src.modules.schema_linking.table_linking import TableLinking
from src.utils.gen_embeddings import gen_column_embeddings
from src.utils.preprocessing import spider2preprocess


HANDLER_REGISTRY = {
    "spider2preprocess": spider2preprocess,
    "gen_column_embeddings": gen_column_embeddings,
    "retrieve_columns": retrieve_columns,
    "TableLinking": TableLinking,
    "ColumnLinking": ColumnLinking,
    "TableColumnLinking": TableColumnLinking,
    "ColumnTableLinking": ColumnTableLinking,
    "BidirectionalLinking": BidirectionalLinking,
    "SchemaLinkingAgentPipeline": SchemaLinkingAgentPipeline,
    "generate_schemas": generate_schemas,
    "exec_exploration": exec_exploration,
    "simple_generation": simple_generation
}


def execute_stage(handler_key: str, kwargs: Dict[str, Any], logger: Optional[logging.Logger]) -> Any:
    """
    Универсальный вызов этапа.
    - Если функция: вызывает напрямую handler(**kwargs)
    - Если класс: создаёт экземпляр, вызывает .run(), затем автоматически ищет и вызывает
      пост-методы: extract_all_candidates, finalize, cleanup, save_results
    """
    if handler_key not in HANDLER_REGISTRY:
        available = ", ".join(HANDLER_REGISTRY.keys())
        raise ValueError(f"Handler '{handler_key}' not found in registry. Available: {available}")

    handler = HANDLER_REGISTRY[handler_key]

    if inspect.isclass(handler):
        # --- ЛОГИКА ДЛЯ КЛАССА ---
        if logger: logger.info(f"Instantiating class '{handler.__name__}'")
        instance = handler(**kwargs)
        
        run_method = getattr(instance, "run", None)
        if not callable(run_method):
            raise AttributeError(f"Class '{handler.__name__}' must have a callable 'run()' method")
        
        result = run_method()
        if logger: logger.info(f"Class '{handler.__name__}.run()' completed")

        # Автоматический вызов пост-методов, если они существуют
        post_methods = ["extract_all_candidates"]
        for method_name in post_methods:
            method = getattr(instance, method_name, None)
            if callable(method):
                method()
                
        return result
    else:
        # --- ЛОГИКА ДЛЯ ФУНКЦИИ ---
        if logger: logger.info(f"Calling function '{handler.__name__}'")
        return handler(**kwargs)
