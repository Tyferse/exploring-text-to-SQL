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
