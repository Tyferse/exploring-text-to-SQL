# INPUT CONTEXT

## User Question
{{USER_QUESTION}}

## Initially Retrieved Schema Candidates
{{RETRIEVED_SCHEMA}}

## Full Table Inventory
{{ALL_TABLES}}

## Complete Table Schemas (for referenced tables)
{{TABLE_SCHEMAS}}

## External Knowledge / Domain Context
{{EXTERNAL_KNOWLEDGE}}

---

# TOOL USAGE FORMAT REFERENCE

When calling tools, use EXACTLY this syntax (no markdown code fences around the call):

@schema_retrieval(table="orders", column="customer_id", description="Foreign key reference to customers table")

@schema_exploration(query="
-- Inspect distinct values and NULL ratio for status column
SELECT status, COUNT(*) FROM orders GROUP BY status ORDER BY COUNT(*) DESC LIMIT 5
")

@join_discovery(
  left_table="concert", left_column="singer_id",
  right_table="singer", right_column="id",
  join_type="INNER",
  validation_query="SELECT COUNT(*) FROM concert c JOIN singer s ON c.singer_id = s.id LIMIT 1"
)

@sql_draft(
  query="SELECT s.name FROM singer s JOIN concert c ON s.id = c.singer_id WHERE c.stadium_id = 'WEM' LIMIT 5",
  purpose="Validate join path singer→concert and stadium code filter"
)

@stop()

---

# GENERAL RULES AND CONSTRAINTS

## Execution Protocol
1. **Multi-Tool Per Turn:** Agent may invoke multiple tools in a single turn, subject to restrictions below.
2. **@stop Isolation:** `@stop()` must be the only tool call in its turn. Never combine with other tools.
3. **@sql_draft Limits:** Maximum 1 call per turn; maximum 3 calls total across the entire session.
4. **Strict Result Waiting:** After any tool call(s), pause generation. Continue reasoning ONLY in the next turn based on actual orchestration results. Never assume outputs.
5. **Query Safety:** All `@sql_draft` queries must be READ-ONLY and include LIMIT clauses. Never generate modifying statements.
6. **Dialect Compliance:** All generated SQL must strictly follow SQL Dialect Specification and Dialect-Specific Optimization Rules (quoting style, function availability, NULL handling, partitioning rules).

## Schema Linking Rules
7. **No Heuristic FK Inference:** Column name patterns (e.g., *_id) or type matches may suggest join hypotheses but NEVER constitute validation. Only execution evidence (COUNT > 0) validates a join.
8. **Explicit Context Only:** Use only tables/columns explicitly added via Initially Retrieved Schema Candidates or @schema_retrieval. Do not reference schema elements not in context.
9. **Confidence Tagging:** When uncertain, include elements with `"confidence": "low"` rather than omitting potentially critical items. Prefer recall over precision.
10. **Join Path Completeness:** Ensure all selected tables are connectable via validated joins. If a path cannot be established, mark the dependent columns as blocked.

## Output Integrity
11. **JSON-Only Final Output:** After @stop(), output ONLY the specified JSON object. No markdown fences, no explanatory text, no trailing commas.
12. **Exact Identifier Matching:** All table_name and column_name values must exactly match database identifiers (case-sensitive where applicable per dialect).
13. **Blocking Issues Documentation:** If `ready_for_sql_generation` is false, populate `blocking_issues` with specific, actionable problems (e.g., "join path between orders and products unvalidated", "column 'status' format unknown").

## Error Handling & Fallback
14. **Tool Error Response:** If a tool returns an error, analyze the message, adjust the hypothesis or query syntax per dialect rules, and retry within turn/iteration limits.
15. **Timeout Fallback:** If turn limit is reached with incomplete schema, call `@stop()` with `ready_for_sql_generation: false` and detailed `blocking_issues` for downstream recovery logic.

---

# OUTPUT REQUIREMENTS

After @stop(), output **ONLY** a valid JSON object with this exact structure (no markdown, no explanatory text):

```json
{
  "schema_linking_result": {
    "question_analysis": {
      "intent": "select|aggregate|filter|join|complex",
      "entities_requested": ["entity1", "entity2"],
      "filters_detected": [{"field_hint": "date", "value": "2023", "operator": "="}],
      "aggregations_needed": ["COUNT", "SUM"]
    },
    "tables_selected": [
      {
        "table_name": "exact_table_name",
        "role": "primary|junction|filter_source",
        "reasoning": "brief justification"
      }
    ],
    "columns_mapped": [
      {
        "table_name": "exact_table_name",
        "column_name": "exact_column_name",
        "usage": "select|filter|join_key|join_foreign|group_by|order_by",
        "confidence": "high|medium|low",
        "reasoning": "why this column is needed",
        "suggested_operator": "=|LIKE|>|<|IN|BETWEEN",
        "literal_value": "extracted value or pattern"
      }
    ],
    "inferred_joins": [
      {
        "left_table": "table_a",
        "left_column": "col_x",
        "right_table": "table_b",
        "right_column": "col_y",
        "join_type": "INNER|LEFT|OUTER|CROSS",
        "confidence": "high|medium|low",
        "evidence": "execution_validated|semantic_only"
      }
    ],
    "exploration_summary": {
      "triggered": true|false,
      "iterations": 0,
      "key_findings": ["finding1", "finding2"],
      "hypotheses_rejected": ["rejected assumption"]
    }
  },
  "ready_for_sql_generation": true|false,
  "blocking_issues": ["issue1", "issue2"]
}
```

Refer to the System Prompt for detailed field specifications and validation rules.

---

# BEGIN PROCESSING

Analyze the User Question and Schema Context above. Apply the algorithm, constraints, and tool protocols defined in the System Prompt. Output tool calls as needed, wait for results, and produce the final JSON after `@stop()`.

Start now.