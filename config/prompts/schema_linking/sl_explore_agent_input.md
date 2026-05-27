# INPUT CONTEXT

## User Question
{{USER_QUESTION}}

## External Knowledge / Domain Context
{{EXTERNAL_KNOWLEDGE}}

## Initially Retrieved Schema Candidates
{{RETRIEVED_SCHEMA}}

## Full Table Inventory
{{ALL_TABLES}}

## Complete Table Schemas (for referenced tables)
{{TABLE_SCHEMAS}}

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
  evidence={
    "naming_pattern": "suffix_match",
    "type_compatibility": true,
    "sample_value_overlap": true,
    "semantic_coherence": "high",
    "exploration_reference": "Turn 3: singer.id values [1,5,8] found in concert.singer_id"
  }
)

@stop()

---

# GENERAL RULES AND CONSTRAINTS

## Execution Protocol
1. **Multi-Tool Per Turn:** Agent may invoke multiple tools in a single turn, subject to restrictions below.
2. **@stop Isolation:** `@stop()` must be the only tool call in its turn. Never combine with other tools.
3. **Strict Result Waiting:** After any tool call(s), pause generation. Continue reasoning ONLY in the next turn based on actual orchestration results. Never assume outputs.
4. **Query Safety:** All `@schema_exploration` queries must be READ-ONLY and include LIMIT clauses. Never generate modifying statements.
5. **Dialect Compliance:** All generated SQL must strictly follow SQL Dialect Specification and Dialect-Specific Optimization Rules (quoting style, function availability, NULL handling, partitioning rules).

## Schema Linking Rules
6. **No Direct SQL Validation for Joins:** Do not use `COUNT(*) JOIN` or similar queries to validate relationships. Rely on sample value overlap, type compatibility, and semantic coherence confirmed via `@schema_exploration`.
7. **Exploration Precondition:** `@join_discovery` must NEVER be called without prior `@schema_exploration` results that provide evidence for the proposed join path.
8. **Explicit Context Only:** Use only tables/columns explicitly added via Initially Retrieved Schema Candidates or @schema_retrieval. Do not reference schema elements not in context.
9. **Confidence Tagging:** When uncertain, include elements with `"confidence": "low"` rather than omitting potentially critical items. Prefer recall over precision.
10. **Join Path Completeness:** Ensure all selected tables are connectable via evidenced joins. If a path cannot be established, mark the dependent columns as blocked.

## Output Integrity
11. **JSON-Only Final Output:** After @stop(), output ONLY the specified JSON object. No markdown fences, no explanatory text, no trailing commas.
12. **Exact Identifier Matching:** All table_name and column_name values must exactly match database identifiers (case-sensitive where applicable per dialect).
13. **Blocking Issues Documentation:** If `ready_for_sql_generation` is false, populate `blocking_issues` with specific, actionable problems (e.g., "insufficient sample overlap between orders.customer_id and users.id", "column 'status' format ambiguous after exploration").

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
        "evidence": {
          "naming_pattern": "...",
          "type_compatibility": true,
          "sample_value_overlap": true,
          "semantic_coherence": "...",
          "external_knowledge_hint": "..."
        }
      }
    ],
    "static_analysis_summary": {
      "metadata_checks_performed": 5,
      "join_hypotheses_evaluated": 3,
      "key_findings": ["status column samples match question literal", "concert.singer_id overlaps with singer.id samples"],
      "hypotheses_rejected": ["direct singer-stadium join (no sample overlap)"]
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