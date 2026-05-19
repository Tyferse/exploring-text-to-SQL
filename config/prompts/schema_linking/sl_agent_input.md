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

@join_discovery(
  left_table="concert", left_column="singer_id",
  right_table="singer", right_column="id",
  join_type="INNER",
  evidence={
    "naming_pattern": "suffix_match",
    "type_compatibility": true,
    "sample_value_overlap": true,
    "semantic_coherence": "high",
    "external_knowledge_hint": "concert table links performers to venues"
  }
)

@stop()

---

# GENERAL RULES AND CONSTRAINTS

## Execution Protocol
1. **Multi-Tool Per Turn:** Agent may invoke multiple tools in a single turn, subject to restrictions below.
2. **@stop Isolation:** `@stop()` must be the only tool call in its turn. Never combine with other tools.
3. **Strict Acknowledgment Waiting:** After any tool call(s), pause generation. Continue reasoning ONLY in the next turn based on orchestration acknowledgment. Never assume outputs.
4. **No Execution Assumptions:** Do not reference, simulate, or assume database query execution. All reasoning must be grounded in provided metadata, samples, and external knowledge.
5. **Dialect Compliance:** When inferring operators, date formats, or quoting styles, apply Dialect-Specific Optimization Rules and SQL Dialect Specification rules to static reasoning.

## Static Validation Rules
6. **Sample-Value Grounding:** When inferring operators or formats, always reference pre-loaded `sample_values` from metadata. Do not assume formats not evidenced by samples.
7. **Join Evidence Threshold:** A join requires both structural evidence (naming + type) AND contextual evidence (sample overlap OR high semantic coherence). Naming patterns alone are insufficient.
8. **Confidence Propagation:** Column confidence affects join confidence: a join involving a "low" confidence column cannot exceed "medium" overall confidence.
9. **Conservative Inclusion:** When static evidence is ambiguous, include elements with `"confidence": "low"` rather than omitting potentially critical items. Let downstream SQL generation handle false positives via schema validation.

## Output Integrity
10. **JSON-Only Final Output:** After @stop(), output ONLY the specified JSON object. No markdown fences, no explanatory text, no trailing commas.
11. **Exact Identifier Matching:** All table_name and column_name values must exactly match database identifiers (case-sensitive where applicable per dialect).
12. **Blocking Issues Documentation:** If `ready_for_sql_generation` is false, populate `blocking_issues` with specific, actionable problems (e.g., "insufficient sample overlap between orders.customer_id and users.id", "column 'status' format ambiguous in metadata").

## Error Handling & Fallback
13. **Metadata Gaps:** If `sample_values` are missing or insufficient for a critical column, mark it as `"confidence": "low"` and document in `blocking_issues` if uncertainty remains.
14. **Timeout Fallback:** If turn limit is reached with incomplete schema, call `@stop()` with `ready_for_sql_generation: false` and detailed `blocking_issues` for downstream recovery logic.

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