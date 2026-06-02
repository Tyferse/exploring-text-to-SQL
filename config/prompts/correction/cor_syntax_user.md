# Task Context
Analyze the failed SQL query below and provide a corrected version based on the provided schema and error details.

## User Question
{{QUESTION}}

## Database Schema
{{SCHEMA}}

## External Knowledge
{{EXTERNAL_KNOWLEDGE}}

## Exploration Results (Sample Data)
{{EXPLORATION_BLOCK}}

## Few-Shot Examples (Reference)
{{FEW_SHOT_EXAMPLES}}

## Original Failed SQL
```sql
{{ORIGINAL_SQL}}
```

## Error Details
- Error Message: {{ERROR_MESSAGE}}
- Failed Operator: {{FAILED_OPERATOR}}

# Instruction
Generate the corrected SQL query. Ensure it strictly follows the {{DIALECT}} rules defined in the system prompt.

Example Output:
```sql
SELECT * FROM table_name;
```