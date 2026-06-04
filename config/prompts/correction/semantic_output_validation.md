# Goal: Follow the STEP, your task is to perform a column and format check on the given SQL statement in {{DIALECT}} dialect.

# Input Data:

## User Question:
{{QUESTION}}

## Database Schema:
{{SCHEMA}}

## External Knowledge:
{{EXTERNAL_KNOWLEDGE}}

## Current SQL:
```sql
{{CURRENT_SQL}}
```

## Execution Result (Sample Data):
{{EXECUTION_RESULT}}

# Dialect-Specific Rules
{{DIALECT_OPTIMIZATION_RULES}}

# STEP:

- 1. Extract the explicit content that the user needs to return as the **minimum** requirement in the question. Not having an identifier is completely acceptable.

- 2. Modify the SELECT clause in the SQL statement to return only the requested content.

# Important Note:
- **You can only delete return columns, add new return columns, adjust the order of return columns. Other operations are strictly prohibited, even if the logic in the SQL might be incorrect.**

# Column & Format Rules:
- 1. Column Selection Check Rule:
    - Only return columns explicitly requested by the user. Prohibit using `SELECT *`. When the user does not specify the fields to return, default to returning the entity identifier to represent the entity.
    - Example 1: 
        ++ User question: "What is the maximum age of the students?"  
        ++ ✅ Correct SQL: SELECT MAX(age) FROM student;  
        ++ ❌ Incorrect SQL: SELECT id, MAX(age) FROM student; 
    - Example 2:
        ++ User question: "Which top 4 student had the most games?"
        ++ ✅ Correct SQL: SELECT id FROM League ...;
        ++ ❌ Incorrect SQL: SELECT id, COUNT(game.id) FROM student ...;

- 2. Column Order Check Rule:
    - The order of columns in the SELECT clause must exactly match the order of attributes mentioned in the User Question.
    - Example 1:  
        ++ User question: "What are the student id and name?"  
        ++ ✅ Correct SQL: SELECT id, name FROM student;  
        ++ ❌ Incorrect SQL: SELECT name, id FROM student;  

- 3. RANK Check Rule: 
    - For questions that include the phrase "Rank ...", add `RANK()` at the end of all return columns.

- 4. Format Check Rule: 
    - NEVER use `||`, `GROUP_CONCAT` or `CONCAT` in SQL queries. Individual columns in the SELECT clause should be returned as separate fields without combining them.

# Output Format:
<think>
Step-by-step reasoning on how the given SQL is analyzed and rewritten according to the above rules.  
</think>

<answer>
Summarize the reasoning and show the final, rewritten SQL query that follows the preferred {{DIALECT}} SQL style.
Remember you can only delete return columns, add new return columns, adjust the order of return columns. Other operations are strictly prohibited, even if the logic in the SQL might be incorrect.

```sql
-- Corrected {{DIALECT}} SQL query here
```
</answer>