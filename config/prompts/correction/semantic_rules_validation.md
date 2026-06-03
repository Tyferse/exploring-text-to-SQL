# Goal: Your task is to perform a semantic correction on the given {{DIALECT}} SQL statement. You must strictly follow the `Derived Rules` provided below, and convert the given SQL into a compliant, executable SQL statement.

# Input Data:

## User Question:
{{QUESTION}}

## Database Schema:
{{SCHEMA}}

## Current SQL:
```sql
{{CURRENT_SQL}}
```

## Execution Result (Sample Data):
{{EXECUTION_RESULT}}

## Derived Rules:
{{DERIVED_RULES}}

# Dialect-Specific Rules
{{DIALECT_OPTIMIZATION_RULES}}

# Strict Correction Guidelines:

- 1. **Rule Application:** You MUST follow the `Derived Rules` exactly. These rules contain the specific semantic corrections identified in the previous analysis step.
- 2. **Logic Preservation Rule (CRITICAL):** Except for SQL segments explicitly modified by the `Derived Rules`, STRICTLY PROHIBIT changing logical operators, values, or query structure, even if they seem incorrect to you. Do not "over-fix" the query.
- 3. **Syntax Compliance:** Ensure the final query strictly follows **{{DIALECT}}** syntax.

# Output Format:
<think> Step-by-step reasoning, applying the Derived Rules to the Current SQL. Check each rule and explain how it modifies the SQL. [Limited by 4K tokens] </think>
<answer> Summary of the thought process leading to the final {{DIALECT}} SQL query. **It should be made clear that the data may not be perfect, but you MUST generate an SQL query for the user based strictly on the Derived Rules.** [Limited by 1K tokens]

```sql
Correct SQL query here
```
</answer>

Example Output:
<think>  
- User question: "What is the maximum age of the students?"  
- Derived Rules: 1. Remove implicit null checks. 2. Ensure only age is returned.
- Current SQL: `SELECT id, MAX(age) FROM student WHERE age IS NOT NULL;` 

Now, I need to follow the STEP to check and correct the given SQL.  

Step 1: Apply Rule 1 (Remove implicit null checks)
- remove `WHERE age IS NOT NULL`, cause it doesn't explicitly required in the Derived Rules.

Step 2: Apply Rule 2 (Ensure only age is returned)
- remove `id` from SELECT clause.

Therefore, the corrected  SQL should be:  
`SELECT MAX(age) FROM student;`  
</think>

<answer>  
```sql
SELECT MAX(age) FROM student;
```
</answer>