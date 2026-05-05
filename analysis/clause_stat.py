import re
import sqlglot
from sqlglot import exp
from entity_recognition import extract_entity


def find_error_operator(sql, error_message=None, error_position=None, dialect=None):
    """
    Определяет оператор SQL запроса, в котором произошла ошибка.
    
    Args:
        sql (str): SQL запрос
        error_message (str, optional): Сообщение об ошибке
        error_position (tuple, optional): Координаты (строка, столбец) первого символа ошибки
        dialect (str, optional): Диалект SQL ('sqlite', 'snowflake', 'bigquery')
    
    Returns:
        str: Название оператора (SELECT, FROM, JOIN, WHERE, и т.д.)
    """
    
    # Если есть позиция ошибки, определяем по позиции напрямую из текста
    if error_position:
        error_line, error_col = error_position
        return find_operator_by_position(sql, error_line, error_col)
    
    # Нормализуем диалект для парсинга
    dialect_map = {
        'sqlite': 'sqlite',
        'snowflake': 'snowflake',
        'bigquery': 'bigquery',
        None: None
    }
    parsed_dialect = dialect_map.get(dialect, dialect)
    
    # Парсим SQL в AST для поиска по имени сущности
    try:
        tree = sqlglot.parse_one(sql, dialect=parsed_dialect, error_level=sqlglot.ErrorLevel.IGNORE)
    except Exception:
        try:
            tree = sqlglot.parse_one(sql, error_level=sqlglot.ErrorLevel.IGNORE)
        except Exception:
            return "UNKNOWN"
    
    if error_message:
        # Пытаемся извлечь имя сущности из сообщения об ошибке
        entity_name = extract_entity(error_message)
        print(entity_name)
        if entity_name:
            return find_operator_by_logical_order(tree, entity_name)
    
    return "UNKNOWN"


def find_operator_by_position(sql, error_line, error_col):
    """
    Определяет оператор по позиции ошибки, анализируя текст SQL.
    """
    
    lines = sql.split('\n')
    
    # Проверяем, что строка существует
    if error_line < 1 or error_line > len(lines):
        return "UNKNOWN"
    
    # Получаем полный текст до позиции ошибки (включая текущую строку до столбца)
    text_before = '\n'.join(lines[:error_line - 1])
    if error_line > 1:
        text_before += '\n'
    text_before += lines[error_line - 1][:error_col - 1] if error_col > 1 else ""
    
    return analyze_sql_context(text_before)


def analyze_sql_context(text_before):
    """
    Анализирует контекст SQL для определения текущего оператора.
    """
    
    # Удаляем комментарии для анализа структуры
    def remove_comments(text):
        text = re.sub(r'--[^\n]*', '', text)
        text = re.sub(r'/\*.*?\*/', '', text, flags=re.DOTALL)
        return text
    
    clean_text = remove_comments(text_before)
    clean_text_upper = clean_text.upper()
    
    # Определяем, находимся ли мы внутри CTE
    cte_name = None
    cte_matches = re.findall(r'(\w+)\s+AS\s*\(', clean_text, re.IGNORECASE)
    if cte_matches:
        # Считаем баланс скобок
        open_parens = clean_text.count('(')
        close_parens = clean_text.count(')')
        if open_parens > close_parens:
            # Мы внутри CTE, находим имя последнего
            cte_name = cte_matches[-1]
    
    # Ищем последний оператор перед позицией ошибки
    # Ключевые слова операторов с их приоритетом (чем позже в списке, тем выше приоритет)
    operator_patterns = [
        (r'\bFROM\b', 'FROM'),
        (r'\b((?:LEFT|RIGHT|INNER|OUTER|FULL|CROSS)?\s*JOIN)\b', 'JOIN'),
        (r'\bWHERE\b', 'WHERE'),
        (r'\bGROUP\s+BY\b', 'GROUP BY'),
        (r'\bHAVING\b', 'HAVING'),
        (r'\bSELECT\b', 'SELECT'),
        (r'\bOVER\b', 'WINDOW'),
        (r'\bORDER\s+BY\b', 'ORDER BY'),
        (r'\bLIMIT\b', 'LIMIT'),
        (r'\bUNION\s+(ALL\s+)?', 'UNION'),
        (r'\bINTERSECT\b', 'INTERSECT'),
        (r'\bEXCEPT\b', 'EXCEPT'),
        (r'\bWITH\b', 'WITH'),
        (r'\bSET\b', 'SET'),
    ]
    
    last_operator = None
    last_position = -1
    
    for pattern, op_type in operator_patterns:
        for match in re.finditer(pattern, clean_text_upper):
            pos = match.start()
            if pos > last_position:
                last_position = pos
                last_operator = op_type
    
    # Если нашли оператор, определяем контекст
    if last_operator:
        if last_operator == 'WITH' and cte_name:
            return f"CTE: {cte_name}"
        elif cte_name and last_operator != 'WITH':
            return f"{last_operator} (CTE: {cte_name})"
        else:
            # Проверяем, не в подзапросе ли мы
            if is_inside_subquery(clean_text):
                return f"{last_operator} (subquery)"
            return last_operator
    
    # Если не нашли явный оператор, пробуем определить контекст
    # Проверяем, не в выражении ли мы (например, внутри функции в SELECT)
    if is_inside_select_expression(clean_text):
        if cte_name:
            return f"SELECT (CTE: {cte_name})"
        return "SELECT (expression)"
    
    # По умолчанию
    if cte_name:
        return f"CTE: {cte_name}"
    
    return "SELECT"


def is_inside_subquery(text):
    """Проверяет, находимся ли мы внутри подзапроса"""
    # Считаем SELECT до и после последней открывающей скобки
    last_open_paren = text.rfind('(')
    if last_open_paren == -1:
        return False
    
    text_after_paren = text[last_open_paren + 1:]
    return bool(re.search(r'\bSELECT\b', text_after_paren, re.IGNORECASE))


def is_inside_select_expression(text):
    """Проверяет, находимся ли мы внутри выражения SELECT"""
    # Ищем последний SELECT и проверяем, что после него нет FROM/WHERE и т.д.
    last_select = re.search(r'\bSELECT\b', text, re.IGNORECASE)
    if not last_select:
        return False
    
    text_after_select = text[last_select.end():]
    # Если после SELECT нет других операторов, мы всё ещё в SELECT выражении
    other_operators = r'\b(?:FROM|WHERE|GROUP\s+BY|HAVING|OVER|ORDER\s+BY|LIMIT|UNION|JOIN)\b'
    return not re.search(other_operators, text_after_select, re.IGNORECASE)


def find_operator_by_logical_order(tree, entity_name):
    operator_priority = [
        'FROM', 'JOIN', 'WHERE', 'GROUP BY', 'HAVING', 
        'SELECT', 'WINDOW', 'ORDER BY', 'LIMIT'
    ]
    
    queries_to_check = []
    
    # Проверяем CTE
    if isinstance(tree, exp.Select) and hasattr(tree, 'args') and tree.args.get('with'):
        with_clause = tree.args['with']
        if hasattr(with_clause, 'expressions'):
            ctes = with_clause.expressions
            for cte in ctes:
                if hasattr(cte, 'alias') and cte.alias:
                    queries_to_check.append((f"CTE: {cte.alias}", cte.this))
    
    # Добавляем основной запрос
    if isinstance(tree, exp.Select):
        queries_to_check.append(("Main query", tree))
    else:
        # Для не-SELECT запросов тоже пробуем
        queries_to_check.append(("Query", tree))
    
    found_in_select_general = None
    
    for query_name, query in queries_to_check:
        for op_type in operator_priority:
            if _check_entity_in_operator(query, entity_name, op_type):
                result = f"{op_type} ({query_name})"
                if op_type == 'SELECT':
                    found_in_select_general = result
                else:
                    return result
    
    if found_in_select_general:
        return found_in_select_general
    
    return "SELECT (Main query)"


def _check_entity_in_operator(query, entity_name, search_op_type):
    """Проверяет, используется ли сущность в определенном типе оператора"""
    try:
        for node in query.walk():
            if search_op_type == 'FROM' and isinstance(node, exp.From):
                return _entity_in_subtree(node, entity_name)
            elif search_op_type == 'JOIN' and isinstance(node, exp.Join):
                return _entity_in_subtree(node, entity_name)
            elif search_op_type == 'WHERE' and isinstance(node, exp.Where):
                return _entity_in_subtree(node, entity_name)
            elif search_op_type == 'GROUP BY' and isinstance(node, exp.Group):
                return _entity_in_subtree(node, entity_name)
            elif search_op_type == 'HAVING' and isinstance(node, exp.Having):
                return _entity_in_subtree(node, entity_name)
            elif search_op_type == 'SELECT' and isinstance(node, exp.Select):
                if 'expressions' in node.args:
                    for expr in node.args['expressions']:
                        return _entity_in_subtree(expr, entity_name)
            elif search_op_type == 'WINDOW' and isinstance(node, exp.Window):
                return _entity_in_subtree(node, entity_name)
            elif search_op_type == 'ORDER BY' and isinstance(node, exp.Order):
                return _entity_in_subtree(node, entity_name)
            elif search_op_type == 'LIMIT' and isinstance(node, exp.Limit):
                return _entity_in_subtree(node, entity_name)
    except Exception:
        pass
    
    return False


def _entity_in_subtree(node, entity_name):
    """Проверяет, есть ли ссылка на сущность в поддереве"""
    entity_name_lower = entity_name.lower()
    
    try:
        for subnode in node.walk():
            if isinstance(subnode, (exp.Column, exp.Table, exp.Anonymous, exp.Var, exp.Identifier)):
                if hasattr(subnode, 'name') and subnode.name and subnode.name.lower() == entity_name_lower:
                    return True
    except Exception:
        pass
    
    return False
    

if __name__ == "__main__":
    sql = """WITH demand AS (
SELECT
o."order_id",
o."product_id",
o."qty" AS "required_qty",
ord."ordered" AS "order_date"
FROM "orderlines" o
JOIN "orders" ord ON o."order_id" = ord."id"
),
supply AS (
SELECT
i."product_id",
i."qty" AS "available_qty",
p."purchased" AS "purchase_date",
i."purchase_id"
FROM "inventory" i
JOIN "purchases" p ON i."purchase_id" = p."id"
),
supply_sorted AS (
SELECT
"product_id",
"available_qty",
"purchase_date",
SUM("available_qty") OVER (PARTITION BY "product_id" ORDER BY "purchase_date" ASC, "available_qty" ASC) AS "cum_supply",
SUM("available_qty") OVER (PARTITION BY "product_id" ORDER BY "purchase_date" ASC, "available_qty" ASC) - "available_qty" AS "cum_supply_before"
FROM supply
),
demand_sorted AS (
SELECT
"order_id",
"product_id",
"required_qty",
"order_date",
SUM("required_qty") OVER (PARTITION BY "product_id" ORDER BY "order_date" ASC, "order_id" ASC) AS "cum_demand",
SUM("required_qty") OVER (PARTITION BY "product_id" ORDER BY "order_date" ASC, "order_id" ASC) - "required_qty" AS "cum_demand_before"
FROM demand
),
allocated AS (
SELECT
d."order_id",
d."product_id",
d."required_qty",
d."order_date",
d."cum_demand",
d."cum_demand_before",
s."purchase_date",
s."available_qty",
s."cum_supply",
s."cum_supply_before",
CASE
WHEN d."cum_demand_before" >= s."cum_supply" THEN 0
WHEN d."cum_demand" <= s."cum_supply_before" THEN 0
ELSE LEAST(d."cum_demand", s."cum_supply") - GREATEST(d."cum_demand_before", s."cum_supply_before")
END AS "picked_qty"
FROM demand_sorted d
LEFT JOIN supply_sorted s ON d."product_id" = s."product_id"
),
order_pick_percent AS (
SELECT
"product_id",
"required_qty",
SUM("picked_qty") AS "total_picked_qty",
("total_picked_qty" / "required_qty") * 100 AS "pick_percent"
FROM allocated
GROUP BY "product_id", "required_qty", "order_id"
),
product_avg_percent AS (
SELECT
o."product_id",
p."name" AS "product_name",
AVG(o."pick_percent") AS "avg_pick_percent"
FROM order_pick_percent o
JOIN "products" p ON o."product_id" = p."id"
GROUP BY o."product_id", p."name"
)
SELECT "product_name", ROUND("avg_pick_percent", 4) AS "avg_pick_percentage"
FROM product_avg_percent
ORDER BY "product_name"
LIMIT 5;"""

    
    
    # Тест с ошибкой "no such function: GREATEST"
    error_msg = "no such function: GREATEST"
    result = find_error_operator(sql, error_message=error_msg, dialect='sqlite')
    print(f"Ошибка: {error_msg}")
    print(f"Оператор: {result}")
    
    # Тест с позицией ошибки (строка 53, столбец 45 - где LEAST/GREATEST)
    result_pos = find_error_operator(sql, error_position=(53, 45), dialect='sqlite')
    print(f"\nПозиция ошибки: строка 53, столбец 45")
    print(f"Оператор: {result_pos}")
