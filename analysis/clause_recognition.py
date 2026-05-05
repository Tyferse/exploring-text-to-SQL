import re
import sqlglot
from sqlglot import exp
from entity_recognition import extract_entity


explicit_groupped_patterns = {
    'FROM': [
        
    ],
    'JOIN': [
        (r"Column\s+['\"]?(\w+)['\"]?\s+in.*?clause not found on left side of join", 1),
        (r"near\s+\"()\"", 1),
        (r"unexpected\s+'(\w+)'", 1),
        "JOIN cannot be used without a condition",
    ],
    'WHERE': [],
    'GROUP BY': [],
    'HAVING': [],
    'SELECT': [],
    'WINDOW': [
        (r"misuse of(?: aliased)? window function:\s*(\w+)", 1),
        (r"(\w+)\s+is not supported for window functions", 1),
        (r"window function.*?(\w+)\s+is not", 1),
        (r"(\w+)\s+cannot be used as a window function", 1),
        "ORDER BY key must be numeric in a RANGE-based window",
    ],
    'ORDER BY': [
        "ORDER BY term does not match any column"
    ],
    'LIMIT': [],
    'UNION': [],
    'INTERSECT': [],
    'EXCEPT': [],
    'WITH': [],
    'SET': []
}


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
    global explicit_groupped_patterns

    # Если есть позиция ошибки, определяем по позиции напрямую из текста
    if error_position:
        error_line, error_col = error_position
        return find_operator_by_position(sql, error_line, error_col)
    
    # Проверяем некоторые известные явные паттерны
    for group in explicit_groupped_patterns:
        for pattern in explicit_groupped_patterns[group]:
            if ((isinstance(pattern, tuple) and re.match(pattern[0], error_message)) 
                or (isinstance(pattern, str) and pattern in error_message)):
                return group

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
    
     # Ищем в логическом порядке только среди основных операторов
    found_operators = []
    for query_name, query in queries_to_check:
        for op_type in operator_priority:
            if _check_entity_in_operator(query, entity_name, op_type):
                found_operators.append((op_type, query_name))
    
    # Анализируем найденные операторы
    if not found_operators:
        # Сущность не найдена в запросе
        return "UNKNOWN"
    
    # Если нашли только в SELECT, нужно дополнительно проверить контекст
    if len(found_operators) == 1 and found_operators[0][0] == 'SELECT':
        op_type, query_name = found_operators[0]
        if _is_meaningful_select_context(entity_name, tree):
            return f"{op_type} ({query_name})"
        else:
            # Недостаточно контекста для определения
            return "UNKNOWN"
    
    # Возвращаем первый найденный оператор по логическому порядку
    # (не SELECT, если есть другие)
    for op_type, query_name in found_operators:
        if op_type != 'SELECT':
            return f"{op_type} ({query_name})"
    
    # Если только SELECT, возвращаем его
    if found_operators:
        return f"SELECT ({found_operators[0][1]})"
    
    return "UNKNOWN"


def _is_meaningful_select_context(entity_name: str, tree) -> bool:
    """
    Проверяет, действительно ли сущность в SELECT имеет значимый контекст.
    """
    entity_name_lower = entity_name.lower()
    # Проверяем, не является ли сущность алиасом таблицы
    try:
        for node in tree.walk():
            if isinstance(node, exp.TableAlias):
                if hasattr(node, 'name') and node.name and node.name.lower() == entity_name_lower:
                    return False  # Это алиас, контекст неоднозначен
    except Exception:
        pass
    
    # Проверяем, используется ли сущность в значимом контексте
    try:
        for node in tree.walk():
            if isinstance(node, exp.Select) and 'expressions' in node.args:
                for expr in node.args['expressions']:
                    # Проверяем, является ли выражение простым именем столбца
                    if isinstance(expr, exp.Column):
                        if expr.name and expr.name.lower() == entity_name_lower:
                            # Проверяем, есть ли у столбца таблица
                            if hasattr(expr, 'table') and expr.table:
                                return True  # Уточненный столбец
                            else:
                                return False  # Просто имя, может быть алиасом
                    
                    # Проверяем, является ли выражение функцией
                    if isinstance(expr, (exp.Anonymous, exp.Func)):
                        if _entity_in_subtree(expr, entity_name):
                            return True  # Используется в функции
                    
                    # Проверяем арифметические выражения
                    if isinstance(expr, exp.Binary):
                        if _entity_in_subtree(expr, entity_name):
                            return True  # Используется в выражении
    except Exception:
        pass

    return False


def _check_entity_in_operator(query, entity_name, search_op_type):
    """Проверяет, используется ли сущность в определенном типе оператора"""
    try:
        for node in query.walk():
            if (
                search_op_type == 'FROM' and isinstance(node, exp.From)
                or search_op_type == 'JOIN' and isinstance(node, exp.Join)
                or search_op_type == 'WHERE' and isinstance(node, exp.Where)
                or search_op_type == 'GROUP BY' and isinstance(node, exp.Group)
                or search_op_type == 'HAVING' and isinstance(node, exp.Having)
            ):
                if _entity_in_subtree(expr, entity_name):
                    return True
            elif search_op_type == 'SELECT' and isinstance(node, exp.Select):
                if 'expressions' in node.args:
                    for expr in node.args['expressions']:
                        if _entity_in_subtree(expr, entity_name):
                            return True
            elif (
                search_op_type == 'WINDOW' and isinstance(node, exp.Window)
                or search_op_type == 'ORDER BY' and isinstance(node, exp.Order)
                or search_op_type == 'LIMIT' and isinstance(node, exp.Limit)
            ):
                if _entity_in_subtree(expr, entity_name):
                    return True
    except Exception:
        pass
    
    return False


def _entity_in_subtree(node, entity_name):
    """Проверяет, есть ли ссылка на сущность в поддереве"""
    entity_name_lower = entity_name.lower()
    
    try:
        for subnode in node.walk():
            if isinstance(subnode, (exp.Column, exp.Table, exp.TableAlias, exp.Anonymous, exp.Var, exp.Identifier)):
                if hasattr(subnode, 'name') and subnode.name and subnode.name.lower() == entity_name_lower:
                    return True
            elif isinstance(subnode, exp.Func):
                # Для стандартных функций
                if hasattr(subnode, 'sql_name'):
                    func_name = subnode.sql_name()
                    if func_name and func_name.lower() == entity_name_lower:
                        return True
                elif hasattr(subnode, 'name') and subnode.name and subnode.name.lower() == entity_name_lower:
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


    tests = [
        ("WITH driver_season AS (  SELECT d.driver_id,         d.forename,         d.surname,         rg.year,         MIN(rg.round) OVER (PARTITION BY r.driver_id, rg.year) AS first_round,         MAX(rg.round) OVER (PARTITION BY r.driver_id, rg.year) AS last_round,         COUNT(DISTINCT rg.round) OVER (PARTITION BY r.driver_id, rg.year) AS round_cnt,         FIRST_VALUE(r.constructor_id) OVER (PARTITION BY r.driver_id, rg.year ORDER BY rg.round ASC) AS first_constructor,         LAST_VALUE(r.constructor_id) OVER (PARTITION BY r.driver_id, rg.year ORDER BY rg.round ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_constructor  FROM drivers d  JOIN results r ON d.driver_id = r.driver_id  JOIN races rg ON r.race_id = rg.race_id  WHERE rg.year BETWEEN 1950 AND 1959 ) SELECT DISTINCT forename, surname, year FROM driver_season WHERE first_round < last_round   AND first_constructor = last_constructor   AND round_cnt >= 2;", 
        "DISTINCT is not supported for window functions"),
        ("WITH cutoff_weeks AS (   SELECT 2018 AS year, DATE '2018-06-11' AS cutoff_week_start UNION ALL   SELECT 2019, DATE '2019-06-10' UNION ALL   SELECT 2020, DATE '2020-06-08' ), leading_weeks AS (   SELECT      cw.year,     SUM(ws.sales) AS leading_sales   FROM cutoff_weeks cw   JOIN weekly_sales ws      ON EXTRACT(YEAR FROM ws.week_date) = cw.year     AND ws.week_date BETWEEN cw.cutoff_week_start - INTERVAL '28 days' AND cw.cutoff_week_start - INTERVAL '7 days'   GROUP BY cw.year ), following_weeks AS (   SELECT      cw.year,     SUM(ws.sales) AS following_sales   FROM cutoff_weeks cw   JOIN weekly_sales ws      ON EXTRACT(YEAR FROM ws.week_date) = cw.year     AND ws.week_date BETWEEN cw.cutoff_week_start + INTERVAL '7 days' AND cw.cutoff_week_start + INTERVAL '28 days'   GROUP BY cw.year ) SELECT    lw.year,   lw.leading_sales,   fw.following_sales,   ( (fw.following_sales - lw.leading_sales) / NULLIF(lw.leading_sales, 0) ) * 100 AS pct_change FROM leading_weeks lw JOIN following_weeks fw ON lw.year = fw.year ORDER BY lw.year;", 
        "near \"AS\": syntax error")
    ]
    for sql, msg in tests:
        print(find_error_operator(sql, error_message=msg, dialect='sqlite'))
