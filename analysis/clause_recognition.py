import re
import sqlglot
from sqlglot import exp
from entity_recognition import extract_entity


explicit_groupped_patterns = {
    'FROM': [],
    'JOIN': [
        r"Column\s+['\"]?([\w\.]+)['\"]?\s+in.*?clause not found on left side of join",
        r"near\s+\"([\w\.]+)\"",
        r"unexpected\s+'([\w\.]+)'",
        r"called with * syntax or a join predicate",
        "JOIN cannot be used without a condition",
        "Correlated subqueries",
        "Unsupported subquery with table in join predicate",
        r"lateral table function called with [\w\s]+ syntax or a join predicate",
    ],
    'WHERE': [],
    'GROUP BY': [
        ("Expressions of type", "cannot be used as GROUP BY keys"),
        "Cannot GROUP BY literal values",
    ],
    'HAVING': [],
    'SELECT': [],
    'WINDOW': [
        r"misuse of(?: aliased)? window function:\s*(\w+)",
        r"(\w+)\s+is not supported for window functions",
        r"window function.*?(\w+)\s+is not",
        r"(\w+)\s+cannot be used as a window function",
        "ORDER BY key must be numeric in a RANGE-based window",
        "window function calls cannot be nested",
    ],
    'ORDER BY': [
        "ORDER BY term does not match any column",
    ],
    'LIMIT': [],
    'UNION': [
        "BY clause should come after UNION ALL",
    ],
    'INTERSECT': [],
    'EXCEPT': [],
    'WITH': [],
    'SET': []
}


def find_error_operator(sql: str, error_message: str = None, error_position: tuple[int] = None, dialect: str = None) -> str:
    """
    Определяет оператор SQL запроса, в котором произошла ошибка, используя AST из sqlglot.
    
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
            if isinstance(pattern, tuple) and all(part in error_message for part in pattern):
                return group
            elif isinstance(pattern, str):
                if pattern in error_message or re.match(pattern, error_message):
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


def normalize_sql(sql: str) -> str:
    """
    Нормализует SQL для поиска: удаляет комментарии, нормализует пробелы.
    """
    # Удаляем однострочные комментарии
    sql = re.sub(r'--[^\n]*', '', sql)
    # Удаляем многострочные комментарии
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    return sql


def analyze_sql_context(text_before):
    """
    Анализирует контекст SQL для определения текущего оператора.
    """    
    clean_text = normalize_sql(text_before)
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
                    
                    # Проверяем CASE выражения
                    if isinstance(expr, exp.Case):
                        if _check_case_expression(expr, entity_name):
                            return True

    except Exception:
        pass

    return False


def _check_case_expression(case_node, entity_name):
    """
    Проверяет CASE выражение на наличие сущности.
    """
    try:
        # Проверяем условия WHEN
        if 'ifs' in case_node.args:
            for if_clause in case_node.args['ifs']:
                if isinstance(if_clause, exp.If):
                    # Проверяем условие и результат
                    if hasattr(if_clause, 'args'):
                        for arg in if_clause.args.values():
                            if _entity_in_subtree(arg, entity_name):
                                return True
        
        # Проверяем ELSE
        if 'default' in case_node.args:
            default = case_node.args['default']
            if default and _entity_in_subtree(default, entity_name):
                return True
        
        # Рекурсивно проверяем все подузлы
        for child in case_node.walk():
            if isinstance(child, (exp.Anonymous, exp.Func)):
                if hasattr(child, 'name') and child.name and child.name.lower() == entity_name.lower():
                    return True
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
    entity_name_lower = entity_name.lower().replace('\"', '')
    try:
        for subnode in node.walk():
            if isinstance(subnode, exp.Column):
                # if subnode.table:
                #     print('.'.join([subnode.table, subnode.name]).lower(), "|", entity_name_lower)
                if (
                    subnode.name and subnode.name.lower() == entity_name_lower 
                    or (subnode.table
                        and '.'.join([subnode.table, subnode.name]).lower() == entity_name_lower)
                    or (subnode.db and subnode.table
                        and '.'.join([subnode.db, subnode.table, subnode.name]).lower() == entity_name_lower)
                    or (subnode.catalog and subnode.db and subnode.table
                        and '.'.join([subnode.catalog, subnode.db, subnode.table, subnode.name]).lower() == entity_name_lower)
                    ):
                    return True
            elif isinstance(subnode, exp.Table):
                if (
                    subnode.name and subnode.name.lower() == entity_name_lower 
                    or (subnode.db
                        and '.'.join([subnode.db, subnode.name]).lower() == entity_name_lower)
                    or (subnode.catalog and subnode.db
                        and '.'.join([subnode.catalog, subnode.db, subnode.name]).lower() == entity_name_lower)
                    ):
                    return True
            elif isinstance(subnode, (exp.TableAlias, exp.Anonymous, exp.Var, exp.Identifier)):
                if subnode.name and subnode.name.lower() == entity_name_lower:
                    return True
            elif isinstance(subnode, exp.Func):
                # Для стандартных функций
                if hasattr(subnode, 'sql_name'):
                    func_name = subnode.sql_name()
                    if func_name and func_name.lower() == entity_name_lower:
                        return True
                elif subnode.name and subnode.name.lower() == entity_name_lower:
                    return True
    except Exception as e:
        print(e)
        pass
    
    return False


# Другой алгоритм распознавания, на основе поиска подстрок

def find_error_operator_simple(sql: str, error_message: str = None, error_position: tuple[int] = None) -> str:
    """
    Определяет оператор SQL запроса, в котором произошла ошибка, используя простой алгоритм поиска подстрок. 
    Подходит для неоднозначных случаев, с которыми не справился `find_error_clause`.

    Args:
        sql (str): SQL запрос
        error_message (str, optional): Сообщение об ошибке. Defaults to None.
        error_position (tuple[int], optional): Координаты (строка, столбец) первого символа ошибки. Defaults to None.

    Returns:
        str: Название оператора (SELECT, FROM, JOIN, WHERE, и т.д.)
    """
    pos = None
    if error_position is not None:
        err_line, err_col = error_position
        err_line = 1 if err_line == 0 else err_line
        err_col = 1 if err_col == 0 else err_col

        # Считаем число символов в строках до ошибочной
        pos = len('\n'.join(sql.split('\n')[:err_line])) + 1 + err_col
    else:
        entity_name = extract_entity(error_message)
        if not entity_name:
            return 'UNKNOWN'
        
        sql = normalize_sql(sql).strip()
        pos, _ = find_entity_boundaries(sql, entity_name)
        if pos is None:
            print('not found')
            return 'UNKNOWN'

    # Поиск ближайшего оператора
    operators = [
        (r'\bSELECT\b', 'SELECT'),
        (r'\bFROM\b', 'FROM'),
        (r'\b(?:LEFT\s+|RIGHT\s+|INNER\s+|OUTER\s+|FULL\s+|CROSS\s+)?JOIN\b', 'JOIN'),
        (r'\bWHERE\b', 'WHERE'),
        (r'\bGROUP\s+BY\b', 'GROUP BY'),
        (r'\bHAVING\b', 'HAVING'),
        (r'\bORDER\s+BY\b', 'ORDER BY'),
        (r'\bLIMIT\b', 'LIMIT'),
        (r'\bOVER\b', 'WINDOW'),
    ]
    min_dist = 1e16
    nearest_operator = 'UNKNOWN'
    truncated_sql = sql[:pos].upper()
    for pattern, op_name in operators:
        for match in re.finditer(pattern, truncated_sql):
            if pos - match.start() < min_dist:
                min_dist = pos - match.start()
                nearest_operator = op_name

    return nearest_operator


def find_entity_boundaries(sql: str, entity_name: str) -> list[tuple[int, int]]:
    """
    Находит все вхождения сущности как отдельного слова/идентификатора.
    Возвращает список кортежей (start, end) с позициями в sql.
    """
    matches = []
    
    # Очищаем имя сущности от кавычек
    clean_entity = entity_name.strip('"\'`')
    
    # Если сущность содержит точку (table.column), разбиваем на части
    if '.' in clean_entity:
        parts = clean_entity.split('.')
        # Очищаем каждую часть от кавычек
        parts = [p.strip('"\'`') for p in parts]
        
        # Создаем паттерны для разных комбинаций кавычек
        patterns = build_qualified_name_patterns(parts)
    else:
        # Простое имя - ищем с разными вариантами кавычек
        patterns = build_simple_name_patterns(clean_entity)
    
    sql_lower = sql.lower()
    
    for pattern in patterns:
        for match in re.finditer(pattern, sql_lower, re.IGNORECASE):
            start = match.start(1) if match.lastindex and match.lastindex >= 1 else match.start()
            end = match.end(1) if match.lastindex and match.lastindex >= 1 else match.end()
            
            # Проверяем, не является ли это частью большего слова
            if is_valid_entity_context(sql, start, end, clean_entity):
                return (start, end)
    
    return None, None
    # matches = list(set(matches))
    # matches.sort()
    
    # return matches


def build_qualified_name_patterns(parts: list[str]) -> list[str]:
    """
    Создает паттерны для поиска квалифицированных имен (table.column).
    Учитывает все возможные комбинации кавычек.
    """
    patterns = []
    
    # Возможные варианты кавычек для каждой части
    quote_options = [
        '',      # Без кавычек
        '"',     # Двойные кавычки
        '`',     # Обратные кавычки
        '\\[',   # Квадратные скобки (с открывающей)
    ]
    
    # Генерируем все комбинации кавычек для всех частей
    from itertools import product
    
    for quotes in product(quote_options, repeat=len(parts)):
        pattern_parts = []
        
        for i, (part, quote) in enumerate(zip(parts, quotes)):
            escaped_part = re.escape(part)
            
            if quote == '':
                pattern_parts.append(rf'({escaped_part})')
            elif quote == '\\[':
                pattern_parts.append(rf'\[({escaped_part})\]')
            else:
                pattern_parts.append(rf'{quote}({escaped_part}){quote}')
        
        # Объединяем части через точку (экранированную)
        pattern = r'\.'.join(pattern_parts)
        
        # Добавляем границы слова
        pattern = rf'(?<![a-zA-Z0-9_"`.\[]){pattern}(?![a-zA-Z0-9_"`.\]])'
        
        patterns.append(pattern)
    
    return patterns


def build_simple_name_patterns(entity_name: str) -> list[str]:
    """
    Создает паттерны для поиска простого имени с разными кавычками.
    """
    escaped = re.escape(entity_name)
    
    patterns = [
        # Без кавычек
        rf'(?<![a-zA-Z0-9_"`.\[])({escaped})(?![a-zA-Z0-9_"`.\]])',
        
        # В двойных кавычках
        rf'(?<![a-zA-Z0-9_`.\[])(")({escaped})(")(?![a-zA-Z0-9_`.\]])',
        
        # В обратных кавычках
        rf'(?<![a-zA-Z0-9_".\[])(`)({escaped})(`)(?![a-zA-Z0-9_".\]])',
        
        # В квадратных скобках
        rf'(?<![a-zA-Z0-9_"`.])(\[)({escaped})(\])(?![a-zA-Z0-9_"`.])',
    ]
    
    return patterns


def is_valid_entity_context(sql: str, start: int, end: int, entity_name: str) -> bool:
    """
    Проверяет, является ли найденная подстрока полноценной сущностью,
    а не частью другого идентификатора или строки.
    """
    # Проверяем, не находится ли сущность внутри строкового литерала
    if is_inside_string_literal(sql, start, end):
        return False
    
    return True


def is_inside_string_literal(sql: str, start: int, end: int) -> bool:
    """
    Проверяет, находится ли позиция внутри строкового литерала.
    """
    # Считаем одинарные кавычки до позиции
    single_quotes_count = 0
    i = 0
    
    while i < start:
        if sql[i] == "'":
            # Проверяем, не экранированная ли кавычка ('' = одна кавычка)
            if i + 1 < len(sql) and sql[i + 1] == "'":
                i += 1  # Пропускаем вторую кавычку
            else:
                single_quotes_count += 1
        elif sql[i] == '\\' and i + 1 < len(sql):
            i += 1  # Пропускаем экранированный символ
        i += 1
    
    # Если нечетное количество - мы внутри строки
    return single_quotes_count % 2 == 1


if __name__ == "__main__":  
    # Тест с ошибкой "no such function: GREATEST"
    # error_msg = "no such function: GREATEST"
    # result = find_error_operator(sql, error_message=error_msg, dialect='sqlite')
    # print(f"Ошибка: {error_msg}")
    # print(f"Оператор: {result}")
    
    # # Тест с позицией ошибки (строка 53, столбец 45 - где LEAST/GREATEST)
    # result_pos = find_error_operator(sql, error_position=(53, 45), dialect='sqlite')
    # print(f"\nПозиция ошибки: строка 53, столбец 45")
    # print(f"Оператор: {result_pos}")

    tests = [
        ("WITH driver_season AS (  SELECT d.driver_id,         d.forename,         d.surname,         rg.year,         MIN(rg.round) OVER (PARTITION BY r.driver_id, rg.year) AS first_round,         MAX(rg.round) OVER (PARTITION BY r.driver_id, rg.year) AS last_round,         COUNT(DISTINCT rg.round) OVER (PARTITION BY r.driver_id, rg.year) AS round_cnt,         FIRST_VALUE(r.constructor_id) OVER (PARTITION BY r.driver_id, rg.year ORDER BY rg.round ASC) AS first_constructor,         LAST_VALUE(r.constructor_id) OVER (PARTITION BY r.driver_id, rg.year ORDER BY rg.round ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_constructor  FROM drivers d  JOIN results r ON d.driver_id = r.driver_id  JOIN races rg ON r.race_id = rg.race_id  WHERE rg.year BETWEEN 1950 AND 1959 ) SELECT DISTINCT forename, surname, year FROM driver_season WHERE first_round < last_round   AND first_constructor = last_constructor   AND round_cnt >= 2;", 
        "DISTINCT is not supported for window functions"),
        ("WITH cutoff_weeks AS (   SELECT 2018 AS year, DATE '2018-06-11' AS cutoff_week_start UNION ALL   SELECT 2019, DATE '2019-06-10' UNION ALL   SELECT 2020, DATE '2020-06-08' ), leading_weeks AS (   SELECT      cw.year,     SUM(ws.sales) AS leading_sales   FROM cutoff_weeks cw   JOIN weekly_sales ws      ON EXTRACT(YEAR FROM ws.week_date) = cw.year     AND ws.week_date BETWEEN cw.cutoff_week_start - INTERVAL '28 days' AND cw.cutoff_week_start - INTERVAL '7 days'   GROUP BY cw.year ), following_weeks AS (   SELECT      cw.year,     SUM(ws.sales) AS following_sales   FROM cutoff_weeks cw   JOIN weekly_sales ws      ON EXTRACT(YEAR FROM ws.week_date) = cw.year     AND ws.week_date BETWEEN cw.cutoff_week_start + INTERVAL '7 days' AND cw.cutoff_week_start + INTERVAL '28 days'   GROUP BY cw.year ) SELECT    lw.year,   lw.leading_sales,   fw.following_sales,   ( (fw.following_sales - lw.leading_sales) / NULLIF(lw.leading_sales, 0) ) * 100 AS pct_change FROM leading_weeks lw JOIN following_weeks fw ON lw.year = fw.year ORDER BY lw.year;", 
        "near \"AS\": syntax error"),
        ("""WITH stopwords_cte AS ( SELECT ARRAY_CONSTRUCT( 'a','about','above','after','again','against','ain','all','am','an','and','any','are','aren','arent','as','at','be','because','been','before','being','below','between','both','but','by','can','couldn','couldnt','d','did','didn','didnt','do','does','doesn','doesnt','doing','don','dont','down','during','each','few','for','from','further','had','hadn','hadnt','has','hasn','hasnt','have','haven','havent','having','he','her','here','hers','herself','him','himself','his','how','i','if','in','into','is','isn','isnt','it','its','itself','just','ll','m','ma','me','mightn','mightnt','more','most','mustn','mustnt','my','myself','needn','neednt','no','nor','not','now','o','of','off','on','once','only','or','other','our','ours','ourselves','out','over','own','re','s','same','shan','shant','she','shes','should','shouldn','shouldnt','shouldve','so','some','such','t','than','that','thatll','the','their','theirs','them','themselves','then','there','these','they','this','those','through','to','too','under','until','up','ve','very','was','wasn','wasnt','we','were','weren','werent','what','when','where','which','while','who','whom','why','will','with','won','wont','wouldn','wouldnt','y','you','youd','youll','your','youre','yours','yourself','yourselves','youve' ) AS stop_array ), tokenized_cte AS ( SELECT "id", "title", "date", REGEXP_EXTRACT_ALL("body", '[a-zA-Z0-9]+') AS tokens FROM WORD_VECTORS_US.WORD_VECTORS_US.NATURE WHERE "body" IS NOT NULL ), words_cte AS ( SELECT t.id, t.title, t.date, f.value AS word_original, LOWER(TRIM(f.value)) AS word_lower FROM tokenized_cte t, LATERAL FLATTEN(input => t.tokens) AS f WHERE TRIM(f.value) != '' AND NOT EXISTS ( SELECT 1 FROM TABLE(FLATTEN(INPUT => (SELECT stop_array FROM stopwords_cte))) s WHERE s.value = LOWER(TRIM(f.value)) ) ), joined_cte AS ( SELECT w.id, w.title, w.date, w.word_lower, g.vector AS glove_vector, wf.frequency AS frequency FROM words_cte w JOIN WORD_VECTORS_US.WORD_VECTORS_US.GLOVE_VECTORS g ON LOWER(g."word") = w.word_lower JOIN WORD_VECTORS_US.WORD_VECTORS_US.WORD_FREQUENCIES wf ON LOWER(wf."word") = w.word_lower ), weighted_elements AS ( SELECT j.id, j.title, j.date, f.seq AS vector_index, f.value AS vec_value, j.frequency, f.value / POWER(j.frequency, 0.4) AS weighted_value FROM joined_cte j, LATERAL FLATTEN(input => j.glove_vector) AS f ), article_vectors AS ( SELECT id, title, date, vector_index, SUM(weighted_value) AS vector_value FROM weighted_elements GROUP BY id, title, date, vector_index ), norms AS ( SELECT id, SQRT(SUM(vector_value * vector_value)) AS norm FROM article_vectors GROUP BY id ), target_vector AS ( SELECT id, vector_index, vector_value FROM article_vectors WHERE id = '8a78ef2d-d5f7-4d2d-9b47-5adb25cbd373' ), dot_products AS ( SELECT v2.id, SUM(v1.vector_value * v2.vector_value) AS dot_product FROM target_vector v1 JOIN article_vectors v2 ON v1.vector_index = v2.vector_index WHERE v2.id != '8a78ef2d-d5f7-4d2d-9b47-5adb25cbd373' GROUP BY v2.id ), similarity AS ( SELECT dp.id, dp.dot_product, n2.norm AS article_norm, (SELECT norm FROM norms WHERE id = '8a78ef2d-d5f7-4d2d-9b47-5adb25cbd373') AS target_norm FROM dot_products dp JOIN norms n2 ON dp.id = n2.id ), article_metadata AS ( SELECT DISTINCT "id" AS id, "title" AS title, "date" AS date FROM WORD_VECTORS_US.WORD_VECTORS_US.NATURE ) SELECT a.id, a.date, a.title, ROUND(s.dot_product / (s.article_norm * s.target_norm), 4) AS cosine_similarity FROM similarity s JOIN article_metadata a ON s.id = a.id ORDER BY cosine_similarity DESC LIMIT 10;""", 
         "invalid identifier 'T.ID'"),
        ("""WITH stopwords_cte AS ( SELECT ARRAY_CONSTRUCT( 'a', 'about', 'above', 'after', 'again', 'against', 'ain', 'all', 'am', 'an', 'and', 'any', 'are', 'aren', 'arent', 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', 'can', 'couldn', 'couldnt', 'd', 'did', 'didn', 'didnt', 'do', 'does', 'doesn', 'doesnt', 'doing', 'don', 'dont', 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', 'hadn', 'hadnt', 'has', 'hasn', 'hasnt', 'have', 'haven', 'havent', 'having', 'he', 'her', 'here', 'hers', 'herself', 'him', 'himself', 'his', 'how', 'i', 'if', 'in', 'into', 'is', 'isn', 'isnt', 'it', 'its', 'itself', 'just', 'll', 'm', 'ma', 'me', 'mightn', 'mightnt', 'more', 'most', 'mustn', 'mustnt', 'my', 'myself', 'needn', 'neednt', 'no', 'nor', 'not', 'now', 'o', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'our', 'ours', 'ourselves', 'out', 'over', 'own', 're', 's', 'same', 'shan', 'shant', 'she', 'shes', 'should', 'shouldn', 'shouldnt', 'shouldve', 'so', 'some', 'such', 't', 'than', 'that', 'thatll', 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there', 'these', 'they', 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 've', 'very', 'was', 'wasn', 'wasnt', 'we', 'were', 'weren', 'werent', 'what', 'when', 'where', 'which', 'while', 'who', 'whom', 'why', 'will', 'with', 'won', 'wont', 'wouldn', 'wouldnt', 'y', 'you', 'youd', 'youll', 'your', 'youre', 'yours', 'yourself', 'yourselves', 'youve' ) AS stop_array ), tokenized_cte AS ( SELECT "id", "title", "date", REGEXP_EXTRACT_ALL("body", '[a-zA-Z0-9]+') AS tokens FROM WORD_VECTORS_US.WORD_VECTORS_US.NATURE WHERE "body" IS NOT NULL ), words_cte AS ( SELECT t.id, t.title, t.date, f.value AS word_original, LOWER(TRIM(f.value)) AS word_lower FROM tokenized_cte t, LATERAL FLATTEN(input => t.tokens) AS f WHERE TRIM(f.value) != '' AND NOT ARRAY_CONTAINS(LOWER(TRIM(f.value)), (SELECT stop_array FROM stopwords_cte)) ), joined_cte AS ( SELECT w.id, w.title, w.date, w.word_lower, g.vector AS glove_vector, wf.frequency FROM words_cte w JOIN WORD_VECTORS_US.WORD_VECTORS_US.GLOVE_VECTORS g ON LOWER(g.word) = w.word_lower JOIN WORD_VECTORS_US.WORD_VECTORS_US.WORD_FREQUENCIES wf ON LOWER(wf.word) = w.word_lower ), weighted_elements AS ( SELECT j.id, j.title, j.date, f.idx AS vector_index, f.val AS vec_value, j.frequency, f.val / POWER(j.frequency, 0.4) AS weighted_value FROM joined_cte j, LATERAL FLATTEN(input => j.glove_vector) AS f(seq, idx, val) ), article_vectors AS ( SELECT id, title, date, vector_index, SUM(weighted_value) AS vector_value FROM weighted_elements GROUP BY id, title, date, vector_index ), norms AS ( SELECT id, SQRT(SUM(vector_value * vector_value)) AS norm FROM article_vectors GROUP BY id ), target_vector AS ( SELECT id, vector_index, vector_value FROM article_vectors WHERE id = '8a78ef2d-d5f7-4d2d-9b47-5adb25cbd373' ), dot_products AS ( SELECT v2.id, SUM(v1.vector_value * v2.vector_value) AS dot_product FROM target_vector v1 JOIN article_vectors v2 ON v1.vector_index = v2.vector_index WHERE v2.id != '8a78ef2d-d5f7-4d2d-9b47-5adb25cbd373' GROUP BY v2.id ), similarity AS ( SELECT dp.id, dp.dot_product, n2.norm AS article_norm, nt.norm AS target_norm FROM dot_products dp JOIN norms n2 ON dp.id = n2.id CROSS JOIN (SELECT norm FROM norms WHERE id = '8a78ef2d-d5f7-4d2d-9b47-5adb25cbd373') AS nt ), article_metadata AS ( SELECT DISTINCT id, title, date FROM WORD_VECTORS_US.WORD_VECTORS_US.NATURE ) SELECT a.id, a.date, a.title, ROUND(s.dot_product / (s.article_norm * s.target_norm), 4) AS cosine_similarity FROM similarity s JOIN article_metadata a ON s.id = a.id ORDER BY cosine_similarity DESC LIMIT 10;""", 
         "invalid identifier 'T.ID'"),
        ("""WITH ref_id AS ( SELECT '8a78ef2d-d5f7-4d2d-9b47-5adb25cbd373' AS ref_id ), stopwords_array AS ( SELECT ARRAY_CONSTRUCT( 'a', 'about', 'above', 'after', 'again', 'against', 'ain', 'all', 'am', 'an', 'and', 'any', 'are', 'aren', 'arent', 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', 'can', 'couldn', 'couldnt', 'd', 'did', 'didn', 'didnt', 'do', 'does', 'doesn', 'doesnt', 'doing', 'don', 'dont', 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', 'hadn', 'hadnt', 'has', 'hasn', 'hasnt', 'have', 'haven', 'havent', 'having', 'he', 'her', 'here', 'hers', 'herself', 'him', 'himself', 'his', 'how', 'i', 'if', 'in', 'into', 'is', 'isn', 'isnt', 'it', 'its', 'itself', 'just', 'll', 'm', 'ma', 'me', 'mightn', 'mightnt', 'more', 'most', 'mustn', 'mustnt', 'my', 'myself', 'needn', 'neednt', 'no', 'nor', 'not', 'now', 'o', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'our', 'ours', 'ourselves', 'out', 'over', 'own', 're', 's', 'same', 'shan', 'shant', 'she', 'shes', 'should', 'shouldn', 'shouldnt', 'shouldve', 'so', 'some', 'such', 't', 'than', 'that', 'thatll', 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there', 'these', 'they', 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 've', 'very', 'was', 'wasn', 'wasnt', 'we', 'were', 'weren', 'werent', 'what', 'when', 'where', 'which', 'while', 'who', 'whom', 'why', 'will', 'with', 'won', 'wont', 'wouldn', 'wouldnt', 'y', 'you', 'youd', 'youll', 'your', 'youre', 'yours', 'yourself', 'yourselves', 'youve' ) AS stopwords ), tokenized AS ( SELECT n."id", n."title", n."date", LOWER(f.value) AS word FROM WORD_VECTORS_US.WORD_VECTORS_US.NATURE n, LATERAL FLATTEN(REGEXP_EXTRACT_ALL( REGEXP_REPLACE(n."body", 'вЂ™|''s(\\W)', '\\1'), '((?:\\d+(?:,\\d+)*(?:\\.\\d+)?)+|(?:[\\w])+)' )) f WHERE LOWER(f.value) NOT IN (SELECT VALUE FROM stopwords_array, LATERAL FLATTEN(stopwords)) ), glove_lower AS ( SELECT LOWER("word") AS word, PARSE_JSON(TO_VARCHAR("vector")) AS vector FROM WORD_VECTORS_US.WORD_VECTORS_US.GLOVE_VECTORS ), wf_lower AS ( SELECT LOWER("word") AS word, "frequency" AS frequency FROM WORD_VECTORS_US.WORD_VECTORS_US.WORD_FREQUENCIES ), joined AS ( SELECT t."id", t."title", t."date", t.word, g.vector, wf.frequency FROM tokenized t LEFT JOIN glove_lower g ON t.word = g.word LEFT JOIN wf_lower wf ON t.word = wf.word WHERE g.vector IS NOT NULL AND wf.frequency IS NOT NULL ), weighted_vectors AS ( SELECT id, title, date, word, vector, frequency, 1 / POWER(frequency, 0.4) AS weight FROM joined ), expanded AS ( SELECT id, title, date, f.index AS dim, f.value * weight AS weighted_value FROM weighted_vectors, LATERAL FLATTEN(vector) f ), aggregated AS ( SELECT id, title, date, dim, SUM(weighted_value) AS agg_value FROM expanded GROUP BY id, title, date, dim ), norms AS ( SELECT id, SQRT(SUM(agg_value * agg_value)) AS norm FROM aggregated GROUP BY id HAVING norm > 0 ), ref_aggregated AS ( SELECT dim, agg_value FROM aggregated WHERE id = (SELECT ref_id FROM ref_id) ), ref_norm_value AS ( SELECT norm FROM norms WHERE id = (SELECT ref_id FROM ref_id) ), similarity_raw AS ( SELECT a."id", a."date", a."title", COALESCE(SUM(a.agg_value * r.agg_value), 0) / (n_a.norm * (SELECT norm FROM ref_norm_value)) AS cosine_similarity_raw FROM aggregated a JOIN norms n_a ON a."id" = n_a."id" LEFT JOIN ref_aggregated r ON a.dim = r.dim WHERE a."id" != (SELECT ref_id FROM ref_id) GROUP BY a."id", a."date", a."title", n_a.norm ) SELECT "id", "date", "title", ROUND(cosine_similarity_raw, 4) AS cosine_similarity FROM similarity_raw ORDER BY cosine_similarity_raw DESC LIMIT 10;""", 
         "invalid identifier 'ID'"),
        ("""WITH stopwords_list AS ( SELECT ARRAY_CONSTRUCT( 'a','about','above','after','again','against','ain','all','am','an','and','any','are','aren','arent','as','at','be','because','been','before','being','below','between','both','but','by','can','couldn','couldnt','d','did','didn','didnt','do','does','doesn','doesnt','doing','don','dont','down','during','each','few','for','from','further','had','hadn','hadnt','has','hasn','hasnt','have','haven','havent','having','he','her','here','hers','herself','him','himself','his','how','i','if','in','into','is','isn','isnt','it','its','itself','just','ll','m','ma','me','mightn','mightnt','more','most','mustn','mustnt','my','myself','needn','neednt','no','nor','not','now','o','of','off','on','once','only','or','other','our','ours','ourselves','out','over','own','re','s','same','shan','shant','she','shes','should','shouldn','shouldnt','shouldve','so','some','such','t','than','that','thatll','the','their','theirs','them','themselves','then','there','these','they','this','those','through','to','too','under','until','up','ve','very','was','wasn','wasnt','we','were','weren','werent','what','when','where','which','while','who','whom','why','will','with','won','wont','wouldn','wouldnt','y','you','youd','youll','your','youre','yours','yourself','yourselves','youve' ) AS stopwords ), cleaned_articles AS ( SELECT "id", REGEXP_REPLACE( REGEXP_REPLACE("body", 'вЂ™', ''), '''s(\\W)', '\\1' ) AS cleaned_text FROM WORD_VECTORS_US.WORD_VECTORS_US.NATURE WHERE "body" IS NOT NULL ), tokenized AS ( SELECT c."id", LOWER(f.value) AS word FROM cleaned_articles c, LATERAL FLATTEN(input => REGEXP_EXTRACT_ALL(c.cleaned_text, '((?:\\d+(?:,\\d+)*(?:\\.\\d+)?)+|(?:[\\w])+)')) f WHERE f.value IS NOT NULL ), filtered_tokens AS ( SELECT "id", word FROM tokenized WHERE word NOT IN (SELECT VALUE FROM stopwords_list, LATERAL FLATTEN(stopwords)) AND word != '' ), token_counts AS ( SELECT "id", word, COUNT(*) AS word_count FROM filtered_tokens GROUP BY "id", word ), word_vectors_weighted AS ( SELECT tc."id", f."index" AS dimension, SUM( (1 / POWER(fr.frequency, 0.4)) * tc.word_count * f.value ) AS weighted_sum FROM token_counts tc JOIN WORD_VECTORS_US.WORD_VECTORS_US.GLOVE_VECTORS gv ON tc.word = gv."word" JOIN WORD_VECTORS_US.WORD_VECTORS_US.WORD_FREQUENCIES fr ON tc.word = fr."word" CROSS JOIN LATERAL FLATTEN(input => gv."vector") f GROUP BY tc."id", f."index" ), article_norms AS ( SELECT "id", SQRT(SUM(POWER(weighted_sum, 2))) AS norm FROM word_vectors_weighted GROUP BY "id" HAVING norm > 0 ), target_norm AS ( SELECT norm FROM article_norms WHERE "id" = '8a78ef2d-d5f7-4d2d-9b47-5adb25cbd373' ), similarities AS ( SELECT a."id", COALESCE(SUM(a.weighted_sum * t.weighted_sum), 0) / (an.norm * tn.norm) AS cosine_similarity FROM word_vectors_weighted a LEFT JOIN word_vectors_weighted t ON a.dimension = t.dimension AND t."id" = '8a78ef2d-d5f7-4d2d-9b47-5adb25cbd373' JOIN article_norms an ON a."id" = an."id" CROSS JOIN target_norm tn WHERE a."id" != '8a78ef2d-d5f7-4d2d-9b47-5adb25cbd373' GROUP BY a."id", an.norm, tn.norm ) SELECT n."id", n."date", n."title", ROUND(s.cosine_similarity, 4) AS cosine_similarity FROM similarities s JOIN WORD_VECTORS_US.WORD_VECTORS_US.NATURE n ON s."id" = n."id" ORDER BY s.cosine_similarity DESC LIMIT 10; """, 
         "invalid identifier 'F.\"index\"'"),
        ("""WITH stopwords_cte AS ( SELECT ARRAY_CONSTRUCT( 'a','about','above','after','again','against','ain','all','am','an','and','any','are','aren','arent','as','at','be','because','been','before','being','below','between','both','but','by','can','couldn','couldnt','d','did','didn','didnt','do','does','doesn','doesnt','doing','don','dont','down','during','each','few','for','from','further','had','hadn','hadnt','has','hasn','hasnt','have','haven','havent','having','he','her','here','hers','herself','him','himself','his','how','i','if','in','into','is','isn','isnt','it','its','itself','just','ll','m','ma','me','mightn','mightnt','more','most','mustn','mustnt','my','myself','needn','neednt','no','nor','not','now','o','of','off','on','once','only','or','other','our','ours','ourselves','out','over','own','re','s','same','shan','shant','she','shes','should','shouldn','shouldnt','shouldve','so','some','such','t','than','that','thatll','the','their','theirs','them','themselves','then','there','these','they','this','those','through','to','too','under','until','up','ve','very','was','wasn','wasnt','we','were','weren','werent','what','when','where','which','while','who','whom','why','will','with','won','wont','wouldn','wouldnt','y','you','youd','youll','your','youre','yours','yourself','yourselves','youve' ) AS stop_array ), tokenized_cte AS ( SELECT "id", "title", "date", REGEXP_EXTRACT_ALL("body", '[a-zA-Z0-9]+') AS tokens FROM WORD_VECTORS_US.WORD_VECTORS_US.NATURE WHERE "body" IS NOT NULL ), words_cte AS ( SELECT t.id, t.title, t.date, f.value AS word_original, LOWER(TRIM(f.value)) AS word_lower FROM tokenized_cte t, LATERAL FLATTEN(input => t.tokens) AS f WHERE TRIM(f.value) != '' AND NOT EXISTS ( SELECT 1 FROM TABLE(FLATTEN(INPUT => (SELECT stop_array FROM stopwords_cte))) s WHERE s.value = LOWER(TRIM(f.value)) ) ), joined_cte AS ( SELECT w.id, w.title, w.date, w.word_lower, g.vector AS glove_vector, wf.frequency AS frequency FROM words_cte w JOIN WORD_VECTORS_US.WORD_VECTORS_US.GLOVE_VECTORS g ON LOWER(g."word") = w.word_lower JOIN WORD_VECTORS_US.WORD_VECTORS_US.WORD_FREQUENCIES wf ON LOWER(wf."word") = w.word_lower ), weighted_elements AS ( SELECT j.id, j.title, j.date, f.seq AS vector_index, f.value AS vec_value, j.frequency, f.value / POWER(j.frequency, 0.4) AS weighted_value FROM joined_cte j, LATERAL FLATTEN(input => j.glove_vector) AS f ), article_vectors AS ( SELECT id, title, date, vector_index, SUM(weighted_value) AS vector_value FROM weighted_elements GROUP BY id, title, date, vector_index ), norms AS ( SELECT id, SQRT(SUM(vector_value * vector_value)) AS norm FROM article_vectors GROUP BY id ), target_vector AS ( SELECT id, vector_index, vector_value FROM article_vectors WHERE id = '8a78ef2d-d5f7-4d2d-9b47-5adb25cbd373' ), dot_products AS ( SELECT v2.id, SUM(v1.vector_value * v2.vector_value) AS dot_product FROM target_vector v1 JOIN article_vectors v2 ON v1.vector_index = v2.vector_index WHERE v2.id != '8a78ef2d-d5f7-4d2d-9b47-5adb25cbd373' GROUP BY v2.id ), similarity AS ( SELECT dp.id, dp.dot_product, n2.norm AS article_norm, (SELECT norm FROM norms WHERE id = '8a78ef2d-d5f7-4d2d-9b47-5adb25cbd373') AS target_norm FROM dot_products dp JOIN norms n2 ON dp.id = n2.id ), article_metadata AS ( SELECT DISTINCT "id" AS id, "title" AS title, "date" AS date FROM WORD_VECTORS_US.WORD_VECTORS_US.NATURE ) SELECT a.id, a.date, a.title, ROUND(s.dot_product / (s.article_norm * s.target_norm), 4) AS cosine_similarity FROM similarity s JOIN article_metadata a ON s.id = a.id ORDER BY cosine_similarity DESC LIMIT 10; """,
         "invalid identifier 'T.ID'"),
        ("""WITH stopwords AS ( SELECT word FROM ( VALUES ('a'), ('about'), ('above'), ('after'), ('again'), ('against'), ('ain'), ('all'), ('am'), ('an'), ('and'), ('any'), ('are'), ('aren'), ('arent'), ('as'), ('at'), ('be'), ('because'), ('been'), ('before'), ('being'), ('below'), ('between'), ('both'), ('but'), ('by'), ('can'), ('couldn'), ('couldnt'), ('d'), ('did'), ('didn'), ('didnt'), ('do'), ('does'), ('doesn'), ('doesnt'), ('doing'), ('don'), ('dont'), ('down'), ('during'), ('each'), ('few'), ('for'), ('from'), ('further'), ('had'), ('hadn'), ('hadnt'), ('has'), ('hasn'), ('hasnt'), ('have'), ('haven'), ('havent'), ('having'), ('he'), ('her'), ('here'), ('hers'), ('herself'), ('him'), ('himself'), ('his'), ('how'), ('i'), ('if'), ('in'), ('into'), ('is'), ('isn'), ('isnt'), ('it'), ('its'), ('itself'), ('just'), ('ll'), ('m'), ('ma'), ('me'), ('mightn'), ('mightnt'), ('more'), ('most'), ('mustn'), ('mustnt'), ('my'), ('myself'), ('needn'), ('neednt'), ('no'), ('nor'), ('not'), ('now'), ('o'), ('of'), ('off'), ('on'), ('once'), ('only'), ('or'), ('other'), ('our'), ('ours'), ('ourselves'), ('out'), ('over'), ('own'), ('re'), ('s'), ('same'), ('shan'), ('shant'), ('she'), ('shes'), ('should'), ('shouldn'), ('shouldnt'), ('shouldve'), ('so'), ('some'), ('such'), ('t'), ('than'), ('that'), ('thatll'), ('the'), ('their'), ('theirs'), ('them'), ('themselves'), ('then'), ('there'), ('these'), ('they'), ('this'), ('those'), ('through'), ('to'), ('too'), ('under'), ('until'), ('up'), ('ve'), ('very'), ('was'), ('wasn'), ('wasnt'), ('we'), ('were'), ('weren'), ('werent'), ('what'), ('when'), ('where'), ('which'), ('while'), ('who'), ('whom'), ('why'), ('will'), ('with'), ('won'), ('wont'), ('wouldn'), ('wouldnt'), ('y'), ('you'), ('youd'), ('youll'), ('your'), ('youre'), ('yours'), ('yourself'), ('yourselves'), ('youve') ) AS t(word) ), article_tokens AS ( SELECT n.id, n.date, n.title, t.value AS word FROM WORD_VECTORS_US.WORD_VECTORS_US.NATURE n, LATERAL REGEXP_SPLIT_TO_TABLE(REPLACE(REPLACE(LOWER(n.body), '''', ''), '''s', ''), '[^a-z0-9]+') AS t WHERE t.value <> '' AND t.value NOT IN (SELECT word FROM stopwords) ), word_vectors_freq AS ( SELECT at.id, at.date, at.title, at.word, gv.vector, wf.frequency FROM article_tokens at INNER JOIN WORD_VECTORS_US.WORD_VECTORS_US.GLOVE_VECTORS gv ON at.word = gv.word INNER JOIN WORD_VECTORS_US.WORD_VECTORS_US.WORD_FREQUENCIES wf ON at.word = wf.word ), article_vectors AS ( SELECT wvf.id, wvf.date, wvf.title, fs.dimension, fs.value / POWER(wvf.frequency, 0.4) AS weighted_value FROM word_vectors_freq wvf, LATERAL FLATTEN(INPUT => wvf.vector) AS fs ), article_vectors_aggregated AS ( SELECT id, date, title, dimension, SUM(weighted_value) AS weighted_sum FROM article_vectors GROUP BY id, date, title, dimension ), article_norm AS ( SELECT id, date, title, SQRT(SUM(weighted_sum * weighted_sum)) AS norm FROM article_vectors_aggregated GROUP BY id, date, title ), target_data AS ( SELECT a.dimension, a.weighted_sum, n.norm AS target_norm FROM article_vectors_aggregated a JOIN article_norm n ON a.id = n.id WHERE a.id = '8a78ef2d-d5f7-4d2d-9b47-5adb25cbd373' ) SELECT a.id, a.date, a.title, SUM(a.weighted_sum * t.weighted_sum) / (a_norm.norm * t.target_norm) AS cosine_similarity FROM article_vectors_aggregated a JOIN article_norm a_norm ON a.id = a_norm.id JOIN target_data t ON a.dimension = t.dimension WHERE a.id != '8a78ef2d-d5f7-4d2d-9b47-5adb25cbd373' GROUP BY a.id, a.date, a.title, a_norm.norm, t.target_norm ORDER BY cosine_similarity DESC LIMIT 10; """, 
         "invalid identifier 'N.BODY'"),
        ("""WITH mr_series AS ( SELECT DISTINCT dp."StudyInstanceUID", dp."SeriesInstanceUID" FROM IDC.IDC_V17.DICOM_PIVOT dp WHERE dp."collection_id" = 'qin_prostate_repeatability' AND dp."Modality" = 'MR' AND dp."BodyPartExamined" ILIKE '%PROSTATE%' AND (dp."SeriesDescription" ILIKE '%T2%' OR dp."PulseSequenceName" ILIKE '%T2%') AND (dp."SeriesDescription" ILIKE '%AXIAL%' OR dp."ImageType" ILIKE '%AXIAL%') ), segmentation_series AS ( SELECT DISTINCT a."StudyInstanceUID", a."SeriesInstanceUID" FROM IDC.IDC_V17.SEGMENTATIONS s INNER JOIN IDC.IDC_V17.AUXILIARY_METADATA a ON s."SeriesInstanceUID" = a."SeriesInstanceUID" WHERE a."collection_id" = 'qin_prostate_repeatability' AND (s."AnatomicRegion":"CodeMeaning" = 'Peripheral zone' OR s."SegmentedPropertyType":"CodeMeaning" = 'Peripheral zone') ) SELECT DISTINCT mr."StudyInstanceUID" FROM mr_series mr INNER JOIN segmentation_series seg ON mr."StudyInstanceUID" = seg."StudyInstanceUID";""", 
         "invalid identifier 'DP.\"PulseSequenceName\"'"),
        ("""SELECT l."repo_name" AS "repo_name", COUNT(*) AS "commit_count" FROM GITHUB_REPOS.GITHUB_REPOS.LANGUAGES l JOIN GITHUB_REPOS.GITHUB_REPOS.SAMPLE_COMMITS c ON l."repo_name" = c."repo_name" WHERE l.language[0] = 'JavaScript' GROUP BY l."repo_name" ORDER BY "commit_count" DESC LIMIT 2;""", 
         "invalid identifier 'L.LANGUAGE'"),
        ("""WITH zip10019 AS ( SELECT TO_GEOGRAPHY("zip_code_geom") AS geom FROM NEW_YORK_CITIBIKE_1.GEO_US_BOUNDARIES.ZIP_CODES WHERE "zip_code" = '10019' ), central_park AS ( SELECT "usaf" AS "stn", "wban" AS "wban" FROM NEW_YORK_CITIBIKE_1.NOAA_GSOD.STATIONS WHERE "name" ILIKE '%CENTRAL PARK%' AND "state" = 'NY' LIMIT 1 ) SELECT t."usertype", ROUND(AVG(NULLIF(g."temp", 9999.9)), 4) AS "avg_temperature", ROUND(AVG(NULLIF(g."prcp", 99.99)), 4) AS "avg_precipitation", ROUND(AVG(NULLIF(TO_NUMBER(g."wdsp"), 999.9)), 4) AS "avg_wind_speed" FROM NEW_YORK_CITIBIKE_1.NEW_YORK_CITIBIKE.CITIBIKE_TRIPS t JOIN central_park cp ON 1=1 JOIN NEW_YORK_CITIBIKE_1.NOAA_GSOD.GSOD2018 g ON g."stn" = cp."stn" AND g."wban" = cp."wban" AND CONVERT_TIMEZONE('UTC', 'America/New_York', TO_TIMESTAMP(t."starttime"/1000000))::DATE = TO_DATE(g."year" || '-' || g."mo" || '-' || g."da") CROSS JOIN zip10019 zp WHERE EXTRACT(YEAR FROM CONVERT_TIMEZONE('UTC', 'America/New_York', TO_TIMESTAMP(t."starttime"/1000000))) = 2018 AND ST_WITHIN(ST_GEOGRAPHYPOINT(t."start_station_longitude", t."start_station_latitude"), zp.geom) AND ST_WITHIN(ST_GEOGRAPHYPOINT(t."end_station_longitude", t."end_station_latitude"), zp.geom) GROUP BY t."usertype" ORDER BY "avg_temperature" DESC LIMIT 1;""",
         "Unknown functions ST_GEOGRAPHYPOINT, ST_GEOGRAPHYPOINT"),
        ("""WITH trips_base AS ( SELECT t."usertype", t."start_station_latitude", t."start_station_longitude", t."end_station_latitude", t."end_station_longitude", CONVERT_TIMEZONE('UTC', 'America/New_York', TO_TIMESTAMP(t."starttime"/1000000)) AS "ts_ny" FROM NEW_YORK_CITIBIKE_1.NEW_YORK_CITIBIKE.CITIBIKE_TRIPS t WHERE EXTRACT(YEAR FROM CONVERT_TIMEZONE('UTC', 'America/New_York', TO_TIMESTAMP(t."starttime"/1000000))) = 2018 ), trips_with_zips AS ( SELECT "usertype", DATE("ts_ny") AS "trip_date", (SELECT z."zip_code" FROM NEW_YORK_CITIBIKE_1.GEO_US_BOUNDARIES.ZIP_CODES z WHERE ST_WITHIN(ST_GEOGPOINT("start_station_longitude", "start_station_latitude"), z."zip_code_geom") LIMIT 1) AS "start_zip", (SELECT z."zip_code" FROM NEW_YORK_CITIBIKE_1.GEO_US_BOUNDARIES.ZIP_CODES z WHERE ST_WITHIN(ST_GEOGPOINT("end_station_longitude", "end_station_latitude"), z."zip_code_geom") LIMIT 1) AS "end_zip" FROM trips_base ), central_park_wban AS ( SELECT s."wban" FROM NEW_YORK_CITIBIKE_1.NOAA_GSOD.STATIONS s JOIN NEW_YORK_CITIBIKE_1.NOAA_GSOD.GSOD2018 g ON s."wban" = g."wban" WHERE LOWER(s."name") LIKE '%CENTRAL PARK%' AND s."state" = 'NY' LIMIT 1 ), weather_data AS ( SELECT TO_DATE(g."year" || g."mo" || g."da", 'YYYYMMDD') AS "weather_date", g."temp", g."prcp", TRY_TO_DOUBLE(g."wdsp") AS "wdsp_num" FROM NEW_YORK_CITIBIKE_1.NOAA_GSOD.GSOD2018 g WHERE g."wban" = (SELECT "wban" FROM central_park_wban) AND g."temp" != 9999.9 AND g."prcp" != 99.99 AND TRY_TO_DOUBLE(g."wdsp") IS NOT NULL AND TRY_TO_DOUBLE(g."wdsp") != 999.9 ), trip_weather AS ( SELECT tw."usertype", w."temp", w."prcp", w."wdsp_num" FROM trips_with_zips tw JOIN weather_data w ON tw."trip_date" = w."weather_date" WHERE tw."start_zip" = '10019' AND tw."end_zip" = '10019' ) SELECT "usertype", ROUND(AVG("temp"), 4) AS "avg_temperature", ROUND(AVG("prcp"), 4) AS "avg_precipitation", ROUND(AVG("wdsp_num"), 4) AS "avg_wind_speed" FROM trip_weather GROUP BY "usertype" ORDER BY "avg_temperature" DESC LIMIT 1;""",
         "Unknown function ST_GEOGPOINT"),
        ("""WITH trips_base AS ( SELECT t."usertype", t."start_station_latitude" AS "start_lat", t."start_station_longitude" AS "start_lon", t."end_station_latitude" AS "end_lat", t."end_station_longitude" AS "end_lon", CONVERT_TIMEZONE('UTC', 'America/New_York', TO_TIMESTAMP(t."starttime"/1000000)) AS "start_time_nyc" FROM NEW_YORK_CITIBIKE_1.NEW_YORK_CITIBIKE.CITIBIKE_TRIPS t WHERE EXTRACT(YEAR FROM CONVERT_TIMEZONE('UTC', 'America/New_York', TO_TIMESTAMP(t."starttime"/1000000))) = 2018 ), trips_in_zip AS ( SELECT "usertype", DATE("start_time_nyc") AS "trip_date" FROM trips_base b WHERE EXISTS ( SELECT 1 FROM NEW_YORK_CITIBIKE_1.GEO_US_BOUNDARIES.ZIP_CODES z WHERE ST_WITHIN( ST_GEOGPOINT("start_lon", "start_lat"), ST_GEOGFROMWKB(z."zip_code_geom") ) AND z."zip_code" = '10019' ) AND EXISTS ( SELECT 1 FROM NEW_YORK_CITIBIKE_1.GEO_US_BOUNDARIES.ZIP_CODES z WHERE ST_WITHIN( ST_GEOGPOINT("end_lon", "end_lat"), ST_GEOGFROMWKB(z."zip_code_geom") ) AND z."zip_code" = '10019' ) ), central_park_weather AS ( SELECT TO_DATE(g."year" || g."mo" || g."da", 'YYYYMMDD') AS "weather_date", g."temp", g."prcp", TRY_TO_DOUBLE(g."wdsp") AS "wind_speed" FROM NEW_YORK_CITIBIKE_1.NOAA_GSOD.GSOD2018 g WHERE g."wban" = ( SELECT s."wban" FROM NEW_YORK_CITIBIKE_1.NOAA_GSOD.STATIONS s WHERE LOWER(s."name") LIKE '%central park%' AND s."state" = 'NY' LIMIT 1 ) AND g."temp" < 999.9 AND g."prcp" < 99.99 AND TRY_TO_DOUBLE(g."wdsp") < 999.9 ), trip_weather_join AS ( SELECT tz."usertype", cw."temp", cw."prcp", cw."wind_speed" FROM trips_in_zip tz JOIN central_park_weather cw ON tz."trip_date" = cw."weather_date" ) SELECT "usertype", ROUND(AVG("temp"), 4) AS "avg_temperature", ROUND(AVG("prcp"), 4) AS "avg_precipitation", ROUND(AVG("wind_speed"), 4) AS "avg_wind_speed" FROM trip_weather_join GROUP BY "usertype" ORDER BY "avg_temperature" DESC LIMIT 1;""", 
         "Unknown function ST_GEOGPOINT"),
        ("""WITH stations_10019 AS ( SELECT s."name" AS station_name FROM NEW_YORK_CITIBIKE_1.NEW_YORK_CITIBIKE.CITIBIKE_STATIONS s JOIN NEW_YORK_CITIBIKE_1.GEO_US_BOUNDARIES.ZIP_CODES z ON ST_WITHIN( ST_GEOGRAPHYPOINT(s."longitude", s."latitude"), TO_GEOGRAPHY(z."zip_code_geom") ) WHERE z."zip_code" = '10019' ), filtered_trips AS ( SELECT t."usertype", DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', TO_TIMESTAMP(t."starttime"/1000000))) AS trip_date FROM NEW_YORK_CITIBIKE_1.NEW_YORK_CITIBIKE.CITIBIKE_TRIPS t JOIN stations_10019 s_start ON t."start_station_name" = s_start.station_name JOIN stations_10019 s_end   ON t."end_station_name" = s_end.station_name WHERE YEAR(DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', TO_TIMESTAMP(t."starttime"/1000000)))) = 2018 ), weather AS ( SELECT TO_DATE(g."year" || '-' || g."mo" || '-' || g."da") AS weather_date, g."temp" AS temp, g."prcp" AS prcp, TRY_CAST(g."wdsp" AS FLOAT) AS wdsp FROM NEW_YORK_CITIBIKE_1.NOAA_GSOD.GSOD2018 g JOIN ( SELECT "wban" FROM NEW_YORK_CITIBIKE_1.NOAA_GSOD.STATIONS WHERE UPPER("name") LIKE '%CENTRAL PARK%' LIMIT 1 ) s ON g."wban" = s."wban" WHERE g."temp" != 9999.9 AND g."prcp" != 99.99 AND g."wdsp" != '999.9' ) SELECT ft."usertype", ROUND(AVG(w.temp), 4) AS avg_temperature_fahrenheit, ROUND(AVG(w.prcp), 4) AS avg_precipitation_inches, ROUND(AVG(w.wdsp), 4) AS avg_wind_speed_knots FROM filtered_trips ft JOIN weather w ON ft.trip_date = w.weather_date GROUP BY ft."usertype" ORDER BY avg_temperature_fahrenheit DESC LIMIT 1;""",
         "Unknown function ST_GEOGRAPHYPOINT"),
        ("""WITH stations_10019 AS ( SELECT s."name" AS station_name FROM NEW_YORK_CITIBIKE_1.NEW_YORK_CITIBIKE.CITIBIKE_STATIONS s JOIN NEW_YORK_CITIBIKE_1.GEO_US_BOUNDARIES.ZIP_CODES z ON ST_WITHIN( ST_GEOGRAPHY(ST_POINT(s."longitude", s."latitude")), TO_GEOGRAPHY(z."zip_code_geom") ) WHERE z."zip_code" = '10019' ), filtered_trips AS ( SELECT t."usertype", DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', TO_TIMESTAMP(t."starttime"/1000000))) AS trip_date FROM NEW_YORK_CITIBIKE_1.NEW_YORK_CITIBIKE.CITIBIKE_TRIPS t JOIN stations_10019 s_start ON t."start_station_name" = s_start.station_name JOIN stations_10019 s_end   ON t."end_station_name" = s_end.station_name WHERE YEAR(DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', TO_TIMESTAMP(t."starttime"/1000000)))) = 2018 ), weather AS ( SELECT TO_DATE(g."year" || '-' || g."mo" || '-' || g."da") AS weather_date, g."temp" AS temp, g."prcp" AS prcp, TRY_CAST(g."wdsp" AS FLOAT) AS wdsp FROM NEW_YORK_CITIBIKE_1.NOAA_GSOD.GSOD2018 g JOIN ( SELECT "wban" FROM NEW_YORK_CITIBIKE_1.NOAA_GSOD.STATIONS WHERE UPPER(TRIM("name")) LIKE '%CENTRAL PARK%' LIMIT 1 ) s ON g."wban" = s."wban" WHERE g."temp" != 9999.9 AND g."prcp" != 99.99 AND g."wdsp" != '999.9' ) SELECT ft."usertype", ROUND(AVG(w.temp), 4) AS avg_temperature_fahrenheit, ROUND(AVG(w.prcp), 4) AS avg_precipitation_inches, ROUND(AVG(w.wdsp), 4) AS avg_wind_speed_knots FROM filtered_trips ft JOIN weather w ON ft.trip_date = w.weather_date GROUP BY ft."usertype" ORDER BY avg_temperature_fahrenheit DESC LIMIT 1;""",
         "Unknown function ST_GEOGRAPHY"),
    ]
    for sql, msg in tests:
        print(
            find_error_operator(sql, error_message=msg, dialect='sqlite'),
            find_error_operator_simple(sql, error_message=msg)
        )
