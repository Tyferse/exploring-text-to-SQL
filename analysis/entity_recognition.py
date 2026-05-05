import re
from typing import Optional, Union, List, Tuple, Dict


error_categories = {
    'Ошибочный анализ данных': {
        "Некорректное использование функций": [
            "No matching signature for function",
            "ST_GeogPoint failed",
            "DATE_SUB does not support the",
            ("Numeric value", "is not recognized"),
            ("Type", "is not supported as argument to"),
            "Unknown parameter",
            "near \"QUALIFY\"", "unexpected 'QUALIFY'", "unexpected 'LATERAL'",
            "unexpected 'TOK_INTERVAL'",
            "is not defined for arguments of type",
            ("Column", "of type", "cannot be used in"), 
        ],
        "Некорректные вычисления": [
            "which is neither grouped nor aggregated", "misuse of window function",
            "wrong number of arguments to function", "Division by zero", "misuse of aggregate",
            "is not supported for window functions", "recursive aggregate queries not supported",
            "misuse of aliased window function", "Invalid floating point operation",
            "unexpected 'AVG'", "unexpected 'test_score'", "unexpected 'ARRAY_AGG'",
            "No matching signature for aggregate function", ("Aggregate function", "not allowed in"),
            ("Grouping by expressions of type", "is not allowed"),
            ("Aggregate functions with", "cannot be used with arguments of type"),
            "Aggregations of aggregations are not allowed", "Cannot GROUP BY literal values",
            ("Expressions of type", "cannot be used as GROUP BY keys"),

        ],
        "Некорректное планирование": [
            "No data found for the specified query",
            "Queries in UNION ALL have mismatched column count",
            "in UNION ALL has incompatible types",
            "Values referenced in UNNEST must be arrays",
            ("Actual statement", "did not match the desired statement"),
            "Single-row subquery returns more than one row",
            "near \"GROUP\"", "unexpected 'UNION'",
            ("Subquery of type", "must have only one output column"),
            "near \"UNION\"",
        ]
    }, 
    "Неверное следование схеме": {
        "Неверная таблица": [
            "must be qualified with a dataset (e.g. dataset.table)",
            "Invalid project ID",
            "Access Denied: Table",
            "Not found: Dataset",
            "no such table",
            "This session does not have a current database",
            ("Table", "was not found"),
            "Cannot read partition information from a table that is not partitioned"
        ],
        "Неверный столбец": [
            "ambiguous column name",
            "no such column",
            ("Name", "not found inside"),
            ("Name", "is ambiguous inside"),
            ("Column name", "is ambiguous at"),
            ("Field name", "does not exist"),
            "circular reference",
            "Cannot access field", "near \"months_for_customer\": syntax error",
            "Wildcard matched incompatible partitioning/clustering tables",
            "near \"AS\"",
        ],
        "Нераспознанное имя": [
            "Unrecognized name: ",
        ]
    },
    "Остальные": {
        "Превышена длина промпта": ["Context length exceeded"],
        # "Непонимание\nвнешних знаний": [],
        "Ошибка условной фильтрации": [
            # ,
            # "not allowed in WHERE clause"
            "near \"WHERE\"",
            "unexpected 'FILTER'",
            "unexpected 'WHERE'",
            "Analytic function not allowed in WHERE clause"
        ],
        "Неверный JOIN": [
            "Correlated subqueries",
            "near \"CROSS\"",
            "nexpected 'CROSS'",
            "unexpected 'LEFT'",
            "unexpected 'JOIN'",
            "Unsupported subquery with table in join predicate.",
            "IN subquery is not supported inside join predicate",
            ("Column", "in", "clause not found on left side of join"),
            "JOIN cannot be used without a condition",
            "a JOIN clause is required before ON",
        ],
        "Ошибка синтаксиса": [
            "Syntax error: Unexpected identifier", "Syntax error: Expected \"", "Syntax error: Unexpected end of script",
            "Syntax error: Expected end of input but got", "Syntax error: Unexpected keyword", "Syntax error: Expected keyword",
            "Syntax error: Unexpected string literal", "Function not found", "unrecognized token:", 
            "Unclosed identifier literal", "Occurrence must be positive", "Unknown function",
            "No matching signature for operator", "No matching signature for function", "no such function:",
            " clause should come after ", "incomplete input", "Cannot parse regular expression", "Invalid regular expression",
            "ordinal must be", "date value out of range", ("Timestamp", "is not recognized"),
            ("Can't parse", "as timestamp with format"), ("Can't parse", "as date with format"),
            ("Date", "is not recognized"), ("Bad output format", "for FIXED"), ("Failed to cast variant value", "to"),
            ("Ordering by expressions of type", "is not allowed at"), ("value", "is not recognized"),
            ("Can't parse", "as number with format"), "Unknown timezone", "Invalid Lng/Lat pair", "Error parsing WKT input",
            "is out of range", "out of representable range", "Unsupported feature", "You can only execute one statement at a time.",
            "Required parameter is missing", "malformed JSON", "Left argument of string is not an array", "Invalid extraction path",
            "concatenated string literals must be separated by whitespace or comments", "Bad int64 value",
            "ORDER BY key must be numeric in a RANGE-based window", ("does not support the", "date part when the argument is", "type"),
            "A valid date part name is required but found", "in arguments is not supported on scalar functions",
            "Illegal escape sequence", "produced too many elements", "ORDER BY term does not match any column in the result set",
            "Bad input format model", ("String", "is too long and would be truncated"), 

            "unexpected", "syntax error"
        ]
    }
}



column_patterns = [
    r"column\s+['\"]?(\w+)['\"]?\s+(?:not found|does not exist|is ambiguous)",
    r"no such column:\s*['\"]?(\w+)['\"]?",
    r"Field name\s+['\"]?(\w+)['\"]?\s+does not exist",
    r"Cannot access field\s+(\w+)",
    r"Column\s+['\"]?(\w+)['\"]?\s+in",
    r"Name\s+['\"]?(\w+)['\"]?\s+not found inside",
    r"Name\s+['\"]?(\w+)['\"]?\s+is ambiguous inside",
    r"Column name\s+['\"]?(\w+)['\"]?\s+is ambiguous at",
    r"ambiguous column name:?\s*['\"]?(\w+)['\"]?",
    r"near\s+\"(\w+)\"",  # Может быть именем столбца
]

table_patterns = [
    r"no such table:\s*['\"]?(\w+)['\"]?",
    r"Table\s+['\"]?(\w+)['\"]?\s+was not found",
    r"Not found: Dataset\s+['\"]?(\w+)['\"]?",
    r"Access Denied: Table\s+['\"]?(\w+)['\"]?",
    r"table\s+['\"]?(\w+)['\"]?\s+was not found",
    r"near\s+\"(\w+)\"",  # Может быть именем таблицы
]

function_patterns = [
    r"no such function:\s*(\w+)",
    r"Function not found:\s*(\w+)",
    r"Unknown function:\s*(\w+)",
    r"misuse of(?: aliased)? window function:\s*(\w+)",
    r"misuse of aggregate:\s*(\w+)",
    r"No matching signature for (?:aggregate )?function:\s*(\w+)",
    r"(\w+)\s+is not defined for arguments of type",
    r"(\w+)\s+is not supported for window functions",
    r"unexpected\s+'(\w+)'",  # Может быть функцией
    r"near\s+\"(\w+)\"",  # Может быть функцией
    r"(\w+)\s+does not support the",
]

join_patterns = [
    r"near\s+\"(\w+)\"",
    r"unexpected\s+'(\w+)'",
    r"Column\s+['\"]?(\w+)['\"]?\s+in.*?clause not found on left side of join",
]

syntax_patterns = [
    r"Syntax error: Unexpected identifier\s+['\"]?(\w+)['\"]?",
    r"Syntax error: Unexpected keyword\s+['\"]?(\w+)['\"]?",
    r"Syntax error: Expected.*?but got\s+['\"]?(\w+)['\"]?",
    r"unexpected\s+'(\w+)'",
    r"near\s+\"(\w+)\"",
    r"unrecognized token:\s*['\"]?(\w+)['\"]?",
    r"(\w+)\s+clause should come after",
]


def extract_entity(error_message: str, error_patterns: Dict = None) -> Optional[str]:
    """
    Извлекает имя сущности из сообщения об ошибке.
    
    Args:
        error_message: Сообщение об ошибке
        error_patterns: Словарь с категориями ошибок (если None, используются стандартные)
    
    Returns:
        Имя сущности или None, если не найдено
    """
    
    if error_patterns is None:
        error_patterns = get_error_categories()
    
    # Нормализуем сообщение об ошибке
    normalized_message = normalize_error_message(error_message)
    
    # Пробуем извлечь имя сущности напрямую из известных шаблонов
    entity = extract_entity_from_known_patterns(normalized_message)
    if entity:
        return entity
    
    # Если прямое извлечение не сработало, пробуем найти по контексту
    entity = extract_entity_from_context(normalized_message, error_patterns)
    if entity:
        return entity
    
    # Пробуем найти имена в кавычках или после ключевых слов
    entity = extract_quoted_entity(normalized_message)
    return entity


def normalize_error_message(error_message: str) -> str:
    """
    Нормализует сообщение об ошибке для лучшего сопоставления.
    """
    # Удаляем экранирование и лишние пробелы
    normalized = error_message.replace('\\n', '\n').replace('\\t', '\t')
    # Удаляем техническую информацию (Job ID, Location и т.д.)
    normalized = re.sub(r'\nLocation:.*', '', normalized)
    normalized = re.sub(r'\nJob ID:.*', '', normalized)
    normalized = re.sub(r'\nreason:.*', '', normalized)
    # Нормализуем пробелы
    normalized = ' '.join(normalized.split())
    return normalized


def extract_entity_from_known_patterns(error_message: str) -> Optional[str]:
    """
    Извлекает сущность из известных шаблонов ошибок.
    Эти шаблоны точно указывают на имя сущности.
    """
    explicit_patterns = [
        # Функции
        (r"no such function:\s*['\"]?(\w+)['\"]?", 1),
        (r"Function not found:\s*(\w+)", 1),
        (r"Unknown function:\s*(\w+)", 1),
        (r"Unknown parameter:\s*(\w+)", 1),
        (r"misuse of aggregate:\s*(\w+)", 1),
        (r"No matching signature for (?:aggregate )?function:\s*(\w+)", 1),
        (r"ST_GeogPoint failed", None),  # Нет извлекаемой сущности
        (r"DATE_SUB does not support the", None),
        
        # Таблицы и столбцы
        (r"no such table:\s*['\"]?(\w+)['\"]?", 1),
        (r"no such column:\s*['\"]?(\w+)['\"]?", 1),
        (r"Unrecognized name:\s*['\"]?(\w+)['\"]?", 1),
        (r"ambiguous column name:?\s*['\"]?(\w+)['\"]?", 1),
        (r"circular reference:\s*(\w+)", 1),
        (r"Cannot access field\s+(\w+)", 1),
        (r"Access Denied: Table\s+['\"]?(\w+)['\"]?", 1),
        (r"Not found: Dataset\s+['\"]?(\w+)['\"]?", 1),
        
        # Оконные функции
        (r"misuse of(?: aliased)? window function:\s*(\w+)", 1),
        (r"(\w+)\s+is not supported for window functions", 1),
        (r"window function.*?(\w+)\s+is not", 1),
        (r"(\w+)\s+cannot be used as a window function", 1),

        # Синтаксис
        (r"near\s+\"(\w+)\"", 1),
        (r"unexpected\s+'(\w+)'", 1),
        (r"Syntax error: Unexpected identifier\s+['\"]?(\w+)['\"]?", 1),
        (r"Syntax error: Unexpected keyword\s+['\"]?(\w+)['\"]?", 1),
        (r"Unknown timezone:\s*(\w+)", 1),
        
        # JOIN
        (r"Column\s+['\"]?(\w+)['\"]?\s+in.*?clause not found on left side of join", 1),
    ]
    
    for pattern, group in explicit_patterns:
        match = re.search(pattern, error_message, re.IGNORECASE)
        if match:
            if group is not None and match.groups():
                return match.group(group)
            elif group is None:
                return None  # Паттерн найден, но сущность не извлекается
    
    return None


def extract_entity_from_context(error_message: str, error_categories: Dict) -> Optional[str]:
    """
    Извлекает сущность на основе контекста ошибки и её классификации.
    """
    # Классифицируем ошибку
    category, subcategory = classify_error(error_message, error_categories)
    
    if not category or not subcategory:
        return None
    
    # Для разных подкатегорий используем разные стратегии извлечения
    if 'столб' in subcategory.lower() or 'column' in subcategory.lower():
        return extract_column_name(error_message)
    elif 'табли' in subcategory.lower() or 'table' in subcategory.lower():
        return extract_table_name(error_message)
    elif 'функц' in subcategory.lower() or 'function' in subcategory.lower():
        return extract_function_name(error_message)
    elif 'join' in subcategory.lower():
        return extract_join_entity(error_message)
    elif 'имя' in subcategory.lower() or 'name' in subcategory.lower():
        return extract_name_entity(error_message)
    elif 'синтакси' in subcategory.lower() or 'syntax' in subcategory.lower():
        return extract_syntax_entity(error_message)
    
    # Если не удалось определить по подкатегории, пробуем общие методы
    return extract_quoted_entity(error_message)


def classify_error(error_message: str, error_categories: Dict = None) -> Tuple[Optional[str], Optional[str]]:
    """
    Классифицирует ошибку на основе шаблонов.
    
    Args:
        error_message: Сообщение об ошибке
        error_categories: Словарь с категориями ошибок в формате:
                         {категория: {подкатегория: [список_паттернов]}}
    
    Returns:
        Кортеж (категория, подкатегория) или (None, None)
    """
    if error_categories is None:
        error_categories = get_error_categories()
    
    # Нормализуем сообщение
    normalized = error_message.lower()
    
    # Проходим по всем категориям и подкатегориям
    for category, subcategories in error_categories.items():
        if isinstance(subcategories, dict):
            for subcategory, patterns in subcategories.items():
                if isinstance(patterns, list):
                    for pattern in patterns:
                        if match_error_pattern(normalized, pattern):
                            return category, subcategory
    
    return None, None


def match_error_pattern(text: str, pattern: Union[str, Tuple[str, ...], List[str]]) -> bool:
    """
    Проверяет, соответствует ли текст шаблону ошибки.
    
    Поддерживает:
    - Строки: проверка наличия подстроки
    - Кортежи/списки: проверка наличия всех подстрок в указанном порядке
    
    Args:
        text: Текст для проверки (должен быть в нижнем регистре)
        pattern: Шаблон для сопоставления
    
    Returns:
        True если текст соответствует шаблону
    """
    if isinstance(pattern, str):
        # Простая проверка подстроки
        return pattern.lower() in text
    elif isinstance(pattern, (tuple, list)):
        # Проверка всех подстрок в указанном порядке
        current_pos = 0
        for substring in pattern:
            pos = text.find(substring.lower(), current_pos)
            if pos == -1:
                return False
            current_pos = pos + len(substring)
        return True
    
    return False


def extract_column_name(error_message: str) -> Optional[str]:
    """Извлекает имя столбца из ошибки."""
    global column_patterns
    
    for pattern in column_patterns:
        match = re.search(pattern, error_message, re.IGNORECASE)
        if match and match.groups():
            return match.group(1)
    
    return None


def extract_table_name(error_message: str) -> Optional[str]:
    """Извлекает имя таблицы из ошибки."""
    global table_patterns
    
    for pattern in table_patterns:
        match = re.search(pattern, error_message, re.IGNORECASE)
        if match and match.groups():
            return match.group(1)
    
    return None


def extract_function_name(error_message: str) -> Optional[str]:
    """Извлекает имя функции из ошибки."""
    global function_patterns
    
    for pattern in function_patterns:
        match = re.search(pattern, error_message, re.IGNORECASE)
        if match and match.groups():
            return match.group(1)
    
    return None


def extract_join_entity(error_message: str) -> Optional[str]:
    """Извлекает сущность, связанную с JOIN ошибками."""
    global join_patterns
    
    for pattern in join_patterns:
        match = re.search(pattern, error_message, re.IGNORECASE)
        if match and match.groups():
            return match.group(1)
    
    return None


def extract_name_entity(error_message: str) -> Optional[str]:
    """Извлекает нераспознанное имя."""
    match = re.search(r"Unrecognized name:\s*['\"]?(\w+)['\"]?", error_message, re.IGNORECASE)
    if match:
        return match.group(1)
    
    return extract_quoted_entity(error_message)


def extract_syntax_entity(error_message: str) -> Optional[str]:
    """Извлекает сущность из синтаксических ошибок."""
    global syntax_patterns
    
    for pattern in syntax_patterns:
        match = re.search(pattern, error_message, re.IGNORECASE)
        if match and match.groups():
            return match.group(1)
    
    return extract_quoted_entity(error_message)


def extract_quoted_entity(error_message: str) -> Optional[str]:
    """
    Извлекает сущность в кавычках из любой части сообщения.
    """
    # Ищем имена в двойных кавычках
    match = re.search(r'"(\w+)"', error_message)
    if match:
        return match.group(1)
    
    # Ищем имена в одинарных кавычках
    match = re.search(r"'(\w+)'", error_message)
    if match:
        return match.group(1)
    
    # Ищем имена в обратных кавычках
    match = re.search(r'`(\w+)`', error_message)
    if match:
        return match.group(1)
    
    return None


def match_error_to_category(error_message: str) -> Dict[str, Optional[str]]:
    """
    Определяет полную классификацию ошибки.
    
    Returns:
        Словарь с ключами:
        - 'category': категория ошибки
        - 'subcategory': подкатегория ошибки
        - 'entity': извлеченная сущность
    """
    global error_categories

    normalized = normalize_error_message(error_message)
    
    # Классифицируем ошибку
    category, subcategory = classify_error(normalized, error_categories)
    
    # Извлекаем сущность
    entity = extract_entity(normalized, error_categories)
    
    return {
        'category': category,
        'subcategory': subcategory,
        'entity': entity
    }


if __name__ == "__main__":
    # Тесты с разными типами ошибок
    test_cases = [
        "no such function: GREATEST",
        "Function not found: SAFE_LOG at [6:1]",
        "Unrecognized name: customer_id",
        "no such column: user_name",
        "Access Denied: Table mydataset.sensitive_data",
        'near "months_for_customer": syntax error',
        "misuse of window function: ROW_NUMBER",
        "Division by zero",
        "Invalid floating point operation",
        "Syntax error: Unexpected identifier test_score",
        "Error: Numeric value is not recognized",
        "Error: Type is not supported as argument to function",
        "Error: Column of type cannot be used in expression",
        "Error: Aggregate function not allowed in WHERE clause",
        "Error: Grouping by expressions of type is not allowed",
        "Error: Can't parse as timestamp with format",
        "Error: does not support the date part when the argument is type",
        "Error: String is too long and would be truncated",
    ]
    
    print("Тестирование классификации и извлечения сущностей:\n")
    
    for error in test_cases:
        print(f"Ошибка: {error}")
        
        # Получаем полную информацию
        result = match_error_to_category(error)
        
        print(f"  Категория: {result['category']}")
        print(f"  Подкатегория: {result['subcategory']}")
        print(f"  Сущность: {result['entity']}")
        
        # Также тестируем отдельные функции
        entity = extract_entity(error)
        category, subcategory = classify_error(normalize_error_message(error))
        
        print(f"  extract_entity: {entity}")
        print(f"  classify_error: {category} -> {subcategory}")
        print()
