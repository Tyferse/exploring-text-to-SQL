import re
from copy import deepcopy
from typing import Optional, Union, List, Tuple, Dict


error_categories = {
    'Ошибочный анализ данных': {
        "Некорректное использование функций": [
            "No matching signature for function",
            "ST_GeogPoint failed", "Invalid Lng/Lat pair",
            "DATE_SUB does not support the", "too many arguments for function",
            ("Type", "is not supported as argument to"),
            "Unknown parameter", "is not defined for arguments of type",
            "wrong number of arguments to function", ("Function", "does not support", "argument type"),
            "Function not found", "Unknown function", "Unknown table function",
            "No matching signature for function", "no such function:",
            "Error parsing WKT input", "Required parameter is missing",
            ("does not support the", "date part when the argument is", "type"),
            "Bad input format model", ("Function", "cannot be used with arguments of types"),
            ("invalid type", "for parameter"), "Invalid argument types for function", 
            ("does not support the", "date part"), "is not a function",
            ("invalid use of", "for function"), ("rgument", "function", "needs to be"),
            
        ],
        "Некорректные вычисления": [
            ("Numeric value", "is not recognized"),
            "near \"QUALIFY\"", "unexpected 'QUALIFY'", 
            ("Column", "of type", "cannot be used in"),
            ("Array position in", "must be coercible to", "type, but has type"),
            "which is neither grouped nor aggregated", "misuse of window function",
            "Division by zero", "misuse of aggregate", "near \"GROUP\"", 
            "is not supported for window functions", "recursive aggregate queries not supported",
            "misuse of aliased window function", "Invalid floating point operation",
            "unexpected 'AVG'", "unexpected 'ARRAY_AGG'", 
            "No matching signature for aggregate function", ("Aggregate function", "not allowed in"),
            ("Grouping by expressions of type", "is not allowed"),
            ("Aggregate functions with", "cannot be used with arguments of type"),
            "Aggregations of aggregations are not allowed", "Cannot GROUP BY literal values",
            ("Expressions of type", "cannot be used as GROUP BY keys"),
            "aggregate function calls cannot be nested", "window function calls cannot be nested", 
            "No matching signature for aggregate function", "Occurrence must be positive", 
            "Cannot read partition information from a table that is not partitioned",  
            "ordinal must be", "date value out of range", ("Failed to cast variant value", "to"),
            ("Ordering by expressions of type", "is not allowed at"), ("value", "is not recognized"),
            "is out of range", "out of representable range", ("Bad", "value"), 
            "ORDER BY key must be numeric in a RANGE-based window", 
            "in arguments is not supported on scalar functions",
            "produced too many elements", ("Window function type", "requires"),
            ("Invalid function type", "for window function"),
            ("found", "clause but no window function"), ("Array index", "is out of bounds"),
            ("An aggregate function that has both", "and", "arguments"),
            ("Partitioning by expressions of type", "is not allowed"),
            "Floating point error in function",

        ],
        "Некорректное планирование": [
            "unexpected 'LATERAL'", "circular reference",
            "No data found for the specified query",
            "Queries in UNION ALL have mismatched column count",
            "in UNION ALL has incompatible types", "duplicate alias", 
            "Values referenced in UNNEST must be arrays",
            "Single-row subquery returns more than one row", "unexpected 'UNION'",
            ("Subquery of type", "must have only one output column"),
            "near \"UNION\"", "Unsupported subquery type cannot be evaluated",
            "Correlated subqueries", "is an invalid Recursive CTE",
            "for set operator input branches", 
        ]
    }, 
    "Неверное следование схеме": {
        "Неверная таблица": [
            "must be qualified with a dataset",
            "Invalid project ID",
            "Access Denied: Table",
            "no such table",
            "This session does not have a current database",
            ("Table", "was not found"),
        ],
        "Неверный столбец": [
            "ambiguous column name", "no such column",
            ("Column name", "is ambiguous at"), ("Field name", "does not exist"),
            "Cannot access field", "Wildcard matched incompatible partitioning/clustering tables", 
        ],
        "Нераспознанное имя": [
            ("Schema","does not exist or not authorized."),
            "invalid identifier '", ("Name", "not found inside"),
            ("Name", "is ambiguous inside"), "Unrecognized name: ",
            ("Object", "does not exist or not authorized"),
            "This session does not have a current database",
            "Not found: Dataset", ("Database", "does not exist or not authorized."),
        ]
    },
    "Остальные": {
        "Превышена длина контекста": [
            "Context length exceeded",
            "maximum context length"
        ],
        "Ошибка условной фильтрации": [
            "not allowed in WHERE clause", "near \"WHERE\"",
            "unexpected 'FILTER'", "unexpected 'WHERE'",
            "Analytic function not allowed in WHERE clause"
        ],
        "Неверный JOIN": [
            "near \"CROSS\"",
            "nexpected 'CROSS'",
            "unexpected 'LEFT'",
            "unexpected 'JOIN'",
            "Unsupported subquery with table in join predicate.",
            "IN subquery is not supported inside join predicate",
            ("Column", "in", "clause not found on left side of join"),
            "JOIN cannot be used without a condition",
            "a JOIN clause is required before ON",
            ("called with", "syntax or a join predicate"),
            "Lateral View cannot be on the left side of join",
            "subquery is not supported inside join predicate",
        ],
        "Ошибка синтаксиса": [
            "Syntax error: Unexpected identifier", "Syntax error: Expected \"", "Syntax error: Unexpected end of script",
            "Syntax error: Expected end of input but got", "Syntax error: Unexpected keyword", "Syntax error: Expected keyword",
            "Syntax error: Unexpected string literal", "unrecognized token:", "Unclosed identifier literal", 
            "No matching signature for operator", " clause should come after ", "incomplete input", 
            "Cannot parse regular expression", "Invalid regular expression", ("Timestamp", "is not recognized"),
            ("Can't parse", "as timestamp with format"), ("Can't parse", "as date with format"),
            ("Date", "is not recognized"), ("Bad output format", "for FIXED"), 
            ("Can't parse", "as number with format"), "Unknown timezone",  
             "malformed JSON", "Left argument of string is not an array", "Invalid extraction path",
            "concatenated string literals must be separated by whitespace or comments",
            "A valid date part name is required but found", "Trailing comma after the WITH clause", 
            "Illegal escape sequence",  "Invalid JSON path syntax",  
            ("String", "is too long and would be truncated"), 
            ("Query without", "clause cannot have a", "clause"),
            ("Bitwise operator", "requires", "arguments of the same type"),
            ("Variable", "are allowed only at the start"), "ORDER BY term does not match any column",

            "unsupported", "Unsupported feature", "invalid identifier", "unexpected", "syntax error"
        ]
    }
}

column_patterns = [
    r"column\s+['\"]?([\w\.\:]+)['\"]?\s+(?:not found|does not exist|is ambiguous)",
    r"no such column:\s*['\"]?([\w\.\:]+)['\"]?",
    r"Field name\s+['\"]?(\w+)['\"\:]?\s+does not exist",
    r"Cannot access field\s+([\w\.\:]+)",
    r"Column\s+['\"]?([\w\.\:]+)['\"]?\s+in",
    r"Name\s+['\"]?([\w\.\:]+)['\"]?\s+not found inside",
    r"Name\s+['\"]?([\w\.\:]+)['\"]?\s+is ambiguous inside",
    r"Column name\s+['\"]?([\w\.\:]+)['\"]?\s+is ambiguous at",
    r"ambiguous column name:?\s*['\"]?([\w\.\:]+)['\"]?",
    r"near\s+\"([\w\.\:]+)\"",  # Может быть именем столбца
    r"invalid identifier\s+[\`\'\"]([\w\.\:\"]+)[\`\'\"]",
    r"Schema\s+['\"]?([\w\.\:]+)['\"]?\s+does not exist or not authorized",
]

table_patterns = [
    r"no such table:\s*['\"]?([\w\.\:]+)['\"]?",
    r"Table\s+['\"]?([\w\.\:]+)['\"]?\s+was not found",
    r"Not found: Dataset\s+['\"]?([\w\.\:]+)['\"]?",
    r"Access Denied: Table\s+['\"]?([\w\.\:]+)['\"]?",
    r"table\s+['\"]?([\w\.\:]+)['\"]?\s+was not found",
    r"near\s+\"([\w\.\:]+)\"",  # Может быть именем таблицы
    r"invalid identifier\s+[\`\'\"]([\w\.\:\"]+)[\`\'\"]",
    r"Schema\s+['\"]?([\w\.\:]+)['\"]?\s+does not exist or not authorized",
]

function_patterns = [
    r"no such function:\s*(\w+)",
    r"Function not found:\s*(\w+)",
    r"Unknown function:\s*([\w\,]+)",
    r"Unknown functions\s+([\w\s,]+?)(?:;|$)",
    r"misuse of(?: aliased)? window function:\s*(\w+)",
    r"misuse of aggregate:\s*(\w+)",
    r"No matching signature for (?:aggregate )?function:\s*(\w+)",
    r"(\w+)\s+is not defined for arguments of type",
    r"(\w+)\s+is not supported for window functions",
    r"unexpected\s+'(\w+)'",  # Может быть функцией
    r"near\s+\"(\w+)\"",  # Может быть функцией
    r"(\w+)\s+does not support the",
    r"wrong number of arguments to function\s+(\w+)",
]

join_patterns = [
    r"Column\s+['\"]?(\w+)['\"]?\s+in.*?clause not found on left side of join",
    "Unsupported subquery with table in join predicate",
]

syntax_patterns = [
    r"Syntax error: Unexpected identifier\s+['\"]?(\w+)['\"]?",
    r"Syntax error: Unexpected keyword\s+['\"]?(\w+)['\"]?",
    r"Syntax error: Expected.*?but got\s+['\"]?(\w+)['\"]?",
    r"unexpected\s+'(\w+)'",
    r"near\s+\"(\w+)\"", 
    r"unrecognized token:\s*['\"]?(\w+)['\"]?",
    r"(\w+)\s+clause should come after",
    r'near\s+"(\w+)"\s*:\s*syntax error',
    r'syntax error at or near\s+"(\w+)"',
    r'parse error.*?near\s+"(\w+)"',
    r'invalid syntax.*?near\s+"(\w+)"',
    r'syntax error.*?at\s+"(\w+)"',
    r"(?:Invalid regular expression|Bad input format|Bad output format)\s*(?:model\s*)?[\'\"]?([^\'\"]+?)[\'\"]?(?:\s*(?:for|is|at|$))",
    r'(?:Numeric value|Timestamp|Date|String)\s*[\'"]?([^\'"]+?)[\'"]?\s*(?:is not recognized|is too long)',
    r"Can't parse\s+'([^']+)'\s+as\s+(?:date|timestamp|number)\s+with\s+format",
    r"Invalid extraction path\s+'([^']+)'",
    r"invalid type\s+'([^']+)'\s+for parameter"
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
        error_patterns = deepcopy(error_categories)
    
    # Нормализуем сообщение об ошибке
    normalized_message = normalize_error_message(error_message)
    
    # Пробуем извлечь имя сущности напрямую из известных шаблонов
    try:
        entity = extract_entity_from_known_patterns(normalized_message)
        if entity:
            return entity
    except:
        pass
    
    # Если прямое извлечение не сработало, пробуем найти по контексту
    try:
        entity = extract_entity_from_context(normalized_message, error_patterns)
        if entity:
            return entity
    except:
        pass
    
    # Пробуем найти имена в кавычках или после ключевых слов
    entity = extract_quoted_entity(normalized_message)
    return entity


def normalize_error_message(error_message: str) -> str:
    """
    Нормализует сообщение об ошибке для лучшего сопоставления.
    """
    # Удаляем экранирование и лишние пробелы
    normalized = error_message.replace('\\n', '\n').replace('\\t', '\t')
    # Удаляем техническую информацию
    normalized = re.sub(r'\nLocation:.*', '', normalized)
    normalized = re.sub(r'\nJob ID:.*', '', normalized)
    normalized = re.sub(r'\nreason:.*', '', normalized)
    normalized = re.sub(r'\nmessage:.*', '', normalized)
    # Удаляем коды ошибок в начале
    normalized = re.sub(r'^\d{6}\s*\(\w+\):\s*', '', normalized)
    normalized = re.sub(r'^\d{3,4}\s+', '', normalized)
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
        (r"Unknown functions\s+([\w\s,]+?)(?:;|$)", 1),
        (r"Unknown parameter:\s*(\w+)", 1),
        (r"misuse of aggregate:\s*(\w+)", 1),
        (r"No matching signature for (?:aggregate )?function:\s*(\w+)", 1),
        (r"wrong number of arguments to function\s+(\w+)", 1),
        (r"Invalid extraction path\s+'([^']+)'", 1),
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
        (r"invalid identifier\s+['\"](\w+)['\"]", 1),
        
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
        (r"(\w+)\s*\([^)]+\)\s*produced too many elements", 1),
        (r"Can't parse\s+'([^']+)'\s+as\s+(?:date|timestamp|number)\s+with\s+format", 1),
        
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


def classify_error(error_message: str, error_cats: Dict = None) -> Tuple[Optional[str], Optional[str]]:
    """
    Классифицирует ошибку на основе шаблонов.
    
    Args:
        error_message: Сообщение об ошибке
        error_categories: Словарь с категориями ошибок в формате:
                         {категория: {подкатегория: [список_паттернов]}}
    
    Returns:
        Кортеж (категория, подкатегория) или (None, None)
    """
    if error_cats is None:
        error_cats = deepcopy(error_categories)
    
    # Нормализуем сообщение
    normalized = error_message.lower()
    
    # Проходим по всем категориям и подкатегориям
    for category, subcategories in error_cats.items():
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
    # Ищем имена в одинарных кавычках
    match = re.search(r"'([\w\.\"\:]+)'", error_message)
    if match:
        return match.group(1)
    
    # Ищем имена в обратных кавычках
    match = re.search(r'`([\w\.\"\:]+)`', error_message)
    if match:
        return match.group(1)

    # Ищем имена в двойных кавычках
    match = re.search(r'"([\w\.\:]+)"', error_message)
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
        # "no such function: GREATEST",
        # "Function not found: SAFE_LOG at [6:1]",
        # "Unrecognized name: customer_id",
        # "no such column: user_name",
        # "Access Denied: Table mydataset.sensitive_data",
        # 'near "months_for_customer": syntax error',
        # "misuse of window function: ROW_NUMBER",
        # "Division by zero",
        # "Invalid floating point operation",
        # "Syntax error: Unexpected identifier test_score",
        # "Error: Numeric value is not recognized",
        # "Error: Type is not supported as argument to function",
        # "Error: Column of type cannot be used in expression",
        # "Error: Aggregate function not allowed in WHERE clause",
        # "Error: Grouping by expressions of type is not allowed",
        # "Error: Can't parse as timestamp with format",
        # "Error: does not support the date part when the argument is type",
        # "Error: String is too long and would be truncated",
        "invalid identifier 'T.ID'"
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
