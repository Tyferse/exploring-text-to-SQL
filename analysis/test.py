import sqlglot
from sqlglot import exp

print("Проверка наличия атрибутов db и catalog у разных типов узлов:\n")
print("=" * 80)

# Тестовые SQL с разными форматами имен
test_sqls = [
    # Простое имя таблицы
    ("SELECT * FROM users", "Простое имя таблицы"),
    # Имя таблицы со схемой
    ("SELECT * FROM public.users", "Таблица со схемой (db)"),
    # Имя таблицы с проектом и схемой
    ("SELECT * FROM project.dataset.table", "Таблица с catalog и db"),
    # Алиас таблицы
    ("SELECT * FROM users u", "Алиас таблицы"),
    # Столбец без таблицы
    ("SELECT id FROM users", "Столбец без таблицы"),
    # Столбец с алиасом
    ("SELECT u.id FROM users u", "Столбец с алиасом"),
    # Столбец с полным путем
    ("SELECT project.dataset.table.column FROM project.dataset.table", "Столбец с полным путем"),
    # Столбец со схемой
    ("SELECT public.users.id FROM public.users", "Столбец со схемой"),
]

for sql, description in test_sqls:
    print(f"\n{description}:")
    print(f"  SQL: {sql}")
    print("-" * 40)
    
    try:
        tree = sqlglot.parse_one(sql, error_level=sqlglot.ErrorLevel.IGNORE)
        
        for node in tree.walk():
            if isinstance(node, (exp.Table, exp.Column)):
                print(f"  {type(node).__name__}:")
                
                # Используем hasattr для безопасной проверки
                print(f"    name: {getattr(node, 'name', 'НЕТ АТРИБУТА')}")
                print(f"    table: {getattr(node, 'table', 'НЕТ АТРИБУТА')}")
                
                # Проверяем наличие db
                if hasattr(node, 'db'):
                    print(f"    db: {node.db}")
                else:
                    print(f"    db: НЕТ АТРИБУТА")
                
                # Проверяем наличие catalog
                if hasattr(node, 'catalog'):
                    print(f"    catalog: {node.catalog}")
                else:
                    print(f"    catalog: НЕТ АТРИБУТА")
                
                # Выводим все атрибуты
                print(f"    Все атрибуты: {[attr for attr in dir(node) if not attr.startswith('_')]}")
                print(f"    args: {node.args}")
                print()
    
    except Exception as e:
        print(f"  Ошибка: {e}")

# Детальный разбор структуры узлов
print("\n\nДетальный разбор структуры узлов:")
print("=" * 80)

# Проверяем Table узел
print("\n1. Table узел для 'project.dataset.table':")
sql = "SELECT * FROM project.dataset.table"
tree = sqlglot.parse_one(sql, error_level=sqlglot.ErrorLevel.IGNORE)
for node in tree.walk():
    if isinstance(node, exp.Table):
        print(f"  Тип: {type(node)}")
        print(f"  name: '{node.name}'")
        print(f"  catalog: '{getattr(node, 'catalog', 'НЕТ')}'")
        print(f"  db: '{getattr(node, 'db', 'НЕТ')}'")
        print(f"  sql(): '{node.sql()}'")
        
        # Проверяем через args
        if 'catalog' in node.args:
            print(f"  catalog в args: '{node.args['catalog']}'")
        if 'db' in node.args:
            print(f"  db в args: '{node.args['db']}'")

# Проверяем Column узел
print("\n2. Column узел для 'project.dataset.table.column':")
sql = "SELECT project.dataset.table.column FROM project.dataset.table"
tree = sqlglot.parse_one(sql, error_level=sqlglot.ErrorLevel.IGNORE)
for node in tree.walk():
    if isinstance(node, exp.Column):
        print(f"  Тип: {type(node)}")
        print(f"  name: '{node.name}'")
        print(f"  table: '{getattr(node, 'table', 'НЕТ')}'")
        print(f"  catalog: '{getattr(node, 'catalog', 'НЕТ')}'")
        print(f"  db: '{getattr(node, 'db', 'НЕТ')}'")
        print(f"  sql(): '{node.sql()}'")

# Проверяем простое имя таблицы
print("\n3. Table узел для простого 'users':")
sql = "SELECT * FROM users"
tree = sqlglot.parse_one(sql, error_level=sqlglot.ErrorLevel.IGNORE)
for node in tree.walk():
    if isinstance(node, exp.Table):
        print(f"  Тип: {type(node)}")
        print(f"  name: '{node.name}'")
        print(f"  hasattr catalog: {hasattr(node, 'catalog')}")
        print(f"  hasattr db: {hasattr(node, 'db')}")
        print(f"  args: {node.args}")


# Безопасный способ получения атрибутов
def safe_get_attr(node, attr_name, default=None):
    """
    Безопасно получает атрибут узла.
    Проверяет и через hasattr, и через args, и через getattr.
    """
    # Сначала проверяем через hasattr
    if hasattr(node, attr_name):
        value = getattr(node, attr_name, default)
        if value is not None:
            return value
    
    # Проверяем в args (для некоторых узлов атрибуты хранятся в args)
    if hasattr(node, 'args') and attr_name in node.args:
        value = node.args[attr_name]
        if isinstance(value, exp.Identifier):
            return value.name
        return str(value)
    
    return default


# Правильная функция для получения полного пути столбца/таблицы
def get_full_path(node):
    """
    Получает полный путь сущности (столбца или таблицы).
    Работает с любыми форматами имен.
    """
    if isinstance(node, exp.Column):
        parts = []
        
        # Проверяем и добавляем catalog
        catalog = safe_get_attr(node, 'catalog')
        if catalog:
            parts.append(catalog)
        
        # Проверяем и добавляем db
        db = safe_get_attr(node, 'db')
        if db:
            parts.append(db)
        
        # Проверяем и добавляем table
        table = safe_get_attr(node, 'table')
        if table:
            parts.append(table)
        
        # Добавляем имя столбца
        name = safe_get_attr(node, 'name')
        if name:
            parts.append(name)
        
        return '.'.join(parts) if parts else None
    
    elif isinstance(node, exp.Table):
        parts = []
        
        # Проверяем и добавляем catalog
        catalog = safe_get_attr(node, 'catalog')
        if catalog:
            parts.append(catalog)
        
        # Проверяем и добавляем db
        db = safe_get_attr(node, 'db')
        if db:
            parts.append(db)
        
        # Добавляем имя таблицы
        name = safe_get_attr(node, 'name')
        if name:
            parts.append(name)
        
        return '.'.join(parts) if parts else None
    
    return None


print("\n\nТестирование safe_get_attr и get_full_path:")
print("=" * 80)

test_cases = [
    "SELECT * FROM users",
    "SELECT * FROM public.users",
    "SELECT * FROM project.dataset.table",
    "SELECT id FROM users",
    "SELECT u.id FROM users u",
    "SELECT project.dataset.table.column FROM project.dataset.table",
    "SELECT public.users.id FROM public.users",
]

for sql in test_cases:
    print(f"\nSQL: {sql}")
    tree = sqlglot.parse_one(sql, error_level=sqlglot.ErrorLevel.IGNORE)
    
    for node in tree.walk():
        if isinstance(node, (exp.Table, exp.Column)):
            full_path = get_full_path(node)
            print(f"  {type(node).__name__}:")
            print(f"    Полный путь: {full_path}")
            print(f"    name: {safe_get_attr(node, 'name')}")
            print(f"    table: {safe_get_attr(node, 'table')}")
            print(f"    db: {safe_get_attr(node, 'db')}")
            print(f"    catalog: {safe_get_attr(node, 'catalog')}")


# Обновленная функция _entity_in_subtree с правильной обработкой
def _entity_in_subtree_safe(node, entity_name):
    """
    Безопасная версия _entity_in_subtree с правильной обработкой db и catalog.
    """
    entity_name_lower = entity_name.lower()
    
    try:
        for subnode in node.walk():
            # Проверяем столбцы
            if isinstance(subnode, exp.Column):
                # Проверяем полный путь
                full_path = get_full_path(subnode)
                if full_path and full_path.lower() == entity_name_lower:
                    return True
                
                # Проверяем только имя столбца
                name = safe_get_attr(subnode, 'name')
                if name and name.lower() == entity_name_lower:
                    return True
                
                # Проверяем table.column
                table = safe_get_attr(subnode, 'table')
                if table and name:
                    table_column = f"{table}.{name}".lower()
                    if table_column == entity_name_lower:
                        return True
            
            # Проверяем таблицы
            elif isinstance(subnode, exp.Table):
                full_path = get_full_path(subnode)
                if full_path and full_path.lower() == entity_name_lower:
                    return True
                
                name = safe_get_attr(subnode, 'name')
                if name and name.lower() == entity_name_lower:
                    return True
            
            # Проверяем алиасы таблиц
            elif isinstance(subnode, exp.TableAlias):
                name = safe_get_attr(subnode, 'name')
                if name and name.lower() == entity_name_lower:
                    return True
            
            # Проверяем функции
            elif isinstance(subnode, (exp.Anonymous, exp.Func)):
                name = safe_get_attr(subnode, 'name')
                if name and name.lower() == entity_name_lower:
                    return True
            
            # Проверяем другие идентификаторы
            elif isinstance(subnode, exp.Identifier):
                name = safe_get_attr(subnode, 'name')
                if name and name.lower() == entity_name_lower:
                    return True
    
    except Exception as e:
        # Для отладки: print(f"Error: {e}")
        pass
    
    return False