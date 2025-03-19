import string
import random


# def generate_sql_statements_old(nr_statements: int, databases: list[tuple[str, str]]):
#     tables = ['t1']
#     columns = ['c1', 'c2']
#     views = ['v1', 'v2']

#     db_name, db_file_name = databases[0]  # TODO use multiple db files
#     statements = [  # TODO randomize statements; TODO generate a variable nr of statements
#         statement_attach(db_file_name),
#         statement_create_table(tables, columns),
#         statement_detach(db_name),
#         statement_insert_into_table(tables, len(columns)),
#     ]
#     return statements


def generate_sql_statements(nr_statements: int, databases: list[tuple[str, str]]):
    tables = ['t1']
    views = ['v1', 'v2']
    columns = ['c1', 'c2']
    statement_weights = [
        (statement_attach, 10),
        (statement_detach, 1),
        (statement_use, 10),
        (statement_create_table, 10),
        (statement_drop_table, 1),
        (statement_insert_into_table, 50),
        (statement_create_view, 10),
        (statement_drop_view, 1),
    ]
    funcs = [sw[0] for sw in statement_weights]
    weights = [sw[1] for sw in statement_weights]
    sampled_funcs = random.choices(funcs, weights=weights, k=nr_statements)
    return [f(databases, tables, views, columns) for f in sampled_funcs]


def statement_attach(databases: list[tuple[str, str]], tables: list[str], views: list[str], columns: list[str]):
    db_file_name = random.choice(databases)[1]
    return f"ATTACH '{db_file_name}';"


def statement_detach(databases: list[tuple[str, str]], tables: list[str], views: list[str], columns: list[str]):
    db_name = random.choice(databases)[0]
    return f"DETACH {db_name};"


def statement_use(databases: list[tuple[str, str]], tables: list[str], views: list[str], columns: list[str]):
    db_name = random.choice(databases)[0]
    schema = 'main'
    return f"USE {db_name}.{schema};"


def statement_create_table(databases: list[tuple[str, str]], tables: list[str], views: list[str], columns: list[str]):
    table_name = random.choice(tables)
    return f"CREATE TABLE {table_name} ({', '.join([f'{col_name} VARCHAR' for col_name in columns])});"


def statement_drop_table(databases: list[tuple[str, str]], tables: list[str], views: list[str], columns: list[str]):
    table_name = random.choice(tables)
    return f"DROP TABLE {table_name};"


def statement_insert_into_table(
    databases: list[tuple[str, str]], tables: list[str], views: list[str], columns: list[str]
):
    table_name = random.choice(tables)
    num_string_columns = len(columns)
    return f"INSERT INTO {table_name} VALUES ({', '.join([quoted_random_string(5000) for _ in range(num_string_columns)])});"


def statement_create_view(databases: list[tuple[str, str]], tables: list[str], views: list[str], columns: list[str]):
    view_name = random.choice(views)
    table_name = random.choice(tables)
    return f"CREATE VIEW {view_name} AS SELECT * FROM {table_name};"


def statement_drop_view(databases: list[tuple[str, str]], tables: list[str], views: list[str], columns: list[str]):
    view_name = random.choice(views)
    return f"DROP VIEW {view_name};"


def quoted_random_string(string_length):
    # single quotes are doubled in sql string literals
    return f"'{''.join(random.choice(string.printable[:-2]) for _ in range(string_length)).replace("'", "''")}'"
