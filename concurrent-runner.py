#!/usr/bin/env python3

import duckdb
from pathlib import Path
import multiprocessing
from multiprocessing import Process
import random
import string

DUCKDB_EXCEPTIONS = (
    ## -- expected exceptions
    duckdb.IOException,  # e.g. Could not set lock on file "db1.duckdb"
    duckdb.BinderException,  # e.g. Failed to detach database with name "db1"
    ## -- unexpected exceptions
    # duckdb.CatalogException,
    # duckdb.ConnectionException,
    # duckdb.ConstraintException,
    # duckdb.ConversionException,
    # duckdb.HTTPException,
    # duckdb.InterruptException,
    # duckdb.InvalidInputException,
    # duckdb.InvalidTypeException,
    # duckdb.NotImplementedException,
    # duckdb.OutOfMemoryException,
    # duckdb.OutOfRangeException,
    # duckdb.ParserException,
    # duckdb.PermissionException,
    # duckdb.SequenceException,
    # duckdb.SerializationException,
    # duckdb.SyntaxException,
    # duckdb.TransactionException,
    # duckdb.TypeMismatchException,
    # duckdb.IntegrityError,
    # duckdb.NotSupportedError,
    # duckdb.OperationalError,
    # duckdb.ProgrammingError,
)


def main():
    test_databases = [('db1', 'db1.duckdb'), ('db2', 'db2.duckdb')]
    sql_file_dir = Path('./sql')
    create_db_files(test_databases)
    sql_files = create_sql_files(2, 10, sql_file_dir, test_databases)
    concurrent_run(sql_files)
    delete_db_files(test_databases)


def create_db_files(test_databases):
    con = duckdb.connect()
    for db_name, db_file_name in test_databases:
        con.sql(f"ATTACH '{db_file_name}' AS {db_name};")
    con.close()


def delete_db_files(test_databases):
    for _, db_file_name in test_databases:
        Path(db_file_name).unlink()


def create_sql_files(nr_sql_files: int, statements_per_file: int, sql_file_dir: Path, test_databases: tuple[str, str]):
    sql_file_dir.mkdir(exist_ok=True)
    sql_files: list[Path] = []
    for sql_file_num in range(1, nr_sql_files + 1):
        sql_file = sql_file_dir / f"file{sql_file_num}.sql"
        sql_file.touch()
        sql_file.write_text(generate_statements(statements_per_file, test_databases))
        sql_files.append(sql_file)
    return sql_files


def generate_statements(nr_statements: int, test_databases: tuple[str, str]):
    table_name = 't1'
    col_names = ['c1', 'c2']
    db_name, db_file_name = test_databases[0]  # TODO use multiple db files
    statements = [  # TODO randomize statements; TODO generate a variable nr of statements
        statement_attach(db_file_name),
        statement_create_table(table_name, col_names),
        statement_detach(db_name),
        statement_insert_into_table(table_name, len(col_names)),
    ]
    all_statements: str = "\n".join(statements)
    return all_statements


def concurrent_run(sql_files: list[Path]):
    # run query files in separate processes
    multiprocessing.set_start_method('fork', force=True)
    all_processes: list[Process] = []
    for sql_file in sql_files:
        process = Process(target=execute_statements, args=(sql_file,))
        process.start()
        all_processes.append(process)
    # wait for all processes to be finished
    for process in all_processes:
        process.join()


def execute_statements(sql_file: Path):
    con = duckdb.connect(":default:")
    statements = get_statements_from_file(sql_file)
    for num, statement in enumerate(statements):
        try:
            if statement.startswith('SELECT'):
                con.sql(statement).fetchall()
            else:
                con.sql(statement)
        except DUCKDB_EXCEPTIONS as e:
            print(f"{sql_file}; statement idx {num}: {statement} raised exception: {e}", flush=True)
    con.close()


def get_statements_from_file(sql_file: Path) -> list[str]:
    # Split on ';' if not within single quotes. Note: statements can contain all printable chars, including newlines.
    sql_statements = []
    current_statement = ''
    in_quotes = False
    for char in sql_file.read_text():
        if char == "'":
            in_quotes = not in_quotes
            current_statement += char
        elif char == ';' and not in_quotes:
            if current_statement.strip():
                sql_statements.append(current_statement.strip())
            current_statement = ''
        else:
            current_statement += char
    if current_statement.strip():
        sql_statements.append(current_statement.strip())
    return sql_statements


def quoted_random_string(string_length):
    # single quotes are doubled in sql string literals
    return f"'{''.join(random.choice(string.printable[:-2]) for _ in range(string_length)).replace("'", "''")}'"


def statement_attach(db_file_name):
    return f"ATTACH '{db_file_name}';"


def statement_detach(db_name):
    return f"DETACH {db_name};"


def statement_create_table(table_name: str, col_names: list[str]):
    return f"CREATE TABLE {table_name} ({', '.join([f'{col_name} VARCHAR' for col_name in col_names])});"


def statement_insert_into_table(table_name: str, num_string_columns: int):
    return f"INSERT INTO {table_name} VALUES ({', '.join([quoted_random_string(5000) for _ in range(num_string_columns)])});"


def statement_create_view():
    # TODO
    pass


def statement_use(db_name, schema='main'):
    return f"USE {db_name}.{schema};"


if __name__ == "__main__":
    main()
