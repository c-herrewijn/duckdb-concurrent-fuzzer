#!/usr/bin/env python3

import duckdb
from pathlib import Path
import multiprocessing
from multiprocessing import Process

import generate_sql


DUCKDB_EXCEPTIONS = (
    ## -- expected exceptions
    duckdb.IOException,  # e.g. Could not set lock on file "db1.duckdb"
    duckdb.BinderException,  # e.g. Failed to detach database with name "db1"
    duckdb.CatalogException,  # e.g. Table with name t1 does not exist!
    ## -- unexpected exceptions
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
    sql_files = create_sql_files(5, 1000, sql_file_dir, test_databases)
    # sql_files = list(sql_file_dir.glob('*.sql'))
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
        print(f"generating sql statements for file {sql_file} ...")
        sql_file.touch()
        sql_file.write_text("\n".join(generate_sql.generate_sql_statements(statements_per_file, test_databases)))
        sql_files.append(sql_file)
    return sql_files


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
            if statement.startswith('SELECT') or statement.startswith('FROM'):
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


if __name__ == "__main__":
    main()
