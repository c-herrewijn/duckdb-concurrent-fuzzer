#!/usr/bin/env python3

import duckdb
from duckdb import DuckDBPyConnection
from enum import Enum, auto
import multiprocessing
from multiprocessing import Process
from pathlib import Path
from threading import Thread

import generate_sql


class ConcurrentMode(Enum):
    MULTI_THREADING = auto()
    MULTI_PROCESSING = auto()


DUCKDB_EXCEPTIONS = (
    ## -- expected exceptions
    duckdb.IOException,  # e.g. Could not set lock on file "db1.duckdb"
    duckdb.BinderException,  # e.g. Failed to detach database with name "db1"
    duckdb.CatalogException,  # e.g. Table with name t1 does not exist!
)


def main():
    # -- settings --
    test_databases = [
        ('memory', ''),
        ('db1', 'db1.duckdb'),
        ('db2', 'db2.duckdb')
    ]
    sql_file_dir = Path('./sql')
    concurrent_mode = ConcurrentMode.MULTI_PROCESSING
    num_files = 5
    statements_per_file = 1000
    generate_statements = True
    # -- end of settings --

    sql_files = (
        create_sql_files(num_files, statements_per_file, sql_file_dir, test_databases)
        if generate_statements
        else list(sql_file_dir.glob('*.sql'))
    )
    create_db_files(test_databases)
    match concurrent_mode:
        case ConcurrentMode.MULTI_PROCESSING:
            run_forked_processes(sql_files)
        case ConcurrentMode.MULTI_THREADING:
            run_threads(sql_files)
        case _:
            raise ('Invalid ConcurrentMode')
    delete_db_files(test_databases)


def create_db_files(test_databases):
    delete_db_files(test_databases)
    con = duckdb.connect()
    for db_name, db_file_name in test_databases:
        if db_name != 'memory':
            con.execute(f"ATTACH '{db_file_name}' AS {db_name};")
    con.close()


def delete_db_files(test_databases):
    for db_name, db_file_name in test_databases:
        path = Path(db_file_name)
        if db_name != 'memory' and path.is_file():
            path.unlink()


def create_sql_files(nr_sql_files: int, nr_statements: int, sql_file_dir: Path, test_databases: tuple[str, str]):
    sql_file_dir.mkdir(exist_ok=True)
    sql_files: list[Path] = []
    for sql_file_num in range(1, nr_sql_files + 1):
        sql_file = sql_file_dir / f"file{sql_file_num}.sql"
        print(f"generating sql statements for file {sql_file} ...")
        sql_file.touch()
        sql_file.write_text("\n".join(generate_sql.generate_sql_statements(nr_statements, test_databases)))
        sql_files.append(sql_file)
    return sql_files


def run_threads(sql_files: list[Path]):
    con = duckdb.connect()
    all_threads: list[Thread] = []
    # define threads
    for sql_file in sql_files:
        statements: list[str] = get_statements_from_file(sql_file)
        thread = Thread(target=execute_statements, args=(statements, con, sql_file.name))
        all_threads.append(thread)
    # run threads and wait for them to be finished
    for thread in all_threads:
        thread.start()
    for thread in all_threads:
        thread.join()
    con.close()


def run_forked_processes(sql_files: list[Path]):
    multiprocessing.set_start_method('fork', force=True)
    all_processes: list[Process] = []
    for sql_file in sql_files:
        statements: list[str] = get_statements_from_file(sql_file)
        process = Process(target=execute_statements, args=(statements, None, sql_file.name))
        all_processes.append(process)
    # run processes and wait for them to be finished
    for process in all_processes:
        process.start()
    for process in all_processes:
        process.join()


def execute_statements(statements: list[str], con: DuckDBPyConnection, sql_file_name: str):
    if con:
        con = con.cursor()
    else:
        con = duckdb.connect()
    for num, statement in enumerate(statements):
        try:
            con.execute(statement).fetchall()
        except DUCKDB_EXCEPTIONS as e:
            print(f"{sql_file_name}: statement idx {num}: {statement} raised exception: {e}", flush=True)
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
