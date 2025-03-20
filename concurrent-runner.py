#!/usr/bin/env python3

import duckdb
from duckdb import DuckDBPyConnection
from pathlib import Path
import multiprocessing
from multiprocessing import Process
from threading import Thread

import generate_sql


DUCKDB_EXCEPTIONS = (
    ## -- expected exceptions
    duckdb.IOException,  # e.g. Could not set lock on file "db1.duckdb"
    duckdb.BinderException,  # e.g. Failed to detach database with name "db1"
    duckdb.CatalogException,  # e.g. Table with name t1 does not exist!
    duckdb.InvalidInputException, # e.g. Attempting to execute an unsuccessful or closed pending query result
)

MODE = 'threads'


def main():
    test_databases = [
        ('memory', ''),
        ('db1', 'db1.duckdb'),
        ('db2', 'db2.duckdb')
    ]
    sql_file_dir = Path('./multithreaded_hang')

    create_db_files(test_databases)
    # sql_files = create_sql_files(2, 10, sql_file_dir, test_databases)
    sql_files = list(sql_file_dir.glob('*.sql'))
    print(sql_files)
    concurrent_run(sql_files)
    delete_db_files(test_databases)


def create_db_files(test_databases):
    con = duckdb.connect()
    for db_name, db_file_name in test_databases:
        if db_name != 'memory':
            con.sql(f"ATTACH '{db_file_name}' AS {db_name};")
    con.close()


def delete_db_files(test_databases):
    for db_name, db_file_name in test_databases:
        if db_name != 'memory':
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
    if (MODE == 'threads'):
        con = duckdb.connect(":default:")
        all_processes: list[Thread] = []
    else:
        multiprocessing.set_start_method('fork', force=True)
        all_processes: list[Process] = []

    statements_all_files: list[list[str]] = [get_statements_from_file(sql_file) for sql_file in sql_files]
    # print(len(statements_all_files))
    # print(statements_all_files[0][0])
    print('---')
    # exit(42)

    # define concurrent jobs
    for sql_file in sql_files:
        statements: list[str] = get_statements_from_file(sql_file)
        thread = Thread(target=execute_statements, args=(statements, con, sql_file.name))
        all_processes.append(thread)

    # run jobs
    for job in all_processes:
        job.start()

    # wait for all jos to be finished
    for job in all_processes:
        print('waiting...', flush=True)
        job.join()
        print(f'joined process: {job}', flush=True)


    # run concurrently run per file
    # for sql_file_idx, sql_file in enumerate(sql_files):
    #     print(f'starting thread for {sql_file}', flush=True)
    #     process = Thread(target=execute_statements, args=(statements_all_files[sql_file_idx], con, sql_file.name))
    #     print(f'process: {process}', flush=True)
    #     process.start()
    #     all_processes.append(process)
    #     print("len(all_processes): ", len(all_processes), flush=True)



    if (MODE == 'threads'):
        con.close()
    print('done')


def execute_statements(statements: list[str], con: DuckDBPyConnection, sql_file_name: str):
    print(f'running statements from {sql_file_name}...', flush=True)
    for num, statement in enumerate(statements):
        print(f"{sql_file_name} - {num} - {statement}", flush=True)
        try:
            if statement.startswith('SELECT') or statement.startswith('FROM'):
                con.sql(statement).fetchall()
            else:
                con.sql(statement)
        except DUCKDB_EXCEPTIONS as e:
            print(f"{sql_file_name}: statement idx {num}: {statement} raised exception: {e}", flush=True)
    print(f'DONE running statements from {sql_file_name}...', flush=True)


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
