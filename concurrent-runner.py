#!/usr/bin/env python3

import duckdb
from pathlib import Path
import multiprocessing
from multiprocessing import Process

DUCKDB_EXCEPTIONS = (
            duckdb.BinderException,
            duckdb.CatalogException,
            duckdb.ConnectionException,
            duckdb.ConstraintException,
            duckdb.ConversionException,
            duckdb.HTTPException,
            duckdb.InterruptException,
            duckdb.InvalidInputException,
            duckdb.InvalidTypeException,
            duckdb.IOException,
            duckdb.NotImplementedException,
            duckdb.OutOfMemoryException,
            duckdb.OutOfRangeException,
            duckdb.ParserException,
            duckdb.PermissionException,
            duckdb.SequenceException,
            duckdb.SerializationException,
            duckdb.SyntaxException,
            duckdb.TransactionException,
            duckdb.TypeMismatchException,
            duckdb.IntegrityError,
            duckdb.NotSupportedError,
            duckdb.OperationalError,
            duckdb.ProgrammingError,
        )


def main():
    sql_dir = Path('./sql')
    db_names = ['db1.duckdb', 'db2.duckdb']
    sql_files = list(sql_dir.glob('*.sql'))
    if not sql_files:
        print(f"no sql files found in dir '{sql_dir.absolute()}'")
        exit(1)
    concurrent_run(db_names, sql_files)


def create_test_dbs(db_names):
    for db_name in db_names:
        duckdb.sql(f"ATTACH '{db_name}';")


def delete_test_dbs(db_names):
    for db_name in db_names:
        Path(db_name).unlink()


def concurrent_run(db_names: list[str], sql_files: list[Path]):
    multiprocessing.set_start_method('fork', force=True)
    # first create db files
    init = Process(target=create_test_dbs, args=(db_names,))
    init.start()
    init.join()
    # run query files in separate processes
    all_processes: list[Process] = []
    for sql_file in sql_files:
        process = Process(target=run_queries, args=(sql_file,))
        process.start()
        all_processes.append(process)
    # wait for all processes to be finished
    for process in all_processes:
        process.join()
    # clean up
    delete_test_dbs(db_names)


def run_queries(file_path: Path):
    con = duckdb.connect(":default:")
    statements = [statement.strip() for statement in file_path.read_text().split(';') if statement.strip()]
    for num, statement in enumerate(statements):
        try:
            con.sql(statement).fetchall()
        except DUCKDB_EXCEPTIONS as e:
            print(f"{file_path}; statement idx {num}: '{statement}' raised exception: {e}", flush=True)
    con.close()


if __name__ == "__main__":
    main()
