#!/usr/bin/env python3
"""
Create the EHR source database, enable CDC, and bulk-load the generated extracts.

    python -m scripts.setup_source_db [--reset]

Loads with BULK INSERT from /data (sample-data/ mounted into the container), which keeps the load
fast enough to scale. Where the server cannot see that directory - a CI service container cannot
mount the workspace - it falls back to batched client-side inserts, which are slower but need no
shared filesystem. Order follows the foreign keys.

CDC is enabled *before* the load, so the initial rows are captured too; bronze still takes a
snapshot for its first load, because CDC retention (3 days by default) is not a place to keep
history.
"""

import argparse
import csv
import os
import sys
import time

from databricks.utilities import sqlserver as mssql

# (table, csv file under /data) in foreign-key order.
TABLES = [
    ("providers", "sql_ehr/providers.csv"),
    ("patients", "sql_ehr/patients.csv"),
    ("encounters", "sql_ehr/encounters.csv"),
    ("diagnoses", "sql_ehr/diagnoses.csv"),
    ("lab_results", "sql_ehr/lab_results.csv"),
    ("medications", "sql_ehr/medications.csv"),
]

DDL_PATH = "sql/source/01_ehr_source_schema.sql"


def split_batches(script: str) -> list[str]:
    """Split a T-SQL script on GO, which is a batch separator for the client, not a statement."""
    batches, current = [], []
    for line in script.splitlines():
        if line.strip().upper() == "GO":
            batches.append("\n".join(current))
            current = []
        else:
            current.append(line)
    batches.append("\n".join(current))
    return [b for b in batches if b.strip()]


def database_exists(name: str) -> bool:
    return mssql.scalar(f"SELECT DB_ID('{name}')", database="master") is not None


def strip_database_statements(batch: str) -> str:
    """Drop the script's own CREATE DATABASE / USE lines.

    Which database to build is a connection setting (MSSQL_DATABASE), so the script's hard-coded
    name is ignored and every batch runs against the configured database.
    """
    kept = [line for line in batch.splitlines() if not line.strip().upper().startswith(("CREATE DATABASE", "USE "))]
    return "\n".join(kept)


def apply_schema(reset: bool) -> None:
    db = mssql.settings()["database"]
    if reset and database_exists(db):
        print(f"dropping database {db}")
        mssql.execute(
            f"ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{db}];", database="master"
        )
    if database_exists(db):
        print(f"database {db} already exists; leaving schema alone")
        return

    mssql.execute(f"CREATE DATABASE [{db}]", database="master")
    with open(DDL_PATH) as handle:
        script = handle.read()
    for batch in split_batches(script):
        statement = strip_database_statements(batch)
        if statement.strip():
            mssql.execute(statement, database=db)
    print(f"created {db} with CDC enabled on {len(mssql.cdc_enabled_tables())} tables")


INSERT_BATCH_ROWS = 1000


def bulk_insert(table: str, csv_path: str) -> None:
    """Server-side load: fastest, but the server must be able to read the file."""
    mssql.execute(
        f"BULK INSERT dbo.{table} FROM '/data/{csv_path}' "
        "WITH (FORMAT = 'CSV', FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', TABLOCK)"
    )


def client_insert(table: str, csv_path: str) -> None:
    """Client-side load for when the server cannot see the extract directory."""
    local_path = os.path.join("sample-data", csv_path)
    with open(local_path, newline="") as handle:
        reader = csv.reader(handle)
        columns = next(reader)
        column_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        statement = f"INSERT INTO dbo.{table} ({column_list}) VALUES ({placeholders})"
        with mssql.connection() as conn, conn.cursor() as cur:
            batch: list[tuple] = []
            for row in reader:
                # Empty CSV fields are absent values, not empty strings.
                batch.append(tuple(value if value != "" else None for value in row))
                if len(batch) >= INSERT_BATCH_ROWS:
                    cur.executemany(statement, batch)
                    batch = []
            if batch:
                cur.executemany(statement, batch)


def server_can_read_extracts() -> bool:
    """Whether /data is visible to SQL Server, which decides how the load happens."""
    # dm_os_enumerate_filesystem raises when the directory is missing, so any error here means
    # "cannot see it" - a broken connection would fail on the very next statement anyway.
    try:
        return bool(
            mssql.scalar("SELECT COUNT(*) FROM sys.dm_os_enumerate_filesystem('/data/sql_ehr', 'patients.csv')")
        )
    except Exception:
        return False


def bulk_load() -> None:
    server_side = server_can_read_extracts()
    print(f"loading via {'BULK INSERT from /data' if server_side else 'batched client-side inserts'}")
    for table, csv_path in TABLES:
        existing = mssql.scalar(f"SELECT COUNT(*) FROM dbo.{table}")
        if existing:
            print(f"{table}: {existing} rows already loaded; skipping")
            continue
        if server_side:
            bulk_insert(table, csv_path)
        else:
            client_insert(table, csv_path)
        print(f"{table}: loaded {mssql.scalar(f'SELECT COUNT(*) FROM dbo.{table}')} rows")


def wait_for_capture(timeout_seconds: int = 120) -> None:
    """The capture job runs asynchronously; give it a chance to catch up before we report done."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if mssql.max_lsn():
            print(f"CDC capture is live (max LSN {mssql.max_lsn()})")
            return
        time.sleep(5)
    print("WARNING: CDC has not produced a max LSN yet; check that the SQL Agent is running", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--reset", action="store_true", help="drop and recreate the database first")
    args = parser.parse_args()

    if not mssql.is_available():
        raise SystemExit("cannot reach SQL Server; is `docker compose up -d sqlserver` running?")
    apply_schema(args.reset)
    bulk_load()
    wait_for_capture()
