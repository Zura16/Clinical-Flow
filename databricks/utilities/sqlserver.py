"""
SQL Server access for the EHR source database.

Two paths, on purpose:
- pymssql for control statements and metadata (DDL, bulk load, CDC housekeeping, LSN functions).
- Spark JDBC for bulk reads into bronze, which is what a Databricks job would do.

Connection settings come from the environment (.env is read if present); nothing is hard-coded.
"""

import os
from contextlib import contextmanager

import pymssql

from databricks.utilities.config import BASE_DIR

# LSN: SQL Server's transaction-log position, binary(10). Rendered as 20 hex characters so it
# stays fixed-width and sorts lexicographically in the same order as the binary value.
LSN_HEX_LENGTH = 20
ZERO_LSN = "0" * LSN_HEX_LENGTH


def _load_dotenv() -> None:
    path = os.path.join(BASE_DIR, ".env")
    if not os.path.isfile(path):
        return
    with open(path) as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def settings() -> dict:
    _load_dotenv()
    password = os.environ.get("MSSQL_SA_PASSWORD")
    if not password:
        raise RuntimeError("MSSQL_SA_PASSWORD is not set (copy .env.example to .env)")
    return {
        "host": os.environ.get("MSSQL_HOST", "localhost"),
        "port": int(os.environ.get("MSSQL_PORT", "1433")),
        "user": os.environ.get("MSSQL_USER", "sa"),
        "password": password,
        "database": os.environ.get("MSSQL_DATABASE", "ehr_source"),
    }


@contextmanager
def connection(database: str | None = None, autocommit: bool = True):
    cfg = settings()
    conn = pymssql.connect(
        server=cfg["host"], port=str(cfg["port"]), user=cfg["user"], password=cfg["password"],
        database=database if database is not None else cfg["database"], autocommit=autocommit,
    )
    try:
        yield conn
    finally:
        conn.close()


def execute(sql: str, database: str | None = None) -> None:
    with connection(database) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


def query(sql: str, database: str | None = None) -> list[dict]:
    with connection(database) as conn:
        with conn.cursor(as_dict=True) as cur:
            cur.execute(sql)
            return cur.fetchall()


def scalar(sql: str, database: str | None = None):
    with connection(database) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return row[0] if row else None


def is_available() -> bool:
    try:
        with connection(database="master"):
            return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# JDBC (Spark side)
# ---------------------------------------------------------------------------

def jdbc_options(database: str | None = None) -> dict:
    cfg = settings()
    db = database if database is not None else cfg["database"]
    return {
        # trustServerCertificate: the container uses a self-signed certificate. Fine locally;
        # a deployment would validate against a real certificate instead.
        "url": f"jdbc:sqlserver://{cfg['host']}:{cfg['port']};databaseName={db};encrypt=true;trustServerCertificate=true",
        "user": cfg["user"],
        "password": cfg["password"],
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }


def read_query(spark, sql: str, database: str | None = None):
    return spark.read.format("jdbc").options(**jdbc_options(database)).option("query", sql).load()


# ---------------------------------------------------------------------------
# CDC helpers
# ---------------------------------------------------------------------------

def capture_instance(table: str) -> str:
    return f"dbo_{table}"


def business_columns(table: str) -> list[str]:
    rows = query(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table}' ORDER BY ORDINAL_POSITION"
    )
    if not rows:
        raise ValueError(f"table dbo.{table} not found in the source database")
    return [r["COLUMN_NAME"] for r in rows]


def max_lsn() -> str:
    return scalar("SELECT CONVERT(CHAR(20), sys.fn_cdc_get_max_lsn(), 2)")


def min_lsn(table: str) -> str:
    return scalar(f"SELECT CONVERT(CHAR(20), sys.fn_cdc_get_min_lsn('{capture_instance(table)}'), 2)")


def increment_lsn(lsn: str) -> str:
    return scalar(f"SELECT CONVERT(CHAR(20), sys.fn_cdc_increment_lsn(CONVERT(BINARY(10), '{lsn}', 2)), 2)")


def cdc_enabled_tables() -> list[str]:
    return [r["name"] for r in query(
        "SELECT t.name FROM sys.tables t WHERE t.is_tracked_by_cdc = 1 ORDER BY t.name"
    )]
