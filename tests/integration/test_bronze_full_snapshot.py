"""
Full loads against a throwaway CSV: a snapshot lands only when the file differs from the one the
table last landed, a restored older file counts as a change, and a missing file fails the run.
"""

import os

import pytest
from pyspark.sql import functions as F

from databricks.bronze.ingest_raw_data import ingest_table
from databricks.silver.bronze_reader import current_state
from databricks.utilities.config import BRONZE_PATH, get_spark_session
from databricks.utilities.control import SourceConfig
from databricks.utilities.logger import AUDIT_TABLE_PATH

HEADER = "claim_id,amount\n"


@pytest.fixture(scope="module")
def spark():
    return get_spark_session("Test_Bronze_Full_Snapshot")


@pytest.fixture(scope="module")
def source_csv(tmp_path_factory):
    return tmp_path_factory.mktemp("full_source") / "claims.csv"


@pytest.fixture(scope="module")
def cfg(source_csv):
    return SourceConfig("claims_csv", "test_full.csv", "bronze_test_full", "Full", None, "claim_id", str(source_csv))


def landed_by_run(spark, cfg) -> dict[str, int]:
    df = spark.read.format("delta").load(os.path.join(BRONZE_PATH, cfg.destination_table))
    return {r["_pipeline_run_id"]: r["count"] for r in df.groupBy("_pipeline_run_id").count().collect()}


def current_claims(spark, cfg) -> dict[str, str]:
    bronze = spark.read.format("delta").load(os.path.join(BRONZE_PATH, cfg.destination_table))
    return {r["claim_id"]: r["amount"] for r in current_state(bronze, cfg).collect()}


def test_full_snapshot_lands_only_when_the_file_changes(spark, cfg, source_csv):
    # 1. First run lands the snapshot.
    source_csv.write_text(HEADER + "c1,100\nc2,200\n")
    first = source_csv.stat()
    assert ingest_table(spark, cfg, "run-1") == 2

    # 2. Unchanged file: nothing lands and the file is not opened. Proven by swapping in same-size
    #    junk with the original mtime: had it been read, run-2 would have landed junk rows. The junk
    #    must parse to rows; a header-only file would land 0 rows and prove nothing.
    original = source_csv.read_bytes()
    source_csv.write_bytes(b"x\n" * (len(original) // 2) + b"x" * (len(original) % 2))
    os.utime(source_csv, ns=(first.st_atime_ns, first.st_mtime_ns))
    assert ingest_table(spark, cfg, "run-2") == 0
    source_csv.write_bytes(original)
    os.utime(source_csv, ns=(first.st_atime_ns, first.st_mtime_ns))
    assert landed_by_run(spark, cfg) == {"run-1": 2}

    # 3. The source changes (c2 removed): a new snapshot lands and becomes the current state.
    source_csv.write_text(HEADER + "c1,150\n")
    assert ingest_table(spark, cfg, "run-3") == 1
    assert current_claims(spark, cfg) == {"c1": "150"}

    # 4. Yesterday's file is restored with its old size and mtime (cp -p from a backup). It matches
    #    run-1's log entry but not the latest one, so it is a change and must land.
    source_csv.write_bytes(original)
    os.utime(source_csv, ns=(first.st_atime_ns, first.st_mtime_ns))
    assert ingest_table(spark, cfg, "run-4") == 2
    assert current_claims(spark, cfg) == {"c1": "100", "c2": "200"}

    # 5. A header-only extract fails the run. Landing it would either change nothing (no rows, no
    #    partition) or, as an empty snapshot, soft-delete every claim. A second run fails too, so
    #    the file was never logged as read and cannot pass later as "unchanged".
    source_csv.write_text(HEADER)
    with pytest.raises(ValueError, match="0 rows"):
        ingest_table(spark, cfg, "run-5")
    with pytest.raises(ValueError, match="0 rows"):
        ingest_table(spark, cfg, "run-6")
    assert statuses(spark, cfg, "run-5") == ["FAILED"]
    assert current_claims(spark, cfg) == {"c1": "100", "c2": "200"}

    # 6. The extract disappears: the run fails loudly instead of treating it as "nothing changed"
    #    or as an empty snapshot that would delete every claim downstream.
    source_csv.unlink()
    with pytest.raises(Exception, match="(?i)path does not exist|PATH_NOT_FOUND"):
        ingest_table(spark, cfg, "run-7")
    assert statuses(spark, cfg, "run-7") == ["FAILED"]
    assert landed_by_run(spark, cfg) == {"run-1": 2, "run-3": 1, "run-4": 2}


def statuses(spark, cfg, run_id: str) -> list[str]:
    rows = (
        spark.read.format("delta")
        .load(AUDIT_TABLE_PATH)
        .filter((F.col("pipeline_run_id") == run_id) & (F.col("pipeline_name") == f"bronze:{cfg.destination_table}"))
        .select("execution_status")
        .collect()
    )
    return [r["execution_status"] for r in rows]
