"""
Bronze incremental behavior against a throwaway CSV source:
first load, incremental load after a source change, rerun of a finished run, a loud failure,
and a retry of the failed run.
"""

import os

import pytest
from pyspark.sql import functions as F

from databricks.bronze.ingest_raw_data import ingest_table
from databricks.silver.bronze_reader import current_state
from databricks.utilities.config import BRONZE_PATH, get_spark_session
from databricks.utilities.control import SourceConfig, get_watermark
from databricks.utilities.logger import AUDIT_TABLE_PATH

HEADER = "patient_id,address_street,updated_at\n"


@pytest.fixture(scope="module")
def spark():
    return get_spark_session("Test_Bronze_Incremental")


@pytest.fixture(scope="module")
def source_csv(tmp_path_factory):
    return tmp_path_factory.mktemp("source") / "patients.csv"


@pytest.fixture(scope="module")
def cfg(source_csv):
    # Absolute source_location: os.path.join(BASE_DIR, abs_path) returns abs_path.
    # sql_ehr selects the CSV reader; the unique source_table keeps this test's watermark separate.
    return SourceConfig("sql_ehr", "test_patients", "bronze_test_patients", "Watermark",
                        "updated_at", "patient_id", str(source_csv))


def landed_by_run(spark, cfg):
    df = spark.read.format("delta").load(os.path.join(BRONZE_PATH, cfg.destination_table))
    return {r["_pipeline_run_id"]: r["count"] for r in df.groupBy("_pipeline_run_id").count().collect()}


def test_bronze_incremental_lifecycle(spark, cfg, source_csv):
    # 1. First load lands everything and sets the watermark to the newest row.
    source_csv.write_text(HEADER + "p1,1 Old St,2024-01-01 00:00:00\np2,2 Elm St,2024-01-02 00:00:00\n")
    assert ingest_table(spark, cfg, "run-a") == 2
    assert get_watermark(spark, cfg) == "2024-01-02 00:00:00.000000"

    # 2. p1 moves and p3 is added: only those two rows land.
    source_csv.write_text(HEADER + "p1,9 New Ave,2024-03-01 00:00:00\np2,2 Elm St,2024-01-02 00:00:00\n"
                                   "p3,3 Oak Rd,2024-03-02 00:00:00\n")
    assert ingest_table(spark, cfg, "run-b") == 2
    assert get_watermark(spark, cfg) == "2024-03-02 00:00:00.000000"

    bronze = spark.read.format("delta").load(os.path.join(BRONZE_PATH, cfg.destination_table))
    current = {r["patient_id"]: r["address_street"] for r in current_state(bronze, cfg).collect()}
    assert current == {"p1": "9 New Ave", "p2": "2 Elm St", "p3": "3 Oak Rd"}

    # 3. Rerunning run-a after the source changed is skipped: its partition still holds p1's
    #    old address (the only copy left anywhere) and the watermark does not move backwards.
    assert ingest_table(spark, cfg, "run-a") == 2
    assert landed_by_run(spark, cfg) == {"run-a": 2, "run-b": 2}
    run_a = bronze.filter(F.col("_pipeline_run_id") == "run-a")
    assert {r["patient_id"]: r["address_street"] for r in run_a.collect()} == {"p1": "1 Old St", "p2": "2 Elm St"}
    assert get_watermark(spark, cfg) == "2024-03-02 00:00:00.000000"

    # 4. A row with an unparseable watermark fails the run loudly: FAILED audit row,
    #    nothing landed for the run, watermark untouched.
    source_csv.write_text(HEADER + "p4,4 Pine St,not-a-date\n")
    with pytest.raises(ValueError, match="unparseable updated_at"):
        ingest_table(spark, cfg, "run-c")
    assert landed_by_run(spark, cfg) == {"run-a": 2, "run-b": 2}
    assert get_watermark(spark, cfg) == "2024-03-02 00:00:00.000000"
    failed = (
        spark.read.format("delta").load(AUDIT_TABLE_PATH)
        .filter((F.col("pipeline_run_id") == "run-c") & (F.col("pipeline_name") == "bronze:bronze_test_patients"))
        .select("execution_status", "error_code")
        .collect()
    )
    assert [(r["execution_status"], r["error_code"]) for r in failed] == [("FAILED", "ValueError")]

    # 5. Once the source is fixed, retrying run-c lands what it missed, exactly once.
    source_csv.write_text(HEADER + "p4,4 Pine St,2024-04-01 00:00:00\n")
    assert ingest_table(spark, cfg, "run-c") == 1
    assert ingest_table(spark, cfg, "run-c") == 1  # a second retry is skipped
    assert landed_by_run(spark, cfg) == {"run-a": 2, "run-b": 2, "run-c": 1}
    assert get_watermark(spark, cfg) == "2024-04-01 00:00:00.000000"
