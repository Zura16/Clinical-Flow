import pytest

from databricks.bronze.ingest_raw_data import count_bad_watermarks, max_watermark, select_increment
from databricks.silver.bronze_reader import latest_per_key, latest_snapshot
from databricks.utilities.config import get_spark_session


@pytest.fixture(scope="module")
def spark():
    return get_spark_session("Test_Bronze_Increment")


@pytest.fixture(scope="module")
def rows(spark):
    return spark.createDataFrame(
        [("p1", "2024-01-01 00:00:00"), ("p2", "2024-02-01 00:00:00"), ("p3", "2024-03-01 00:00:00")],
        ["patient_id", "updated_at"],
    )


def ids(df):
    return sorted(r["patient_id"] for r in df.collect())


def test_first_load_takes_everything(rows):
    assert ids(select_increment(rows, "updated_at", None)) == ["p1", "p2", "p3"]


def test_start_bound_is_exclusive(rows):
    # A row exactly at the stored watermark was landed by the previous run.
    assert ids(select_increment(rows, "updated_at", "2024-02-01 00:00:00")) == ["p3"]


def test_max_watermark_is_formatted_to_microseconds(rows):
    assert max_watermark(rows, "updated_at") == "2024-03-01 00:00:00.000000"


def test_missing_and_unparseable_watermarks_are_counted(spark):
    df = spark.createDataFrame(
        [("p1", "2024-01-01 00:00:00"), ("p2", None), ("p3", "not-a-date")],
        "patient_id STRING, updated_at STRING",
    )
    assert count_bad_watermarks(df, "updated_at") == 2


def test_latest_per_key_keeps_newest_version(spark):
    df = spark.createDataFrame(
        [
            ("p1", "old st", "2024-01-01 00:00:00", "2024-01-02 00:00:00"),
            ("p1", "new st", "2024-06-01 00:00:00", "2024-06-02 00:00:00"),
            ("p2", "only st", "2024-01-01 00:00:00", "2024-01-02 00:00:00"),
        ],
        "patient_id STRING, address STRING, updated_at STRING, _ingested_at STRING",
    ).selectExpr("patient_id", "address", "updated_at", "to_timestamp(_ingested_at) AS _ingested_at")
    result = {r["patient_id"]: r["address"] for r in latest_per_key(df, ["patient_id"], "updated_at").collect()}
    assert result == {"p1": "new st", "p2": "only st"}


def test_latest_snapshot_drops_keys_removed_from_the_source(spark):
    df = spark.createDataFrame(
        [
            ("c1", "run-a", "2024-01-01 00:00:00"),
            ("c2", "run-a", "2024-01-01 00:00:00"),
            ("c1", "run-b", "2024-02-01 00:00:00"),  # c2 no longer in the newest snapshot
        ],
        "claim_id STRING, _pipeline_run_id STRING, _ingested_at STRING",
    ).selectExpr("claim_id", "_pipeline_run_id", "to_timestamp(_ingested_at) AS _ingested_at")
    assert [r["claim_id"] for r in latest_snapshot(df).collect()] == ["c1"]
