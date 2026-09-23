"""
Silver merge semantics, exercised directly against Delta (no bronze or SQL Server needed):
inserts, version-guarded updates, out-of-order batches, CDC soft deletes, and snapshot deletes.
"""

import os

import pytest
from pyspark.sql import functions as F

from databricks.silver.silver_engine import SilverSpec, collapse_batch, merge_into_silver, to_silver_columns, version_expr
from databricks.utilities.config import SILVER_PATH, get_spark_session
from databricks.utilities.control import SourceConfig

CDC_CFG = SourceConfig("sql_ehr", "patients", "bronze_ehr_patients", "CDC", "_cdc_lsn", "patient_id", "dbo.patients")
FULL_CFG = SourceConfig("claims_csv", "f.csv", "bronze_facilities", "Full", None, "facility_id", "f.csv")

SPEC = SilverSpec(
    name="silver_merge_test_patients",
    bronze_table="bronze_ehr_patients",
    key_columns=["patient_id"],
    columns={"patient_id": "patient_id", "address_street": "address_street"},
    hash_columns=["patient_id", "address_street"],
)
FULL_SPEC = SilverSpec(
    name="silver_merge_test_facilities",
    bronze_table="bronze_facilities",
    key_columns=["facility_id"],
    columns={"facility_id": "facility_id", "facility_name": "facility_name"},
    hash_columns=["facility_id", "facility_name"],
)


@pytest.fixture(scope="module")
def spark():
    return get_spark_session("Test_Silver_Merge")


def raw_cdc(spark, rows):
    """Bronze-shaped CDC rows: (patient_id, address, lsn, seqval, operation)."""
    df = spark.createDataFrame(rows, "patient_id STRING, address_street STRING, _cdc_lsn STRING, "
                                     "_cdc_seqval STRING, _cdc_operation INT")
    df = df.withColumn("_ingested_at", F.current_timestamp()).withColumn("_pipeline_run_id", F.lit("t"))
    return df.withColumn("_version", version_expr(CDC_CFG))


def cdc_batch(spark, rows):
    """What the engine hands to the merge: collapsed, then mapped to silver columns."""
    return to_silver_columns(collapse_batch(raw_cdc(spark, rows), ["patient_id"]), SPEC, CDC_CFG)


def silver_rows(spark, spec):
    df = spark.read.format("delta").load(os.path.join(SILVER_PATH, spec.name))
    return {r[spec.key_columns[0]]: r for r in df.collect()}


def test_insert_then_version_guarded_update(spark):
    merge_into_silver(spark, cdc_batch(spark, [
        ("p1", "1 Old St", "00000000000000000010", "0" * 20, 2),
        ("p2", "2 Elm St", "00000000000000000010", "0" * 20, 2),
    ]), SPEC, CDC_CFG)
    assert {k: r["address_street"] for k, r in silver_rows(spark, SPEC).items()} == {"p1": "1 Old St", "p2": "2 Elm St"}

    # A newer log position updates the row.
    merge_into_silver(spark, cdc_batch(spark, [("p1", "9 New Ave", "00000000000000000020", "0" * 20, 4)]), SPEC, CDC_CFG)
    assert silver_rows(spark, SPEC)["p1"]["address_street"] == "9 New Ave"

    # An older log position (a replayed or out-of-order batch) must not win.
    merge_into_silver(spark, cdc_batch(spark, [("p1", "STALE", "00000000000000000015", "0" * 20, 4)]), SPEC, CDC_CFG)
    assert silver_rows(spark, SPEC)["p1"]["address_street"] == "9 New Ave"


def test_cdc_delete_is_soft(spark):
    merge_into_silver(spark, cdc_batch(spark, [("p2", "2 Elm St", "00000000000000000030", "0" * 20, 1)]), SPEC, CDC_CFG)
    row = silver_rows(spark, SPEC)["p2"]
    assert row["_is_deleted"] is True
    assert row["_deleted_at"] is not None
    # The record keeps its last known values: the deletion is auditable.
    assert row["address_street"] == "2 Elm St"
    # Untouched rows are unaffected.
    assert silver_rows(spark, SPEC)["p1"]["_is_deleted"] is False


def test_collapse_keeps_last_change_in_a_batch(spark):
    # One batch containing insert then update for the same key collapses to the update.
    collapsed = collapse_batch(raw_cdc(spark, [
        ("p9", "first", "00000000000000000040", "0" * 20, 2),
        ("p9", "second", "00000000000000000041", "0" * 20, 4),
    ]), ["patient_id"])
    assert collapsed.count() == 1
    assert collapsed.first()["address_street"] == "second"


def test_full_snapshot_soft_deletes_missing_keys(spark):
    def snapshot(rows):
        df = spark.createDataFrame(rows, "facility_id STRING, facility_name STRING")
        df = df.withColumn("_ingested_at", F.current_timestamp()).withColumn("_pipeline_run_id", F.lit("t"))
        return to_silver_columns(df.withColumn("_version", version_expr(FULL_CFG)), FULL_SPEC, FULL_CFG)

    merge_into_silver(spark, snapshot([("f1", "Main"), ("f2", "Valley")]), FULL_SPEC, FULL_CFG)
    assert {k: r["_is_deleted"] for k, r in silver_rows(spark, FULL_SPEC).items()} == {"f1": False, "f2": False}

    # f2 is gone from the new snapshot: it was removed at the source.
    merge_into_silver(spark, snapshot([("f1", "Main")]), FULL_SPEC, FULL_CFG)
    rows = silver_rows(spark, FULL_SPEC)
    assert rows["f2"]["_is_deleted"] is True and rows["f2"]["facility_name"] == "Valley"
    assert rows["f1"]["_is_deleted"] is False
