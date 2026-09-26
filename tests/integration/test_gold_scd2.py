"""
SCD Type 2 on dim_patient, and the point-in-time join that makes it worth having.

Drives build_dim_patient over a two-patient silver table, so the versions and their windows are
small enough to assert exactly.
"""

import pytest
from pyspark.sql import functions as F

from databricks.gold.build_dimensions import build_dim_patient, gold_path
from databricks.gold.build_facts import patient_sk_as_of
from databricks.gold.keys import UNKNOWN_SK
from databricks.utilities.config import get_spark_session

T0 = "2024-01-01 00:00:00"   # when both patients were last updated at source
T1 = "2026-05-01 12:00:00"   # when patient 1 moves


@pytest.fixture(scope="module")
def spark():
    return get_spark_session("Test_Gold_SCD2")


def patients(spark, rows):
    """A silver-shaped patient batch: (patient_id, address_street, source_updated_at, is_deleted).

    Passed straight to build_dim_patient rather than written to silver_ehr_patients, so this test
    cannot disturb the tables the rest of the suite reads.
    """
    df = spark.createDataFrame(rows, "patient_id STRING, address_street STRING, source_updated_at STRING, _is_deleted BOOLEAN")
    return (
        df
        .withColumn("source_system", F.lit("sql_ehr"))
        .withColumn("source_updated_at", F.col("source_updated_at").cast("timestamp"))
        .withColumn("first_name", F.lit("Test")).withColumn("last_name", F.lit("Patient"))
        .withColumn("date_of_birth", F.lit("1980-01-01").cast("date"))
        .withColumn("gender", F.lit("Female"))
        .withColumn("city", F.lit("Seattle")).withColumn("state", F.lit("WA"))
        .withColumn("postal_code", F.lit("98101")).withColumn("phone_number", F.lit(None).cast("string"))
        .withColumn("insurance_type", F.lit("Commercial"))
        .withColumn("_updated_at", F.current_timestamp())
        .withColumn("record_hash", F.sha2(F.concat_ws("||", "patient_id", "address_street"), 256))
    )


def versions(spark, patient_id):
    """Versions of one patient, with the window formatted by Spark.

    Timestamps are formatted in the query rather than read as Python datetimes: PySpark converts
    timestamps to the driver's local zone on collect, which would make a stored 1900-01-01 UTC
    arrive as 1899-12-31 and turn an assertion about UTC into an assertion about the test machine.
    """
    return (
        spark.read.format("delta").load(gold_path("dim_patient"))
        .filter(F.col("patient_id") == patient_id)
        .withColumn("start_utc", F.date_format("effective_start_date", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("end_utc", F.date_format("effective_end_date", "yyyy-MM-dd HH:mm:ss"))
        .orderBy("effective_start_date")
        .collect()
    )


def test_scd2_lifecycle_and_point_in_time_join(spark):
    # 1. First build: one version per patient, opened at the beginning of time so that facts
    #    predating our first sight of the record still resolve.
    build_dim_patient(spark, "scd2-run-1",
                      patients(spark, [("P1", "1 Old St", T0, False), ("P2", "2 Elm St", T0, False)]))

    first = versions(spark, "P1")
    assert len(first) == 1
    assert first[0]["is_current"] is True and first[0]["end_utc"] is None
    assert first[0]["start_utc"] == "1900-01-01 00:00:00"

    # 2. Rebuilding with unchanged data changes nothing: no second version, no renumbering.
    original_sk = first[0]["patient_sk"]
    unchanged = patients(spark, [("P1", "1 Old St", T0, False), ("P2", "2 Elm St", T0, False)])
    assert build_dim_patient(spark, "scd2-run-2", unchanged) == 0
    assert [r["patient_sk"] for r in versions(spark, "P1")] == [original_sk]

    # 3. P1 moves: the old version closes exactly where the new one opens.
    moved = patients(spark, [("P1", "9 New Ave", T1, False), ("P2", "2 Elm St", T0, False)])
    assert build_dim_patient(spark, "scd2-run-3", moved) == 1

    p1 = versions(spark, "P1")
    assert len(p1) == 2
    old, new = p1
    assert old["address_street"] == "1 Old St" and old["is_current"] is False
    assert new["address_street"] == "9 New Ave" and new["is_current"] is True
    assert old["end_utc"] == new["start_utc"] == "2026-05-01 12:00:00"  # contiguous: no gap, no overlap
    assert new["end_utc"] is None
    assert old["patient_sk"] != new["patient_sk"]

    # 4. The point of all this: an event resolves to the version current when it happened.
    events = spark.createDataFrame(
        [("P1", "2025-06-01 09:00:00"), ("P1", "2026-06-01 09:00:00"), ("P_unknown", "2026-06-01 09:00:00")],
        "patient_id STRING, event_time STRING",
    ).withColumn("event_time", F.col("event_time").cast("timestamp"))
    resolved = {
        r["patient_id"] + "@" + str(r["event_time"].year): r["patient_sk"]
        for r in patient_sk_as_of(spark, events, F.col("patient_id"), F.col("event_time"), "sql_ehr").collect()
    }
    assert resolved["P1@2025"] == old["patient_sk"]        # before the move: the old version
    assert resolved["P1@2026"] == new["patient_sk"]        # after the move: the new version
    assert resolved["P_unknown@2026"] == UNKNOWN_SK        # no such patient: the unknown member

    # 5. A deletion is a version too, so the warehouse records when the record went away.
    with_delete = patients(spark, [("P1", "9 New Ave", T1, False), ("P2", "2 Elm St", T1, True)])
    assert build_dim_patient(spark, "scd2-run-4", with_delete) == 1
    p2 = versions(spark, "P2")
    assert [r["is_deleted"] for r in p2] == [False, True]
    assert p2[0]["is_current"] is False and p2[1]["is_current"] is True
