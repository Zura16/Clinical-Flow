import pytest
from pyspark.sql import functions as F

from databricks.gold.keys import UNKNOWN_SK, date_key, surrogate_key, with_unknown_member
from databricks.utilities.config import get_spark_session


@pytest.fixture(scope="module")
def spark():
    return get_spark_session("Test_Gold_Keys")


def key_of(spark, *values):
    df = spark.createDataFrame([values], ",".join(f"c{i} STRING" for i in range(len(values))))
    return df.select(surrogate_key(*[F.col(f"c{i}") for i in range(len(values))]).alias("k")).first()["k"]


def test_same_natural_key_gives_same_surrogate_key(spark):
    # The property the whole design rests on: a rebuild must not renumber.
    assert key_of(spark, "sql_ehr", "P-1") == key_of(spark, "sql_ehr", "P-1")


def test_different_natural_keys_give_different_surrogate_keys(spark):
    assert key_of(spark, "sql_ehr", "P-1") != key_of(spark, "sql_ehr", "P-2")
    # Same values, different fields: the separator keeps them apart.
    assert key_of(spark, "ab", "c") != key_of(spark, "a", "bc")


def test_null_is_distinct_from_empty_string(spark):
    df = spark.createDataFrame([(None,), ("",)], "c0 STRING")
    keys = [r["k"] for r in df.select(surrogate_key(F.col("c0")).alias("k")).collect()]
    assert keys[0] != keys[1]


def test_no_real_member_claims_the_unknown_key(spark):
    assert key_of(spark, "sql_ehr", "P-1") != UNKNOWN_SK


def test_date_key_is_yyyymmdd_and_falls_back_to_unknown(spark):
    df = spark.createDataFrame([("2024-03-10 02:50:22",), (None,)], "ts STRING")
    keys = [r["k"] for r in df.select(date_key(F.col("ts").cast("timestamp")).alias("k")).collect()]
    assert keys == [20240310, UNKNOWN_SK]


def test_unknown_member_row_is_added_with_typed_columns(spark):
    df = spark.createDataFrame([(123, "Main St Clinic", 4.5)], "facility_sk LONG, facility_name STRING, score DOUBLE")
    rows = with_unknown_member(df, spark, "facility_sk").collect()
    unknown = next(r for r in rows if r["facility_sk"] == UNKNOWN_SK)
    assert unknown["facility_name"] == "UNKNOWN"  # strings say so
    assert unknown["score"] is None  # numbers stay NULL rather than pretending to be 0
    assert len(rows) == 2
