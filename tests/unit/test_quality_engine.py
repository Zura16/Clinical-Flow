"""
Data quality engine: one test per rule type, plus severity, thresholds and quarantine idempotency.
Rules are written directly into the rule table so each test states the rule it exercises.
"""

import pytest
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from databricks.utilities.config import get_spark_session
from databricks.utilities.quality_engine import (
    DATA_QUALITY_RESULT_PATH,
    QUARANTINE_TABLE_PATH,
    DataQualityEngine,
    DataQualityThresholdError,
)
from databricks.utilities.quality_rules import (
    DATA_QUALITY_RULE_PATH,
    DATA_QUALITY_RULE_SCHEMA,
    QualityRule,
    clear_caches,
    ensure_rules,
)


@pytest.fixture(scope="module")
def spark():
    return get_spark_session("Test_Quality_Engine")


def install_rules(spark, rules):
    """Replace the rules for these datasets with exactly the ones given."""
    ensure_rules(spark)
    table = DeltaTable.forPath(spark, DATA_QUALITY_RULE_PATH)
    for dataset in {r.dataset_name for r in rules}:
        table.delete(F.col("dataset_name") == dataset)
    spark.createDataFrame([r.__dict__ for r in rules], DATA_QUALITY_RULE_SCHEMA) \
        .write.format("delta").mode("append").save(DATA_QUALITY_RULE_PATH)
    clear_caches()


def observations(spark, rows):
    return spark.createDataFrame(rows, "observation_id STRING, patient_id STRING, result_value DOUBLE, loinc_code STRING")


def quarantined(spark, run_id):
    return spark.read.format("delta").load(QUARANTINE_TABLE_PATH).filter(F.col("pipeline_run_id") == run_id)


def test_range_rule_quarantines_and_keeps_good_rows(spark):
    install_rules(spark, [QualityRule("dq_range", "result_value", "RANGE",
                                      "result_value IS NULL OR result_value BETWEEN -500 AND 50000", "ERROR", 50.0)])
    df = observations(spark, [("o1", "p1", 100.0, "2345-7"), ("o2", "p2", -9999.0, "2345-7")])
    valid, rejected, results = DataQualityEngine(spark, "dq_range", "r1").validate(df, id_col="observation_id")

    assert [r["observation_id"] for r in valid.collect()] == ["o1"]
    assert rejected == 1
    assert results[0]["rows_checked"] == 2 and results[0]["rows_failed"] == 1
    assert results[0]["failure_rate_pct"] == 50.0 and results[0]["passed"] is True
    # The rejected row is recoverable: its payload is kept, not just its id.
    row = quarantined(spark, "r1").first()
    assert row["record_identifier"] == "o2" and "-9999" in row["raw_payload"]


def test_warning_severity_records_but_does_not_remove(spark):
    install_rules(spark, [QualityRule("dq_warn", "loinc_code", "REGEX",
                                      "loinc_code RLIKE '^[0-9]{1,5}-[0-9]$'", "WARNING", 100.0)])
    df = observations(spark, [("o1", "p1", 1.0, "2345-7"), ("o2", "p2", 1.0, "NOT-A-LOINC")])
    valid, rejected, results = DataQualityEngine(spark, "dq_warn", "r2").validate(df, id_col="observation_id")

    assert valid.count() == 2 and rejected == 0        # the row continues
    assert results[0]["rows_failed"] == 1              # but the violation is on record
    assert quarantined(spark, "r2").count() == 1


def test_unique_rule_keeps_one_row_per_key(spark):
    install_rules(spark, [QualityRule("dq_unique", "observation_id", "UNIQUE", "observation_id", "CRITICAL", 50.0)])
    df = observations(spark, [("o1", "p1", 1.0, "x"), ("o1", "p1", 2.0, "x"), ("o2", "p2", 3.0, "x")])
    valid, rejected, _ = DataQualityEngine(spark, "dq_unique", "r3").validate(df, id_col="observation_id")

    # Duplicates are quarantined; the key itself is not dropped from silver.
    assert rejected == 1
    assert sorted(r["observation_id"] for r in valid.collect()) == ["o1", "o2"]


def test_referential_rule_flags_unknown_parents(spark, tmp_path_factory):
    from databricks.utilities.config import SILVER_PATH
    import os

    spark.createDataFrame([("p1",), ("p2",)], "patient_id STRING") \
        .write.format("delta").mode("overwrite").save(os.path.join(SILVER_PATH, "dq_ref_patients"))
    install_rules(spark, [QualityRule("dq_ref", "patient_id", "REFERENTIAL",
                                      "dq_ref_patients.patient_id", "ERROR", 50.0)])
    df = observations(spark, [("o1", "p1", 1.0, "x"), ("o2", "p_missing", 2.0, "x")])
    valid, rejected, results = DataQualityEngine(spark, "dq_ref", "r4").validate(df, id_col="observation_id")

    assert [r["observation_id"] for r in valid.collect()] == ["o1"]
    assert rejected == 1 and results[0]["rows_failed"] == 1


def test_threshold_breach_fails_the_run(spark):
    install_rules(spark, [QualityRule("dq_threshold", "patient_id", "NOT_NULL",
                                      "patient_id IS NOT NULL", "CRITICAL", 0.0)])
    df = observations(spark, [("o1", "p1", 1.0, "x"), ("o2", None, 2.0, "x")])
    with pytest.raises(DataQualityThresholdError, match="NOT_NULL:patient_id"):
        DataQualityEngine(spark, "dq_threshold", "r5").validate(df, id_col="observation_id")

    # The failure is still evidence: the bad row is in quarantine and the result is recorded.
    assert quarantined(spark, "r5").count() == 1
    result = spark.read.format("delta").load(DATA_QUALITY_RESULT_PATH).filter("pipeline_run_id = 'r5'").first()
    assert result["passed"] is False and result["rows_failed"] == 1


def test_passing_rules_are_recorded_too(spark):
    install_rules(spark, [QualityRule("dq_pass", "observation_id", "NOT_NULL",
                                      "observation_id IS NOT NULL", "CRITICAL", 0.0)])
    df = observations(spark, [("o1", "p1", 1.0, "x")])
    _, rejected, results = DataQualityEngine(spark, "dq_pass", "r6").validate(df, id_col="observation_id")

    assert rejected == 0
    assert results[0]["rows_failed"] == 0 and results[0]["passed"] is True
    stored = spark.read.format("delta").load(DATA_QUALITY_RESULT_PATH).filter("pipeline_run_id = 'r6'").count()
    assert stored == 1


def test_rerunning_the_same_batch_does_not_duplicate_quarantine(spark):
    install_rules(spark, [QualityRule("dq_idem", "result_value", "RANGE",
                                      "result_value >= 0", "ERROR", 100.0)])
    df = observations(spark, [("o1", "p1", -5.0, "x")])
    engine = DataQualityEngine(spark, "dq_idem", "r7")
    engine.validate(df, id_col="observation_id")
    engine.validate(df, id_col="observation_id")

    assert quarantined(spark, "r7").count() == 1
    assert spark.read.format("delta").load(DATA_QUALITY_RESULT_PATH).filter("pipeline_run_id = 'r7'").count() == 1
