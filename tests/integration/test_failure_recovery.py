"""
Failure and recovery through the silver stage, without needing SQL Server.

The behaviour under test is what makes a failure survivable:
- a bad batch fails the run and leaves evidence (FAILED audit row, alert, quarantine, rule results),
- silver is untouched and the stage watermark does not move,
- so a rerun after the fix reprocesses the *same* bronze batch and lands it exactly once.
"""

import os

import pytest
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from databricks.silver.silver_engine import SilverSpec, process_spec
from databricks.utilities.alerting import ALERT_TABLE_PATH
from databricks.utilities.config import BRONZE_PATH, SILVER_PATH, get_spark_session
from databricks.utilities.control import (
    PIPELINE_CONFIG_PATH,
    PIPELINE_CONFIG_SCHEMA,
    SILVER_STAGE,
    SourceConfig,
    clear_caches,
    ensure_pipeline_config,
    get_stage_watermark,
)
from databricks.utilities.logger import AUDIT_TABLE_PATH
from databricks.utilities.quality_engine import (
    DATA_QUALITY_RESULT_PATH,
    QUARANTINE_TABLE_PATH,
    DataQualityThresholdError,
)
from databricks.utilities.quality_rules import (
    DATA_QUALITY_RULE_PATH,
    DATA_QUALITY_RULE_SCHEMA,
    QualityRule,
    ensure_rules,
)
from databricks.utilities.quality_rules import clear_caches as clear_rule_caches

BRONZE_TABLE = "bronze_recovery_test"
DATASET = "dq_recovery_test"
SPEC = SilverSpec(
    name="silver_recovery_test",
    bronze_table=BRONZE_TABLE,
    key_columns=["record_id"],
    columns={"record_id": "record_id", "result_value": "CAST(result_value AS DOUBLE)"},
    hash_columns=["record_id", "result_value"],
    dq_dataset=DATASET,
)


@pytest.fixture(scope="module")
def spark():
    return get_spark_session("Test_Failure_Recovery")


@pytest.fixture(scope="module", autouse=True)
def control_rows(spark):
    """A pipeline_config row for the test's bronze table, and one rule with a 10% threshold."""
    ensure_pipeline_config(spark)
    # active_flag=False: silver looks the row up by destination table regardless, but bronze only
    # ingests active rows, so a whole-pipeline run in the same lakehouse ignores this fixture.
    config = SourceConfig(
        "claims_csv", "recovery_test", BRONZE_TABLE, "Full", None, "record_id", "unused-by-this-test", active_flag=False
    )
    spark.createDataFrame([config.__dict__], PIPELINE_CONFIG_SCHEMA).write.format("delta").mode("append").save(
        PIPELINE_CONFIG_PATH
    )
    clear_caches()

    ensure_rules(spark)
    DeltaTable.forPath(spark, DATA_QUALITY_RULE_PATH).delete(F.col("dataset_name") == DATASET)
    rule = QualityRule(DATASET, "result_value", "RANGE", "result_value BETWEEN 0 AND 100", "ERROR", 10.0, True)
    spark.createDataFrame([rule.__dict__], DATA_QUALITY_RULE_SCHEMA).write.format("delta").mode("append").save(
        DATA_QUALITY_RULE_PATH
    )
    clear_rule_caches()


def land_in_bronze(spark, run_id, rows, replace=False):
    """Write a bronze batch the way ingest_raw_data does."""
    df = (
        spark.createDataFrame(rows, "record_id STRING, result_value DOUBLE")
        .withColumn("_pipeline_run_id", F.lit(run_id))
        .withColumn("_source_name", F.lit("claims_csv"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingest_date", F.current_date())
    )
    writer = df.write.format("delta").mode("overwrite" if replace else "append")
    if replace:
        writer = writer.option("replaceWhere", f"_pipeline_run_id = '{run_id}'")
    writer.partitionBy("_ingest_date", "_pipeline_run_id").save(os.path.join(BRONZE_PATH, BRONZE_TABLE))


def silver_rows(spark):
    path = os.path.join(SILVER_PATH, SPEC.name)
    if not os.path.isdir(os.path.join(path, "_delta_log")):
        return {}
    return {r["record_id"]: r["result_value"] for r in spark.read.format("delta").load(path).collect()}


def rows_for(spark, path, run_id):
    if not os.path.isdir(os.path.join(path, "_delta_log")):
        return []
    return spark.read.format("delta").load(path).filter(F.col("pipeline_run_id") == run_id).collect()


def test_bad_batch_fails_then_recovers_without_duplicating(spark):
    # 1. A clean batch lands normally.
    land_in_bronze(spark, "rec-run-1", [("r1", 10.0), ("r2", 20.0)])
    process_spec(spark, SPEC, "rec-run-1")
    assert silver_rows(spark) == {"r1": 10.0, "r2": 20.0}
    watermark_after_good = get_stage_watermark(spark, SILVER_STAGE, SPEC.name)
    assert watermark_after_good is not None

    # 2. A batch that is mostly bad fails the run: 2 of 3 rows is 67%, over the 10% threshold.
    land_in_bronze(spark, "rec-run-2", [("r3", 30.0), ("r4", -500.0), ("r5", 9999.0)])
    with pytest.raises(DataQualityThresholdError, match="RANGE:result_value"):
        process_spec(spark, SPEC, "rec-run-2")

    # 3. Evidence: the failure is diagnosable without rerunning anything.
    audit = rows_for(spark, AUDIT_TABLE_PATH, "rec-run-2")
    assert [(r["execution_status"], r["error_code"]) for r in audit] == [("FAILED", "DataQualityThresholdError")]
    alerts = rows_for(spark, ALERT_TABLE_PATH, "rec-run-2")
    assert len(alerts) == 1 and "DataQualityThresholdError" in alerts[0]["summary"]
    assert alerts[0]["next_step"]  # an alert that does not say what to do next is noise
    quarantined = {r["record_identifier"] for r in rows_for(spark, QUARANTINE_TABLE_PATH, "rec-run-2")}
    assert quarantined == {"r4", "r5"}
    breached = [r for r in rows_for(spark, DATA_QUALITY_RESULT_PATH, "rec-run-2") if not r["passed"]]
    assert len(breached) == 1 and breached[0]["rows_failed"] == 2

    # 4. Nothing moved: silver is unchanged and the watermark still points before the bad batch,
    #    which is what makes the rerun reprocess it.
    assert silver_rows(spark) == {"r1": 10.0, "r2": 20.0}
    assert get_stage_watermark(spark, SILVER_STAGE, SPEC.name) == watermark_after_good

    # 5. The fix: the source corrects the two bad values and the batch is re-landed.
    land_in_bronze(spark, "rec-run-2", [("r3", 30.0), ("r4", 40.0), ("r5", 50.0)], replace=True)
    process_spec(spark, SPEC, "rec-run-2")

    # 6. Each record is present exactly once, with its corrected value, and r3 - which was fine all
    #    along and sat in the failed batch - was not landed twice.
    assert silver_rows(spark) == {"r1": 10.0, "r2": 20.0, "r3": 30.0, "r4": 40.0, "r5": 50.0}
    assert get_stage_watermark(spark, SILVER_STAGE, SPEC.name) > watermark_after_good
