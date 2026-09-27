#!/usr/bin/env python3
"""
Controlled failure and recovery demonstration.

Nothing here is simulated except the bad data: the rows go into SQL Server, CDC carries them,
a real quality threshold fails a real run, and the recovery is the same code path an operator
would use.

    python -m databricks.utilities.failure_simulation [--bad-rows 500]

The story it tells, in order:
  1. Baseline. Counts across bronze, silver, gold and quarantine.
  2. Injection. 500 lab results are corrupted at source: a negative result value and a result
     timestamped before its order.
  3. Failure. Bronze lands them (raw data always lands - that is what bronze is for), then silver
     fails the RANGE rules because the bad rows are most of the batch. The run writes a FAILED
     audit row and raises an alert. The stage watermark does NOT move.
  4. Evidence. The quarantined rows with their payloads, the rule results, the audit row, the alert.
  5. Correction. The operator fixes the source rows.
  6. Replay. The same silver stage runs again. It reprocesses the same bronze batch, because the
     watermark never advanced, and this time it passes.
  7. Proof. Counts reconcile to the baseline: the valid rows were not duplicated, the corrected
     rows are present once, and bronze still holds every version including the corrupt ones.
"""

import argparse
import uuid

from pyspark.sql import functions as F

from databricks.bronze.ingest_raw_data import run_bronze_ingestion
from databricks.gold.build_facts import build_gold_facts
from databricks.silver.silver_engine import process_spec
from databricks.silver.specs import ALL_SPECS
from databricks.utilities import sqlserver as mssql
from databricks.utilities.alerting import ALERT_TABLE_PATH
from databricks.utilities.config import BRONZE_PATH, GOLD_PATH, SILVER_PATH, get_spark_session
from databricks.utilities.control import SILVER_STAGE, get_stage_watermark
from databricks.utilities.logger import AUDIT_TABLE_PATH
from databricks.utilities.quality_engine import (
    DATA_QUALITY_RESULT_PATH,
    QUARANTINE_TABLE_PATH,
    DataQualityThresholdError,
)
from databricks.utilities.quality_rules import DATA_QUALITY_RULE_PATH

LAB_SPEC = next(s for s in ALL_SPECS if s.name == "silver_ehr_lab_results")
BAD_VALUE = -99999.0


def banner(step: str, title: str) -> None:
    print(f"\n{'=' * 78}\n  [{step}] {title}\n{'=' * 78}")


def table_count(spark, root, table, live_only: bool = False) -> int:
    import os
    path = os.path.join(root, table)
    if not os.path.isdir(os.path.join(path, "_delta_log")):
        return 0
    df = spark.read.format("delta").load(path)
    if live_only and "_is_deleted" in df.columns:
        df = df.filter(~F.col("_is_deleted"))
    return df.count()


def snapshot(spark) -> dict:
    return {
        "source lab_results": mssql.scalar("SELECT COUNT(*) FROM dbo.lab_results"),
        "bronze lab rows (all versions)": table_count(spark, BRONZE_PATH, "bronze_ehr_lab_results"),
        # Silver keeps deleted records with a flag, so only the live ones can match the source.
        "silver lab rows (live)": table_count(spark, SILVER_PATH, "silver_ehr_lab_results", live_only=True),
        "silver lab rows (incl. soft-deleted)": table_count(spark, SILVER_PATH, "silver_ehr_lab_results"),
        "gold fact_observation": table_count(spark, GOLD_PATH, "fact_observation"),
        "quarantine rows": table_count(spark, QUARANTINE_TABLE_PATH.rsplit("/", 1)[0],
                                       QUARANTINE_TABLE_PATH.rsplit("/", 1)[1]),
    }


def print_counts(label: str, counts: dict) -> None:
    print(f"{label}:")
    for name, value in counts.items():
        print(f"    {name:34} {value}")


def corrupt_lab_results(rows: int) -> list[str]:
    """Corrupt real rows at source: a negative value, and a result before its own order."""
    ids = [r["lab_result_id"] for r in mssql.query(
        f"SELECT TOP {rows} lab_result_id FROM dbo.lab_results ORDER BY lab_result_id")]
    id_list = "', '".join(ids)
    mssql.execute(
        f"UPDATE dbo.lab_results SET result_value = {BAD_VALUE}, "
        "result_timestamp = DATEADD(MINUTE, -30, order_timestamp), updated_at = SYSUTCDATETIME() "
        f"WHERE lab_result_id IN ('{id_list}')"
    )
    return ids


def repair_lab_results(ids: list[str]) -> None:
    """The fix an operator would apply: plausible value, result after order."""
    id_list = "', '".join(ids)
    mssql.execute(
        "UPDATE dbo.lab_results SET result_value = 5.0, "
        "result_timestamp = DATEADD(MINUTE, 45, order_timestamp), updated_at = SYSUTCDATETIME() "
        f"WHERE lab_result_id IN ('{id_list}')"
    )


VIOLATION_PREDICATE = ("result_value < -500 OR result_value > 50000 "
                       "OR result_timestamp < order_timestamp")


def source_violations() -> int:
    return mssql.scalar(f"SELECT COUNT(*) FROM dbo.lab_results WHERE {VIOLATION_PREDICATE}")


def ensure_clean_baseline() -> None:
    """The demo has to start from a source the rules accept, or step 1 fails instead of step 3.

    A previous aborted run leaves its corruption behind, so repair anything outstanding first.
    """
    outstanding = source_violations()
    if not outstanding:
        return
    print(f"    repairing {outstanding} pre-existing violation(s) left by an earlier run")
    mssql.execute(
        "UPDATE dbo.lab_results SET result_value = 5.0, "
        "result_timestamp = DATEADD(MINUTE, 45, order_timestamp), updated_at = SYSUTCDATETIME() "
        f"WHERE {VIOLATION_PREDICATE}"
    )
    mssql.wait_for_capture()


def show(spark, path, condition, columns, title, limit=5) -> int:
    import os
    if not os.path.isdir(os.path.join(path, "_delta_log")):
        print(f"    {title}: table does not exist yet")
        return 0
    df = spark.read.format("delta").load(path).filter(condition)
    total = df.count()
    print(f"    {title}: {total} row(s)")
    if total:
        df.select(*columns).show(limit, truncate=60)
    return total


def run_demo(bad_rows: int = 500) -> None:
    spark = get_spark_session("ClinicalFlow_Failure_Demo")
    spark.sparkContext.setLogLevel("ERROR")
    if not mssql.is_available():
        raise SystemExit("SQL Server is not reachable: docker compose up -d sqlserver")

    run_baseline = f"demo-baseline-{uuid.uuid4().hex[:6]}"
    run_bad = f"demo-bad-{uuid.uuid4().hex[:6]}"
    run_fixed = f"demo-fixed-{uuid.uuid4().hex[:6]}"

    banner("1", "Baseline")
    ensure_clean_baseline()
    run_bronze_ingestion(spark, run_baseline, source_name="sql_ehr", source_table="lab_results")
    process_spec(spark, LAB_SPEC, run_baseline)
    baseline = snapshot(spark)
    print_counts("  baseline counts", baseline)
    watermark_before = get_stage_watermark(spark, SILVER_STAGE, LAB_SPEC.name)
    print(f"    silver stage watermark             {watermark_before}")

    corrupted: list[str] = []
    repaired = False
    try:
        banner("2", f"Injecting {bad_rows} corrupt lab results at source")
        corrupted = corrupt_lab_results(bad_rows)
        print(f"    corrupted {len(corrupted)} rows in dbo.lab_results")
        print(f"    result_value = {BAD_VALUE} (plausible range is -500..50000)")
        print("    result_timestamp = order_timestamp - 30 minutes (resulted before it was ordered)")
        print(f"    example id: {corrupted[0]}")

        banner("3", "Running the pipeline: bronze lands them, silver rejects them")
        print(f"    waiting for CDC capture... caught up to {mssql.wait_for_capture()}")
        run_bronze_ingestion(spark, run_bad, source_name="sql_ehr", source_table="lab_results")
        print("    bronze landed the corrupt rows: raw data is kept exactly as the source sent it")
        failure = None
        try:
            process_spec(spark, LAB_SPEC, run_bad)
        except DataQualityThresholdError as exc:
            failure = exc
        if failure is None:
            raise SystemExit("expected the quality gate to fail the run, but it passed: check the rule thresholds")
        print(f"    silver failed as designed: {failure}")

        banner("4", "Evidence left behind")
        show(spark, AUDIT_TABLE_PATH, f"pipeline_run_id = '{run_bad}' AND execution_status = 'FAILED'",
             ["pipeline_name", "execution_status", "error_code", "rows_read"], "FAILED audit rows")
        show(spark, ALERT_TABLE_PATH, f"pipeline_run_id = '{run_bad}'",
             ["pipeline_name", "severity", "summary", "next_step"], "alerts raised", limit=2)
        show(spark, QUARANTINE_TABLE_PATH, f"pipeline_run_id = '{run_bad}'",
             ["record_identifier", "failed_rule", "error_message"], "quarantined records", limit=3)
        show(spark, DATA_QUALITY_RESULT_PATH, f"pipeline_run_id = '{run_bad}' AND NOT passed",
             ["rule_name", "rows_checked", "rows_failed", "failure_rate_pct", "failure_threshold_pct"],
             "rules over threshold")
        watermark_after_failure = get_stage_watermark(spark, SILVER_STAGE, LAB_SPEC.name)
        print(f"    stage watermark unchanged: {watermark_after_failure == watermark_before} "
              f"({watermark_after_failure})")
        print(f"    silver row count unchanged: "
              f"{table_count(spark, SILVER_PATH, 'silver_ehr_lab_results', live_only=True) == baseline['silver lab rows (live)']}")

        banner("5", "Correcting the source")
        repair_lab_results(corrupted)
        repaired = True
        print(f"    repaired {len(corrupted)} rows: plausible value, result 45 minutes after the order")

        banner("6", "Replaying the failed stage")
        print(f"    waiting for CDC capture... caught up to {mssql.wait_for_capture()}")
        run_bronze_ingestion(spark, run_fixed, source_name="sql_ehr", source_table="lab_results")
        process_spec(spark, LAB_SPEC, run_fixed)
        build_gold_facts(spark, run_fixed)

        banner("7", "Proof: nothing duplicated, nothing lost")
        after = snapshot(spark)
        print_counts("  counts after recovery", after)
        source_rows = after["source lab_results"]
        live_rows = after["silver lab rows (live)"]
        bronze_rows = after["bronze lab rows (all versions)"]

        checks = {
            "live silver rows match the source exactly (no duplicates)": live_rows == source_rows,
            "silver did not grow by the corrupt batch": live_rows == baseline["silver lab rows (live)"],
            "bronze kept every version, including the corrupt ones": bronze_rows > baseline["bronze lab rows (all versions)"],
            "quarantine kept the evidence": after["quarantine rows"] > baseline["quarantine rows"],
        }
        repaired = (
            spark.read.format("delta").load(f"{SILVER_PATH}/silver_ehr_lab_results")
            .filter(F.col("lab_result_id").isin(corrupted[:50]))
        )
        checks["repaired rows are present exactly once"] = repaired.count() == len(corrupted[:50])
        checks["no corrupt value survived into silver"] = repaired.filter(F.col("result_value") == BAD_VALUE).count() == 0

        print()
        for label, ok in checks.items():
            print(f"    [{'PASS' if ok else 'FAIL'}] {label}")
        if not all(checks.values()):
            raise SystemExit("recovery verification failed")
        print("\n  Recovery verified. Bronze holds the corrupt versions, quarantine holds the rejected rows,\n"
              "  silver holds one correct row per lab result, and no run was lost or double-counted.")
    finally:
        if corrupted and not repaired:
            print("\n  demo aborted: repairing the source so the next run starts clean")
            repair_lab_results(corrupted)

    assert repaired, "the demo must leave the source repaired"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bad-rows", type=int, default=500, help="how many lab results to corrupt")
    run_demo(parser.parse_args().bad_rows)
