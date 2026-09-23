"""
ClinicalFlow data quality engine.

Runs the rules in `data_quality_rule` against a silver batch and returns the rows that passed.

What it guarantees:
- Failing rows are quarantined with their payload and the rule that rejected them, never dropped.
- Quarantine writes are idempotent: rerunning the same run and batch does not duplicate rows.
- Every rule's outcome is recorded in `data_quality_result`, including the ones that passed, so
  "no violations" is a measurement rather than an absence of evidence.
- A rule whose failure rate exceeds its threshold raises DataQualityThresholdError, which fails
  the table's run rather than loading data nobody has judged.

Severity decides the fate of a failing row: CRITICAL and ERROR keep it out of silver, WARNING
records it and lets it through.
"""

import os
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from databricks.utilities.config import META_PATH, SILVER_PATH
from databricks.utilities.quality_rules import (
    QUARANTINING_SEVERITIES,
    ROW_RULE_TYPES,
    QualityRule,
    load_rules,
)

QUARANTINE_TABLE_PATH = os.path.join(META_PATH, "quarantine_records")
DATA_QUALITY_RESULT_PATH = os.path.join(META_PATH, "data_quality_result")


class DataQualityThresholdError(Exception):
    """A rule failed for a larger share of the batch than its threshold allows."""


def _violations(spark: SparkSession, df: DataFrame, rule: QualityRule) -> DataFrame | None:
    """The rows of df that break this rule. None means the rule is not row-scoped."""
    if rule.rule_type in ROW_RULE_TYPES:
        # A rule holds only when its expression is true: NULL is not a pass.
        return df.filter(~F.coalesce(F.expr(rule.rule_expression), F.lit(False)))

    if rule.rule_type == "UNIQUE":
        keys = [c.strip() for c in rule.rule_expression.split(",")]
        ordering = F.col("_version").desc() if "_version" in df.columns else F.lit(1)
        numbered = df.withColumn("_dupe_rank", F.row_number().over(Window.partitionBy(*keys).orderBy(ordering)))
        # The first row of each key is the survivor; the rest are the duplicates.
        return numbered.filter("_dupe_rank > 1").drop("_dupe_rank")

    if rule.rule_type == "REFERENTIAL":
        table, column = rule.rule_expression.rsplit(".", 1)
        reference_path = os.path.join(SILVER_PATH, table)
        if not os.path.isdir(os.path.join(reference_path, "_delta_log")):
            return df.limit(0)  # reference not built yet: nothing to check against
        reference = spark.read.format("delta").load(reference_path)
        if "_is_deleted" in reference.columns:
            reference = reference.filter(~F.col("_is_deleted"))
        reference = reference.select(F.col(column).alias("_ref_key")).distinct()
        return (
            df.join(reference, df[rule.column_name] == F.col("_ref_key"), "left_anti")
            .filter(F.col(rule.column_name).isNotNull())
        )

    return None  # FRESHNESS is dataset-scoped and handled separately


def _freshness_failed(df: DataFrame, rule: QualityRule) -> bool:
    max_age_hours = float(rule.rule_expression)
    newest = df.agg(F.max(F.col(rule.column_name)).alias("newest")).first()["newest"]
    if newest is None:
        return True
    age_hours = (datetime.utcnow() - newest.replace(tzinfo=None)).total_seconds() / 3600
    return age_hours > max_age_hours


def _quarantine(spark: SparkSession, invalid: DataFrame, rule: QualityRule, run_id: str, id_col: str) -> None:
    """Append violations to the quarantine table, keyed so a rerun cannot duplicate them."""
    rows = (
        invalid
        .withColumn("pipeline_run_id", F.lit(run_id))
        .withColumn("source_name", F.lit(rule.dataset_name))
        .withColumn("record_identifier", F.coalesce(F.col(id_col).cast("string"), F.lit("UNKNOWN")))
        .withColumn("failed_rule", F.lit(rule.name))
        .withColumn("error_message", F.lit(f"{rule.rule_type} failed: {rule.rule_expression} (severity {rule.severity})"))
        .withColumn("raw_payload", F.to_json(F.struct("*")))
        .withColumn("detected_timestamp", F.current_timestamp())
        .withColumn("resolution_status", F.lit("PENDING"))
        .withColumn("quarantine_key",
                    F.sha2(F.concat_ws("||", "pipeline_run_id", "source_name", "record_identifier", "failed_rule"), 256))
        .select("quarantine_key", "pipeline_run_id", "source_name", "record_identifier", "failed_rule",
                "error_message", "raw_payload", "detected_timestamp", "resolution_status")
    )
    if not DeltaTable.isDeltaTable(spark, QUARANTINE_TABLE_PATH):
        rows.write.format("delta").save(QUARANTINE_TABLE_PATH)
        return
    (
        DeltaTable.forPath(spark, QUARANTINE_TABLE_PATH).alias("t")
        .merge(rows.alias("s"), "t.quarantine_key = s.quarantine_key")
        .whenNotMatchedInsertAll()
        .execute()
    )


def _record_results(spark: SparkSession, results: list[dict]) -> None:
    if not results:
        return
    df = (
        spark.createDataFrame(results)
        .withColumn("checked_at", F.current_timestamp())
        .withColumn("result_key", F.sha2(F.concat_ws("||", "pipeline_run_id", "dataset_name", "rule_name"), 256))
    )
    if not DeltaTable.isDeltaTable(spark, DATA_QUALITY_RESULT_PATH):
        df.write.format("delta").save(DATA_QUALITY_RESULT_PATH)
        return
    (
        DeltaTable.forPath(spark, DATA_QUALITY_RESULT_PATH).alias("t")
        .merge(df.alias("s"), "t.result_key = s.result_key")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


class DataQualityEngine:
    def __init__(self, spark: SparkSession, dataset_name: str, pipeline_run_id: str):
        self.spark = spark
        self.dataset_name = dataset_name
        self.pipeline_run_id = pipeline_run_id
        self.rules = load_rules(spark, dataset_name)

    def validate(self, df: DataFrame, id_col: str = "id") -> tuple[DataFrame, int, list[dict]]:
        """Returns (rows that may continue, rows quarantined, one result row per rule)."""
        if not self.rules:
            return df, 0, []

        # Soft-deleted rows are excluded: a record being removed does not have to satisfy
        # constraints, and its parent may legitimately be gone already.
        live = df.filter(~F.col("_is_deleted")) if "_is_deleted" in df.columns else df
        deleted = df.filter(F.col("_is_deleted")) if "_is_deleted" in df.columns else None

        total = live.count()
        if total == 0:
            return df, 0, []

        valid = live
        quarantined_total = 0
        results: list[dict] = []
        breaches: list[str] = []

        for rule in self.rules:
            if rule.rule_type == "FRESHNESS":
                failed_count = total if _freshness_failed(live, rule) else 0
            else:
                # Each rule is evaluated against the original batch, so one bad row can fail several
                # rules and be visible in quarantine under each. Only removal is cumulative.
                invalid = _violations(self.spark, live, rule)
                failed_count = invalid.count()
                if failed_count:
                    _quarantine(self.spark, invalid, rule, self.pipeline_run_id, id_col)
                    if rule.severity in QUARANTINING_SEVERITIES:
                        if rule.rule_type == "UNIQUE":
                            # Only the surplus copies are rejected; the key keeps one row.
                            keys = [c.strip() for c in rule.rule_expression.split(",")]
                            ordering = F.col("_version").desc() if "_version" in valid.columns else F.lit(1)
                            valid = (
                                valid.withColumn("_dupe_rank", F.row_number().over(Window.partitionBy(*keys).orderBy(ordering)))
                                .filter("_dupe_rank = 1").drop("_dupe_rank")
                            )
                        else:
                            valid = valid.join(invalid.select(id_col).distinct(), on=id_col, how="left_anti")
                        quarantined_total += failed_count

            rate = (failed_count / total) * 100
            passed = rate <= rule.failure_threshold
            results.append({
                "pipeline_run_id": self.pipeline_run_id,
                "dataset_name": self.dataset_name,
                "rule_name": rule.name,
                "rule_type": rule.rule_type,
                "severity": rule.severity,
                "rows_checked": int(total),
                "rows_failed": int(failed_count),
                "failure_rate_pct": float(round(rate, 4)),
                "failure_threshold_pct": float(rule.failure_threshold),
                "passed": bool(passed),
            })
            if failed_count:
                print(f"[DQ] {self.dataset_name} {rule.name}: {failed_count}/{total} failed "
                      f"({rate:.2f}% vs {rule.failure_threshold}% allowed){'' if passed else ' -> THRESHOLD BREACH'}")
            if not passed:
                breaches.append(f"{rule.name} {rate:.2f}% > {rule.failure_threshold}% ({failed_count}/{total} rows)")

        _record_results(self.spark, results)

        if breaches:
            raise DataQualityThresholdError(
                f"{self.dataset_name}: {len(breaches)} rule(s) over threshold: " + "; ".join(breaches)
            )
        # Soft deletes rejoin the batch: they still have to reach silver to mark the record gone.
        if deleted is not None:
            valid = valid.unionByName(deleted)
        return valid, quarantined_total, results
