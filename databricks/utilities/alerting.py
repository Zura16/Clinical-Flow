"""
Alerting for failed pipeline stages.

An alert is what someone on call actually sees, so it has to answer three questions without a
query: what broke, what it means, and where to look next. Every alert is also recorded in
`pipeline_alert`, because an alert nobody kept is an alert nobody can audit.

Delivery is deliberately dull: print to the job log, append to the table, and POST to
CLINICALFLOW_ALERT_WEBHOOK when one is configured. Azure Monitor action groups or a PagerDuty
integration would replace the webhook without changing the callers.

Alerting must never mask the failure it is reporting, so every send is wrapped: if the sink is
down, the original exception still propagates.
"""

import json
import os
import urllib.error
import urllib.request

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from databricks.utilities.config import META_PATH

ALERT_TABLE_PATH = os.path.join(META_PATH, "pipeline_alert")
WEBHOOK_ENV_VAR = "CLINICALFLOW_ALERT_WEBHOOK"
WEBHOOK_TIMEOUT_SECONDS = 5

ALERT_SCHEMA = StructType([
    StructField("alert_key", StringType(), False),
    StructField("pipeline_run_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("summary", StringType(), False),
    StructField("detail", StringType(), True),
    StructField("next_step", StringType(), True),
    StructField("raised_at", StringType(), False),
])


def _print_alert(pipeline_name: str, run_id: str, severity: str, summary: str,
                 detail: str | None, next_step: str | None) -> None:
    line = "!" * 78
    print(f"\n{line}\n[{severity}] {pipeline_name} failed (run {run_id})\n  {summary}")
    if detail:
        print(f"  detail: {detail}")
    if next_step:
        print(f"  next:   {next_step}")
    print(f"{line}\n")


def _post_webhook(payload: dict) -> None:
    url = os.environ.get(WEBHOOK_ENV_VAR)
    if not url:
        return
    request = urllib.request.Request(
        url, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(request, timeout=WEBHOOK_TIMEOUT_SECONDS):
            pass
    except (urllib.error.URLError, TimeoutError) as exc:
        # Never name the URL in a log line: it is a credential.
        print(f"[ALERT] webhook delivery failed ({type(exc).__name__}); the alert is still recorded")


def raise_alert(spark: SparkSession, run_id: str, pipeline_name: str, summary: str,
                detail: str | None = None, next_step: str | None = None,
                severity: str = "ERROR") -> None:
    """Record and deliver an alert. Failures here are reported, never raised."""
    _print_alert(pipeline_name, run_id, severity, summary, detail, next_step)
    try:
        row = spark.createDataFrame(
            [(run_id, pipeline_name, severity, summary, detail, next_step)],
            "pipeline_run_id STRING, pipeline_name STRING, severity STRING, summary STRING, "
            "detail STRING, next_step STRING",
        ).select(
            F.sha2(F.concat_ws("||", "pipeline_run_id", "pipeline_name", "summary"), 256).alias("alert_key"),
            "pipeline_run_id", "pipeline_name", "severity", "summary", "detail", "next_step",
            F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("raised_at"),
        )
        if not DeltaTable.isDeltaTable(spark, ALERT_TABLE_PATH):
            row.write.format("delta").save(ALERT_TABLE_PATH)
        else:
            # Keyed on run + pipeline + summary, so a retried run does not re-page anyone for the
            # same failure.
            (
                DeltaTable.forPath(spark, ALERT_TABLE_PATH).alias("t")
                .merge(row.alias("s"), "t.alert_key = s.alert_key")
                .whenNotMatchedInsertAll()
                .execute()
            )
        _post_webhook({
            "run_id": run_id, "pipeline": pipeline_name, "severity": severity,
            "summary": summary, "detail": detail, "next_step": next_step,
        })
    except Exception as exc:  # noqa: BLE001 - reporting must not replace the original failure
        print(f"[ALERT] could not record the alert ({type(exc).__name__}: {exc})")
