"""
ClinicalFlow Metadata-Driven Bronze Ingestion Engine

For every active row in pipeline_config: read the source, keep only rows past the table's
watermark (Watermark loads) or the whole source (Full loads), and append them to an immutable
bronze Delta table partitioned by _ingest_date / _pipeline_run_id.

Idempotency: a run writes with replaceWhere on its own _pipeline_run_id, so rerunning the same
run ID replaces exactly what that run wrote and nothing else. A replay reuses the watermark window
the original run recorded in pipeline_run_audit, so it lands the same rows even after later runs
have moved the watermark on.

Usage:
    python -m databricks.bronze.ingest_raw_data [--run-id ID] [--source NAME] [--table NAME]
"""

import argparse
import os
import re
import uuid

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from databricks.utilities.config import BASE_DIR, BRONZE_PATH, get_spark_session
from databricks.utilities.control import SourceConfig, advance_watermark, get_watermark, load_source_configs
from databricks.utilities.logger import AUDIT_TABLE_PATH, PipelineLogger

RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
WATERMARK_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"

# Reading the bundle's entries as STRING keeps each resource as its original JSON text:
# bronze stores the source record untouched and never infers a schema.
FHIR_BUNDLE_SCHEMA = "resourceType STRING, entry ARRAY<STRUCT<resource: STRING>>"


# ---------------------------------------------------------------------------
# Source readers (I/O)
# ---------------------------------------------------------------------------

def read_csv_source(spark: SparkSession, location: str, cfg: SourceConfig) -> DataFrame:
    # No inferSchema: every column lands as STRING exactly as the source wrote it.
    return (
        spark.read.option("header", "true").csv(location)
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


def read_fhir_source(spark: SparkSession, location: str, cfg: SourceConfig) -> DataFrame:
    bundles = (
        spark.read.text(location, wholetext=True)
        .select(F.from_json("value", FHIR_BUNDLE_SCHEMA).alias("bundle"), F.col("_metadata.file_path").alias("_source_file"))
    )
    unparseable = bundles.filter(F.col("bundle").isNull() | F.col("bundle.entry").isNull()).count()
    if unparseable:
        raise ValueError(f"{unparseable} FHIR bundle file(s) under {location} could not be parsed as a Bundle")

    return (
        bundles
        .select(F.explode("bundle.entry.resource").alias("resource_json"), "_source_file")
        .select(
            F.get_json_object("resource_json", "$.resourceType").alias("resource_type"),
            F.get_json_object("resource_json", "$.id").alias("resource_id"),
            F.get_json_object("resource_json", "$.meta.lastUpdated").alias("meta_lastUpdated"),
            "resource_json",
            "_source_file",
        )
        .filter(F.col("resource_type") == cfg.source_table)
    )


SOURCE_READERS = {
    "sql_ehr": read_csv_source,  # CSV extracts stand in for SQL Server until fix-plan step 2
    "claims_csv": read_csv_source,
    "fhir_r4": read_fhir_source,
}


def read_source(spark: SparkSession, cfg: SourceConfig) -> DataFrame:
    reader = SOURCE_READERS.get(cfg.source_name)
    if reader is None:
        raise ValueError(f"no reader registered for source_name {cfg.source_name!r}")
    return reader(spark, os.path.join(BASE_DIR, cfg.source_location), cfg)


# ---------------------------------------------------------------------------
# Pure transforms
# ---------------------------------------------------------------------------

def watermark_ts(watermark_column: str):
    return F.try_to_timestamp(F.col(watermark_column))


def select_increment(df: DataFrame, watermark_column: str, start: str | None, end: str | None) -> DataFrame:
    """Rows with start < watermark <= end. A None bound is open."""
    ts = watermark_ts(watermark_column)
    condition = F.lit(True)
    if start is not None:
        condition = condition & (ts > F.to_timestamp(F.lit(start)))
    if end is not None:
        condition = condition & (ts <= F.to_timestamp(F.lit(end)))
    return df.filter(condition)


def count_bad_watermarks(df: DataFrame, watermark_column: str) -> int:
    """Rows whose watermark is missing or unparseable. They can never be selected by a
    watermark filter, so ingesting around them would drop them silently."""
    return df.filter(watermark_ts(watermark_column).isNull()).count()


def max_watermark(df: DataFrame, watermark_column: str) -> str | None:
    return df.agg(F.date_format(F.max(watermark_ts(watermark_column)), WATERMARK_FORMAT).alias("wm")).first()["wm"]


def add_bronze_metadata(df: DataFrame, run_id: str, cfg: SourceConfig) -> DataFrame:
    return (
        df
        .withColumn("_pipeline_run_id", F.lit(run_id))
        .withColumn("_source_name", F.lit(cfg.source_name))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingest_date", F.current_date())
    )


# ---------------------------------------------------------------------------
# Replay support
# ---------------------------------------------------------------------------

def find_replay_window(spark: SparkSession, run_id: str, pipeline_name: str) -> tuple[str | None, str | None] | None:
    """If this run ID has already run this table, return the (start, end) watermark window it
    used; otherwise None. The original attempt's start is authoritative; end comes from the
    latest successful attempt, or is open if every attempt failed."""
    if not DeltaTable.isDeltaTable(spark, AUDIT_TABLE_PATH):
        return None
    attempts = (
        spark.read.format("delta").load(AUDIT_TABLE_PATH)
        .filter((F.col("pipeline_run_id") == run_id) & (F.col("pipeline_name") == pipeline_name))
        .orderBy("start_timestamp")
        .collect()
    )
    if not attempts:
        return None
    start = attempts[0]["watermark_start"]
    successful_ends = [a["watermark_end"] for a in attempts if a["execution_status"] == "SUCCESS" and a["watermark_end"]]
    return start, (max(successful_ends) if successful_ends else None)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def ingest_table(spark: SparkSession, cfg: SourceConfig, run_id: str) -> int:
    pipeline_name = f"bronze:{cfg.destination_table}"
    logger = PipelineLogger(spark, run_id, pipeline_name, cfg.source_name, "BRONZE")
    target = os.path.join(BRONZE_PATH, cfg.destination_table)
    wm_start = None
    try:
        source_df = read_source(spark, cfg)

        if cfg.ingestion_type == "Full":
            increment, wm_end = source_df, None
        else:
            bad = count_bad_watermarks(source_df, cfg.watermark_column)
            if bad:
                raise ValueError(f"{bad} row(s) in {cfg.source_location} have a missing or unparseable {cfg.watermark_column}")
            replay = find_replay_window(spark, run_id, pipeline_name)
            if replay is not None:
                wm_start, wm_bound = replay
            else:
                wm_start, wm_bound = get_watermark(spark, cfg), None
            increment = select_increment(source_df, cfg.watermark_column, wm_start, wm_bound)

        landed = add_bronze_metadata(increment, run_id, cfg).cache()
        rows = landed.count()
        if cfg.ingestion_type != "Full":
            # An empty increment leaves the window where it was, so a replay of it is also empty.
            wm_end = max_watermark(landed, cfg.watermark_column) if rows else wm_start

        (
            landed.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"_pipeline_run_id = '{run_id}'")
            .partitionBy("_ingest_date", "_pipeline_run_id")
            .save(target)
        )
        landed.unpersist()

        # Only after the bronze commit succeeds does the watermark move.
        if wm_end is not None:
            advance_watermark(spark, cfg, wm_end, run_id)

        logger.log_run(rows_read=rows, rows_inserted=rows, watermark_start=wm_start, watermark_end=wm_end, status="SUCCESS")
        print(f"[BRONZE] {cfg.destination_table}: landed {rows} rows ({cfg.ingestion_type}, window {wm_start} -> {wm_end})")
        return rows
    except Exception as exc:
        logger.log_run(watermark_start=wm_start, status="FAILED", error_code=type(exc).__name__, error_message=str(exc)[:2000])
        raise


def run_bronze_ingestion(spark: SparkSession | None = None, run_id: str | None = None,
                         source_name: str | None = None, source_table: str | None = None) -> str:
    spark = spark or get_spark_session("ClinicalFlow_Bronze_Ingestion")
    run_id = run_id or f"run-{uuid.uuid4().hex[:10]}"
    if not RUN_ID_PATTERN.match(run_id):
        raise ValueError(f"run_id must match {RUN_ID_PATTERN.pattern}: {run_id!r}")

    print(f"STARTING BRONZE INGESTION (run {run_id})")
    configs = load_source_configs(spark, source_name, source_table)
    if not configs:
        raise ValueError(f"no active pipeline_config rows match source={source_name!r} table={source_table!r}")

    # One table failing doesn't stop the others; the run still fails at the end.
    failures = []
    for cfg in configs:
        try:
            ingest_table(spark, cfg, run_id)
        except Exception as exc:
            failures.append(f"{cfg.destination_table}: {type(exc).__name__}: {exc}")
    if failures:
        raise RuntimeError(f"bronze run {run_id} failed for {len(failures)} table(s):\n" + "\n".join(failures))

    print(f"BRONZE INGESTION COMPLETE (run {run_id})")
    return run_id


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id", help="reuse an existing run ID to replay exactly what it ingested")
    parser.add_argument("--source", help="limit to one source_name")
    parser.add_argument("--table", help="limit to one source_table")
    args = parser.parse_args()
    run_bronze_ingestion(run_id=args.run_id, source_name=args.source, source_table=args.table)
