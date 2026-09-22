"""
ClinicalFlow Metadata-Driven Bronze Ingestion Engine

For every active row in pipeline_config: read the source, keep only rows past the table's
watermark (Watermark loads) or the whole source (Full loads), and append them to an immutable
bronze Delta table partitioned by _ingest_date / _pipeline_run_id.

Idempotency and restarts, per table:
- A run ID that already succeeded is skipped (audit status SKIPPED). Its bronze partition is the
  only copy of the source rows as they were then; the source may have changed since, so
  re-extracting would replace history with a different set of rows.
- A run ID that failed is retried as a normal incremental run from the current watermark. Failed
  runs never advance the watermark, so the retry picks up exactly what the failed attempt missed.
- Writes use replaceWhere on the run's own _pipeline_run_id, so a retry after a crash between the
  bronze commit and the audit row replaces that partial partition instead of duplicating it.
- Commit order is bronze write -> SUCCESS audit row -> watermark advance. If the process dies
  before the watermark moves, the next attempt at that run ID sees SUCCESS and repairs the watermark.

Reprocessing downstream ("replay the failed partition") reads from bronze, never from the source.

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

from databricks.utilities import sqlserver as mssql
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
    "claims_csv": read_csv_source,
    "fhir_r4": read_fhir_source,
}

# CDC operation codes as SQL Server reports them, plus 0 for rows we read from the table itself.
CDC_SNAPSHOT, CDC_DELETE, CDC_INSERT, CDC_UPDATE_AFTER = 0, 1, 2, 4


def read_cdc_increment(spark: SparkSession, cfg: SourceConfig, wm_start: str | None) -> tuple[DataFrame, str]:
    """Changes from SQL Server CDC since wm_start, with the LSN they are read up to.

    First load has no position to resume from, so it reads the table itself as of the current
    max LSN and marks the rows CDC_SNAPSHOT. CDC retention (3 days by default) is a change buffer,
    not a history store, so the snapshot is what establishes the baseline.

    Any row written between reading max LSN and reading the table appears both in the snapshot and
    in the next increment. The later copy has the higher LSN, so current-state resolution picks it.
    """
    table = cfg.source_table
    to_lsn = mssql.max_lsn()
    if to_lsn is None:
        raise ValueError("SQL Server returned no max LSN: is the SQL Agent running and CDC enabled?")
    columns = ", ".join(f"[{c}]" for c in mssql.business_columns(table))

    if wm_start is None:
        sql = (f"SELECT '{to_lsn}' AS _cdc_lsn, '{mssql.ZERO_LSN}' AS _cdc_seqval, "
               f"{CDC_SNAPSHOT} AS _cdc_operation, {columns} FROM dbo.{table}")
        return mssql.read_query(spark, sql), to_lsn

    from_lsn = mssql.increment_lsn(wm_start)
    if from_lsn > to_lsn:
        # Nothing has been written to the log since the last run.
        empty = (f"SELECT '{wm_start}' AS _cdc_lsn, '{mssql.ZERO_LSN}' AS _cdc_seqval, "
                 f"{CDC_SNAPSHOT} AS _cdc_operation, {columns} FROM dbo.{table} WHERE 1 = 0")
        return mssql.read_query(spark, empty), wm_start

    min_lsn = mssql.min_lsn(table)
    if min_lsn and from_lsn < min_lsn:
        raise ValueError(
            f"CDC retention gap on dbo.{table}: need changes from {from_lsn}, but the capture only "
            f"retains from {min_lsn}. Changes were cleaned up before they were ingested; the table "
            "must be re-snapshotted (clear its watermark_state row) rather than silently skipped."
        )

    sql = (
        "SELECT CONVERT(CHAR(20), __$start_lsn, 2) AS _cdc_lsn, "
        "CONVERT(CHAR(20), __$seqval, 2) AS _cdc_seqval, "
        f"__$operation AS _cdc_operation, {columns} "
        f"FROM cdc.fn_cdc_get_all_changes_{mssql.capture_instance(table)}("
        f"CONVERT(BINARY(10), '{from_lsn}', 2), CONVERT(BINARY(10), '{to_lsn}', 2), 'all')"
    )
    return mssql.read_query(spark, sql), to_lsn


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


def select_increment(df: DataFrame, watermark_column: str, start: str | None) -> DataFrame:
    """Rows strictly past the stored watermark; everything on a first load (start is None)."""
    if start is None:
        return df
    return df.filter(watermark_ts(watermark_column) > F.to_timestamp(F.lit(start)))


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
# Restart support
# ---------------------------------------------------------------------------

def run_partition_exists(spark: SparkSession, target: str, run_id: str) -> bool:
    """Whether this run already wrote a partition, by listing directories.

    A Spark query to answer this costs ~1.7s; the partition layout makes it a directory check.
    """
    if not os.path.isdir(target):
        return False
    marker = f"_pipeline_run_id={run_id}"
    return any(
        os.path.isdir(os.path.join(target, date_dir, marker))
        for date_dir in os.listdir(target)
        if date_dir.startswith("_ingest_date=")
    )


def find_successful_attempt(spark: SparkSession, run_id: str, pipeline_name: str):
    """The audit row of this run ID's successful attempt at this table, or None."""
    if not DeltaTable.isDeltaTable(spark, AUDIT_TABLE_PATH):
        return None
    return (
        spark.read.format("delta").load(AUDIT_TABLE_PATH)
        .filter(
            (F.col("pipeline_run_id") == run_id)
            & (F.col("pipeline_name") == pipeline_name)
            & (F.col("execution_status") == "SUCCESS")
        )
        .orderBy(F.col("end_timestamp").desc())
        .first()
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def ingest_table(spark: SparkSession, cfg: SourceConfig, run_id: str) -> int:
    pipeline_name = f"bronze:{cfg.destination_table}"
    logger = PipelineLogger(spark, run_id, pipeline_name, cfg.source_name, "BRONZE")
    target = os.path.join(BRONZE_PATH, cfg.destination_table)
    wm_start = None
    try:
        previous = find_successful_attempt(spark, run_id, pipeline_name)
        if previous is not None:
            if previous["watermark_end"] is not None:
                advance_watermark(spark, cfg, previous["watermark_end"], run_id)  # no-op unless a crash skipped it
            logger.log_run(watermark_start=previous["watermark_start"], watermark_end=previous["watermark_end"],
                           status="SKIPPED", error_message="run ID already landed this table; partition left untouched")
            print(f"[BRONZE] {cfg.destination_table}: run {run_id} already landed {previous['rows_inserted']} rows; skipped")
            return previous["rows_inserted"]

        # CDC builds its own query; other sources read a file.
        source_df = read_source(spark, cfg) if cfg.ingestion_type != "CDC" else None

        if cfg.ingestion_type == "Full":
            increment, wm_end = source_df, None
        elif cfg.ingestion_type == "CDC":
            wm_start = get_watermark(spark, cfg)
            increment, wm_end = read_cdc_increment(spark, cfg, wm_start)
        else:
            bad = count_bad_watermarks(source_df, cfg.watermark_column)
            if bad:
                raise ValueError(f"{bad} row(s) in {cfg.source_location} have a missing or unparseable {cfg.watermark_column}")
            wm_start = get_watermark(spark, cfg)
            increment = select_increment(source_df, cfg.watermark_column, wm_start)

        landed = add_bronze_metadata(increment, run_id, cfg).cache()
        rows = landed.count()
        if cfg.ingestion_type == "Watermark":
            wm_end = max_watermark(landed, cfg.watermark_column) if rows else wm_start

        # An empty increment with nothing already written for this run has nothing to commit.
        # A Delta commit costs seconds, and on an idle run that is most of the run's cost.
        # The exception is a retry whose earlier attempt did write: that partition must be replaced.
        if rows or run_partition_exists(spark, target, run_id):
            (
                landed.write.format("delta")
                .mode("overwrite")
                .option("replaceWhere", f"_pipeline_run_id = '{run_id}'")
                .partitionBy("_ingest_date", "_pipeline_run_id")
                .save(target)
            )
        landed.unpersist()

        logger.log_run(rows_read=rows, rows_inserted=rows, watermark_start=wm_start, watermark_end=wm_end, status="SUCCESS")
        # The watermark moves last: only once the rows are committed and the run is recorded.
        if wm_end is not None:
            advance_watermark(spark, cfg, wm_end, run_id)
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
    parser.add_argument("--run-id", help="reuse a run ID to restart it: tables it already landed are skipped, failed ones retried")
    parser.add_argument("--source", help="limit to one source_name")
    parser.add_argument("--table", help="limit to one source_table")
    args = parser.parse_args()
    run_bronze_ingestion(run_id=args.run_id, source_name=args.source, source_table=args.table)
