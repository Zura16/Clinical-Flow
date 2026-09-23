"""
Incremental silver: read what bronze landed since last time, collapse it, merge it.

Each silver table is a SilverSpec (what to read, how to map columns, what identifies a record).
One engine runs them all, so adding a table is a spec, not another hand-written pipeline.

Per table:
1. Read only bronze partitions landed after this table's stage watermark (`_ingested_at`).
   Bronze's landing time is the cursor, not the source's own timestamps, so a retried run that
   lands old source rows today is still picked up: it lands with a new `_ingested_at`.
2. Collapse the batch to one row per key, keeping the newest version. A batch can contain several
   changes to the same record (CDC insert then update), and only the last one matters.
3. Map to silver columns, hash the business attributes, run the quality rules.
4. MERGE into the silver table:
   - matched and the incoming version is newer  -> update
   - not matched                                -> insert
   - Full snapshots only: not matched by source -> soft-delete, the key is gone from the snapshot
   A version guard means an out-of-order batch can never overwrite newer data with older.
5. Advance the stage watermark, and record the MERGE's own row counts in the audit table.

Deletes are soft: `_is_deleted` / `_deleted_at`. A hard delete would strand facts that reference
the record and destroy the evidence that it ever existed, which is not acceptable for clinical data.
"""

import os
from dataclasses import dataclass, field

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from databricks.utilities.config import BRONZE_PATH, SILVER_PATH, add_record_hash
from databricks.utilities.control import (
    SILVER_STAGE,
    SourceConfig,
    advance_stage_watermark,
    get_source_config,
    get_stage_watermark,
)
from databricks.utilities.logger import PipelineLogger
from databricks.utilities.quality_engine import DataQualityEngine

WATERMARK_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"
CDC_DELETE = 1


@dataclass(frozen=True)
class SilverSpec:
    """One silver table: where it comes from and what it looks like."""

    name: str                       # silver table name
    bronze_table: str               # bronze table it consumes
    key_columns: list[str]          # business key in silver
    columns: dict[str, str]         # silver column -> SQL expression over the bronze/parsed row
    hash_columns: list[str]         # attributes whose change means the record changed
    dq_dataset: str | None = None   # rules to apply, if any
    resource_schema: str | None = None  # FHIR only: parse resource_json with this schema first
    deleted_expr: str | None = None     # extra source-side soft-delete condition
    extra_columns: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Pure transforms
# ---------------------------------------------------------------------------

def version_expr(cfg: SourceConfig):
    """A single comparable string for "how new is this row", per ingestion type.

    Fixed-width in every case, so string comparison orders versions correctly.
    """
    if cfg.ingestion_type == "CDC":
        return F.concat(F.col("_cdc_lsn"), F.col("_cdc_seqval"))
    if cfg.ingestion_type == "Watermark":
        return F.coalesce(
            F.date_format(F.try_to_timestamp(F.col(cfg.watermark_column)), WATERMARK_FORMAT),
            F.date_format(F.col("_ingested_at"), WATERMARK_FORMAT),
        )
    return F.date_format(F.col("_ingested_at"), WATERMARK_FORMAT)


def collapse_batch(df: DataFrame, key_columns: list[str]) -> DataFrame:
    """One row per key: the newest version in this batch."""
    w = Window.partitionBy(*key_columns).orderBy(F.col("_version").desc(), F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def read_increment(spark: SparkSession, bronze_table: str, since: str | None) -> DataFrame:
    df = spark.read.format("delta").load(os.path.join(BRONZE_PATH, bronze_table))
    if since is None:
        return df
    return df.filter(F.col("_ingested_at") > F.to_timestamp(F.lit(since)))


def to_silver_columns(df: DataFrame, spec: SilverSpec, cfg: SourceConfig) -> DataFrame:
    """Map a collapsed bronze batch onto the silver schema."""
    if spec.resource_schema:
        # Parse the raw FHIR JSON, keeping the bronze bookkeeping columns alongside it.
        df = df.select(
            F.from_json("resource_json", spec.resource_schema).alias("r"),
            "_version", "_ingested_at", "_pipeline_run_id",
        ).select("r.*", "_version", "_ingested_at", "_pipeline_run_id")

    # A CDC delete row carries the deleted record's values, so the column mapping still applies.
    deleted = F.lit(False)
    if cfg.ingestion_type == "CDC" and "_cdc_operation" in df.columns:
        deleted = F.col("_cdc_operation") == CDC_DELETE
    if spec.deleted_expr:
        deleted = deleted | F.expr(spec.deleted_expr)

    projected = df.select(
        *[F.expr(expression).alias(name) for name, expression in {**spec.columns, **spec.extra_columns}.items()],
        F.col("_version"),
        F.col("_pipeline_run_id").alias("_source_run_id"),
        deleted.alias("_is_deleted"),
    )
    projected = add_record_hash(projected, spec.hash_columns)

    return (
        projected
        .withColumn("_deleted_at", F.when(F.col("_is_deleted"), F.current_timestamp()))
        .withColumn("_updated_at", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------

def merge_into_silver(spark: SparkSession, batch: DataFrame, spec: SilverSpec, cfg: SourceConfig) -> dict:
    target_path = os.path.join(SILVER_PATH, spec.name)

    if not DeltaTable.isDeltaTable(spark, target_path):
        batch.write.format("delta").save(target_path)
        return {"numTargetRowsInserted": batch.count(), "numTargetRowsUpdated": 0, "numTargetRowsDeleted": 0}

    target = DeltaTable.forPath(spark, target_path)
    condition = " AND ".join(f"t.{k} = s.{k}" for k in spec.key_columns)
    merge = (
        target.alias("t").merge(batch.alias("s"), condition)
        # The version guard: an older version can never overwrite a newer one.
        .whenMatchedUpdateAll(condition="s._version > t._version")
        .whenNotMatchedInsertAll()
    )
    if cfg.ingestion_type == "Full":
        # The batch is a complete snapshot, so a key that is absent from it was removed at source.
        merge = merge.whenNotMatchedBySourceUpdate(
            condition="t._is_deleted = false",
            set={"_is_deleted": F.lit(True), "_deleted_at": F.current_timestamp(), "_updated_at": F.current_timestamp()},
        )
    merge.execute()

    metrics = target.history(1).select("operationMetrics").first()["operationMetrics"]
    return {k: int(metrics.get(k, 0)) for k in ("numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted")}


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def process_spec(spark: SparkSession, spec: SilverSpec, run_id: str) -> int:
    pipeline_name = f"silver:{spec.name}"
    cfg = get_source_config(spark, spec.bronze_table)
    logger = PipelineLogger(spark, run_id, pipeline_name, cfg.source_name, "SILVER")
    since = get_stage_watermark(spark, SILVER_STAGE, spec.name)
    try:
        if not os.path.isdir(os.path.join(BRONZE_PATH, spec.bronze_table, "_delta_log")):
            logger.log_run(status="SUCCESS", error_message=f"no bronze table {spec.bronze_table} yet")
            return 0

        increment = read_increment(spark, spec.bronze_table, since).withColumn("_version", version_expr(cfg)).cache()
        rows_read = increment.count()
        if rows_read == 0:
            logger.log_run(watermark_start=since, watermark_end=since, status="SUCCESS")
            print(f"[SILVER] {spec.name}: nothing new")
            increment.unpersist()
            return 0

        new_watermark = increment.agg(
            F.date_format(F.max("_ingested_at"), WATERMARK_FORMAT).alias("wm")).first()["wm"]

        # Collapse on the source's own key (pipeline_config), not silver's: at this point the
        # rows are still bronze-shaped (a FHIR row is keyed by resource_id, not patient_id).
        batch = to_silver_columns(collapse_batch(increment, cfg.primary_keys), spec, cfg)
        rejected = 0
        if spec.dq_dataset:
            engine = DataQualityEngine(spark, spec.dq_dataset, run_id)
            batch, rejected = engine.validate(batch, id_col=spec.key_columns[0])

        metrics = merge_into_silver(spark, batch, spec, cfg)
        # The watermark moves only once the merge has committed.
        advance_stage_watermark(spark, SILVER_STAGE, spec.name, new_watermark, run_id)

        logger.log_run(
            rows_read=rows_read,
            rows_inserted=metrics["numTargetRowsInserted"],
            rows_updated=metrics["numTargetRowsUpdated"],
            rows_deleted=metrics["numTargetRowsDeleted"],
            rows_rejected=rejected,
            watermark_start=since, watermark_end=new_watermark, status="SUCCESS",
        )
        print(f"[SILVER] {spec.name}: read {rows_read}, inserted {metrics['numTargetRowsInserted']}, "
              f"updated {metrics['numTargetRowsUpdated']}, rejected {rejected}")
        increment.unpersist()
        return rows_read
    except Exception as exc:
        logger.log_run(watermark_start=since, status="FAILED",
                       error_code=type(exc).__name__, error_message=str(exc)[:2000])
        raise


def process_specs(spark: SparkSession, specs: list[SilverSpec], run_id: str) -> int:
    failures, total = [], 0
    for spec in specs:
        try:
            total += process_spec(spark, spec, run_id)
        except Exception as exc:
            failures.append(f"{spec.name}: {type(exc).__name__}: {exc}")
    if failures:
        raise RuntimeError(f"silver run {run_id} failed for {len(failures)} table(s):\n" + "\n".join(failures))
    return total
