"""
Read the current state of a source table out of append-only bronze.

Bronze holds every version ever landed, one partition per run. Silver wants one row per business
key, so this module collapses bronze using the table's pipeline_config row:

- Watermark loads: latest version per primary key (highest watermark, then latest landing).
- CDC loads: latest change per primary key by log position, then drop keys whose last change was
  a delete (__$operation = 1). The delete row stays in bronze as the evidence it happened.
- Full loads: every run is a complete snapshot, so the current state is the newest snapshot.
  A key missing from it was removed at the source; taking the latest-per-key across all
  snapshots would keep it alive forever.
"""

import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from databricks.utilities.config import BRONZE_PATH
from databricks.utilities.control import SourceConfig, get_source_config


def latest_per_key(df: DataFrame, key_columns: list[str], watermark_column: str) -> DataFrame:
    w = Window.partitionBy(*key_columns).orderBy(
        F.try_to_timestamp(F.col(watermark_column)).desc_nulls_last(),
        F.col("_ingested_at").desc(),
    )
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def latest_snapshot(df: DataFrame) -> DataFrame:
    newest = df.orderBy(F.col("_ingested_at").desc()).select("_pipeline_run_id").first()
    if newest is None:
        return df
    return df.filter(F.col("_pipeline_run_id") == newest["_pipeline_run_id"])


CDC_DELETE = 1


def latest_cdc_state(df: DataFrame, key_columns: list[str]) -> DataFrame:
    w = Window.partitionBy(*key_columns).orderBy(
        F.col("_cdc_lsn").desc(), F.col("_cdc_seqval").desc(), F.col("_ingested_at").desc()
    )
    latest = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    return latest.filter(F.col("_cdc_operation") != CDC_DELETE)


def current_state(df: DataFrame, cfg: SourceConfig) -> DataFrame:
    if cfg.ingestion_type == "Full":
        return latest_snapshot(df)
    if cfg.ingestion_type == "CDC":
        return latest_cdc_state(df, cfg.primary_keys)
    return latest_per_key(df, cfg.primary_keys, cfg.watermark_column)


def bronze_exists(destination_table: str) -> bool:
    return os.path.isdir(os.path.join(BRONZE_PATH, destination_table, "_delta_log"))


def read_bronze_current(spark: SparkSession, destination_table: str) -> DataFrame:
    cfg = get_source_config(spark, destination_table)
    df = spark.read.format("delta").load(os.path.join(BRONZE_PATH, destination_table))
    return current_state(df, cfg)
