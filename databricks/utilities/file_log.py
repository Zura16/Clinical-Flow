"""
Which source files each FileIncremental bronze table has already ingested.

A file counts as ingested once a run that read it has a SUCCESS audit row for the table: the audit
row is the commit marker, so there is no separate "mark committed" write that a crash could skip.
Entries from a run that never succeeded are ignored, and a retry reads those files again.

A file is identified by (path, size, modification time). Rewriting a file in place changes one of
them, so the file is read again; a same-size rewrite that also preserves the mtime is not detected.
Without hashing every file on every run, that is the limit of any listing-based discovery.

Grain: one row per (destination_table, file_path, file_size, file_modification_time, pipeline_run_id).
"""

import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from databricks.utilities.config import META_PATH
from databricks.utilities.logger import AUDIT_TABLE_PATH

FILE_INGEST_LOG_PATH = os.path.join(META_PATH, "file_ingest_log")

FILE_INGEST_LOG_SCHEMA = StructType(
    [
        StructField("destination_table", StringType(), False),
        StructField("file_path", StringType(), False),
        StructField("file_size", LongType(), False),
        StructField("file_modification_time", TimestampType(), False),
        StructField("pipeline_run_id", StringType(), False),
        StructField("logged_at", TimestampType(), False),
    ]
)

FILE_IDENTITY = ["file_path", "file_size", "file_modification_time"]


def list_source_files(spark: SparkSession, location: str) -> DataFrame:
    """Every file matching location, with the size and mtime that identify its version.

    binaryFile with only these columns selected lists files without reading their contents, and
    goes through the same filesystem layer as the readers, so it works on ADLS as it does locally.
    """
    return (
        spark.read.format("binaryFile")
        .load(location)
        .select(
            F.col("path").alias("file_path"),
            F.col("length").alias("file_size"),
            F.col("modificationTime").alias("file_modification_time"),
        )
    )


def committed_entries(spark: SparkSession, destination_table: str) -> DataFrame:
    """Log entries of this table whose run succeeded: the only entries that count."""
    if not DeltaTable.isDeltaTable(spark, FILE_INGEST_LOG_PATH) or not DeltaTable.isDeltaTable(spark, AUDIT_TABLE_PATH):
        return spark.createDataFrame([], FILE_INGEST_LOG_SCHEMA)
    succeeded = (
        spark.read.format("delta")
        .load(AUDIT_TABLE_PATH)
        .filter((F.col("pipeline_name") == f"bronze:{destination_table}") & (F.col("execution_status") == "SUCCESS"))
        .select("pipeline_run_id")
        .distinct()
    )
    return (
        spark.read.format("delta")
        .load(FILE_INGEST_LOG_PATH)
        .filter(F.col("destination_table") == destination_table)
        .join(succeeded, "pipeline_run_id")
    )


def committed_files(spark: SparkSession, destination_table: str) -> DataFrame:
    """Files this table has ingested in any run that succeeded (the FileIncremental rule)."""
    return committed_entries(spark, destination_table).select(*FILE_IDENTITY).distinct()


def latest_committed_files(spark: SparkSession, destination_table: str) -> tuple[str | None, set[tuple]]:
    """(run ID, file identities) of the newest successful run that logged files for this table.

    The Full rule. A snapshot source has one current state, so "unchanged" means "the same as what
    was last landed", not "the same as anything ever landed": a file restored from a backup with
    its old size and mtime matches an older entry, and must still land.
    """
    entries = committed_entries(spark, destination_table)
    newest = entries.orderBy(F.col("logged_at").desc()).select("pipeline_run_id").first()
    if newest is None:
        return None, set()
    run_id = newest["pipeline_run_id"]
    files = entries.filter(F.col("pipeline_run_id") == run_id).select(*FILE_IDENTITY).collect()
    return run_id, {tuple(f) for f in files}


def files_to_ingest(spark: SparkSession, location: str, destination_table: str) -> tuple[list[Row], int]:
    """(files not yet ingested by this table, total files present)."""
    present = list_source_files(spark, location).cache()
    total = present.count()
    pending = present.join(committed_files(spark, destination_table), FILE_IDENTITY, "left_anti")
    files = pending.orderBy("file_path").collect()
    present.unpersist()
    return files, total


def record_files(spark: SparkSession, destination_table: str, run_id: str, files: list[Row]) -> None:
    """Log the files a run read. They count only once the run's SUCCESS audit row exists."""
    rows = spark.createDataFrame(
        [(destination_table, f["file_path"], f["file_size"], f["file_modification_time"], run_id) for f in files],
        "destination_table STRING, file_path STRING, file_size BIGINT, file_modification_time TIMESTAMP, "
        "pipeline_run_id STRING",
    ).withColumn("logged_at", F.current_timestamp())

    if not DeltaTable.isDeltaTable(spark, FILE_INGEST_LOG_PATH):
        spark.createDataFrame([], FILE_INGEST_LOG_SCHEMA).write.format("delta").save(FILE_INGEST_LOG_PATH)
    # Insert-only on the full grain, so a retry of the same run does not duplicate its entries.
    grain = ["destination_table", *FILE_IDENTITY, "pipeline_run_id"]
    (
        DeltaTable.forPath(spark, FILE_INGEST_LOG_PATH)
        .alias("t")
        .merge(rows.alias("s"), " AND ".join(f"t.{c} = s.{c}" for c in grain))
        .whenNotMatchedInsertAll()
        .execute()
    )
