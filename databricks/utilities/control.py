"""
ClinicalFlow ingestion control plane.

pipeline_config says WHAT to ingest (one row per source table); watermark_state says HOW FAR
each watermark-driven table has been ingested. Locally both are Delta tables under metadata/.
In the Azure design they live in the SQL control database (sql/quality/01_data_quality_framework.sql)
and ADF reads pipeline_config with a Lookup activity.
"""

import os
from dataclasses import dataclass, asdict

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

from databricks.utilities.config import META_PATH

PIPELINE_CONFIG_PATH = os.path.join(META_PATH, "pipeline_config")
WATERMARK_STATE_PATH = os.path.join(META_PATH, "watermark_state")

INGESTION_TYPES = {"Full", "Watermark", "CDC"}


@dataclass(frozen=True)
class SourceConfig:
    source_name: str
    source_table: str
    destination_table: str
    ingestion_type: str
    watermark_column: str | None
    primary_key_columns: str
    source_location: str
    active_flag: bool = True
    data_quality_threshold: float = 95.0

    @property
    def primary_keys(self) -> list[str]:
        return [c.strip() for c in self.primary_key_columns.split(",")]


PIPELINE_CONFIG_SCHEMA = StructType([
    StructField("source_name", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("destination_table", StringType(), False),
    StructField("ingestion_type", StringType(), False),
    StructField("watermark_column", StringType(), True),
    StructField("primary_key_columns", StringType(), False),
    StructField("source_location", StringType(), False),
    StructField("active_flag", BooleanType(), False),
    StructField("data_quality_threshold", DoubleType(), False),
])

WATERMARK_STATE_SCHEMA = StructType([
    StructField("source_name", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("watermark_value", StringType(), False),
    StructField("ingestion_type", StringType(), False),
    StructField("last_pipeline_run_id", StringType(), False),
    StructField("updated_at", StringType(), False),
])

# Seed rows. source_location is a repo-relative path for file sources, or schema.table for CDC.
# Keep in sync with the INSERT in sql/quality/01_data_quality_framework.sql.
PIPELINE_CONFIG_SEED = [
    SourceConfig("sql_ehr", "patients", "bronze_ehr_patients", "CDC", "_cdc_lsn", "patient_id", "dbo.patients", True, 98.0),
    SourceConfig("sql_ehr", "encounters", "bronze_ehr_encounters", "CDC", "_cdc_lsn", "encounter_id", "dbo.encounters", True, 98.0),
    SourceConfig("sql_ehr", "providers", "bronze_ehr_providers", "CDC", "_cdc_lsn", "provider_id", "dbo.providers", True, 99.0),
    SourceConfig("sql_ehr", "diagnoses", "bronze_ehr_diagnoses", "CDC", "_cdc_lsn", "diagnosis_id", "dbo.diagnoses", True, 95.0),
    SourceConfig("sql_ehr", "lab_results", "bronze_ehr_lab_results", "CDC", "_cdc_lsn", "lab_result_id", "dbo.lab_results", True, 95.0),
    SourceConfig("sql_ehr", "medications", "bronze_ehr_medications", "CDC", "_cdc_lsn", "medication_order_id", "dbo.medications", True, 95.0),
    SourceConfig("fhir_r4", "Patient", "bronze_fhir_patient", "Watermark", "meta_lastUpdated", "resource_id", "sample-data/fhir_r4/*.json", True, 98.0),
    SourceConfig("fhir_r4", "Encounter", "bronze_fhir_encounter", "Watermark", "meta_lastUpdated", "resource_id", "sample-data/fhir_r4/*.json", True, 95.0),
    SourceConfig("fhir_r4", "Observation", "bronze_fhir_observation", "Watermark", "meta_lastUpdated", "resource_id", "sample-data/fhir_r4/*.json", True, 95.0),
    SourceConfig("fhir_r4", "Condition", "bronze_fhir_condition", "Watermark", "meta_lastUpdated", "resource_id", "sample-data/fhir_r4/*.json", True, 95.0),
    SourceConfig("fhir_r4", "MedicationRequest", "bronze_fhir_medication_request", "Watermark", "meta_lastUpdated", "resource_id", "sample-data/fhir_r4/*.json", True, 95.0),
    SourceConfig("fhir_r4", "Practitioner", "bronze_fhir_practitioner", "Watermark", "meta_lastUpdated", "resource_id", "sample-data/fhir_r4/*.json", True, 99.0),
    SourceConfig("claims_csv", "insurance_claims.csv", "bronze_claims", "Full", None, "claim_id", "sample-data/claims_csv/insurance_claims.csv", True, 95.0),
    SourceConfig("claims_csv", "facility_info.csv", "bronze_facilities", "Full", None, "facility_id", "sample-data/claims_csv/facility_info.csv", True, 99.0),
]


def ensure_pipeline_config(spark: SparkSession) -> None:
    """Create pipeline_config from the seed, or reconcile it with the seed.

    The seed defines HOW a source is ingested, so those columns are refreshed on every run.
    active_flag is operational, not definitional: an operator turning a table off keeps it off.
    """
    seed_df = spark.createDataFrame([asdict(c) for c in PIPELINE_CONFIG_SEED], PIPELINE_CONFIG_SCHEMA)
    if not DeltaTable.isDeltaTable(spark, PIPELINE_CONFIG_PATH):
        seed_df.write.format("delta").save(PIPELINE_CONFIG_PATH)
        return
    definition_columns = {c: f"s.{c}" for c in seed_df.columns if c != "active_flag"}
    (
        DeltaTable.forPath(spark, PIPELINE_CONFIG_PATH).alias("t")
        .merge(seed_df.alias("s"), "t.source_name = s.source_name AND t.source_table = s.source_table")
        .whenMatchedUpdate(set=definition_columns)
        .whenNotMatchedInsertAll()
        .execute()
    )


def load_source_configs(spark: SparkSession, source_name: str | None = None,
                        source_table: str | None = None, active_only: bool = True) -> list[SourceConfig]:
    ensure_pipeline_config(spark)
    df = spark.read.format("delta").load(PIPELINE_CONFIG_PATH)
    if active_only:
        df = df.filter("active_flag")
    if source_name:
        df = df.filter(F.col("source_name") == source_name)
    if source_table:
        df = df.filter(F.col("source_table") == source_table)
    configs = [SourceConfig(**row.asDict()) for row in df.orderBy("source_name", "source_table").collect()]
    for cfg in configs:
        if cfg.ingestion_type not in INGESTION_TYPES:
            raise ValueError(f"pipeline_config {cfg.source_name}.{cfg.source_table}: unknown ingestion_type {cfg.ingestion_type!r}")
        if cfg.ingestion_type != "Full" and not cfg.watermark_column:
            raise ValueError(f"pipeline_config {cfg.source_name}.{cfg.source_table}: {cfg.ingestion_type} load needs a watermark_column")
    return configs


def get_source_config(spark: SparkSession, destination_table: str) -> SourceConfig:
    """Look up a config by destination table (used by silver to learn keys and load type)."""
    matches = [c for c in load_source_configs(spark, active_only=False) if c.destination_table == destination_table]
    if len(matches) != 1:
        raise ValueError(f"expected exactly one pipeline_config row for {destination_table}, found {len(matches)}")
    return matches[0]


def get_watermark(spark: SparkSession, cfg: SourceConfig) -> str | None:
    if not DeltaTable.isDeltaTable(spark, WATERMARK_STATE_PATH):
        return None
    rows = (
        spark.read.format("delta").load(WATERMARK_STATE_PATH)
        .filter((F.col("source_name") == cfg.source_name) & (F.col("source_table") == cfg.source_table))
        .select("watermark_value", "ingestion_type")
        .collect()
    )
    if not rows:
        return None
    # A timestamp watermark and an LSN watermark are not comparable. If the table's ingestion
    # type changed, the stored position means nothing: start over with a fresh full read.
    if rows[0]["ingestion_type"] != cfg.ingestion_type:
        print(f"[WATERMARK] {cfg.source_name}.{cfg.source_table}: ingestion_type changed "
              f"{rows[0]['ingestion_type']} -> {cfg.ingestion_type}; ignoring the stored watermark")
        return None
    return rows[0]["watermark_value"]


def advance_watermark(spark: SparkSession, cfg: SourceConfig, new_value: str, run_id: str) -> None:
    """Move the watermark forward to new_value. Never moves it backwards (a rerun of an old run
    must not rewind the table's position).

    Both watermark formats are fixed-width and zero-padded (timestamps 'yyyy-MM-dd HH:mm:ss.ffffff',
    LSNs 20 hex characters), so a string comparison orders them the same way the values do.
    """
    update_df = spark.createDataFrame(
        [(cfg.source_name, cfg.source_table, new_value, cfg.ingestion_type, run_id)],
        "source_name STRING, source_table STRING, watermark_value STRING, ingestion_type STRING, last_pipeline_run_id STRING",
    ).withColumn("updated_at", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSSSSS"))

    if not DeltaTable.isDeltaTable(spark, WATERMARK_STATE_PATH):
        spark.createDataFrame([], WATERMARK_STATE_SCHEMA).write.format("delta").save(WATERMARK_STATE_PATH)
    (
        DeltaTable.forPath(spark, WATERMARK_STATE_PATH).alias("t")
        .merge(update_df.alias("s"), "t.source_name = s.source_name AND t.source_table = s.source_table")
        .whenMatchedUpdateAll(condition="s.ingestion_type <> t.ingestion_type OR s.watermark_value > t.watermark_value")
        .whenNotMatchedInsertAll()
        .execute()
    )
