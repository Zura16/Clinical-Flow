"""
ClinicalFlow Dynamic Data Quality Engine
Evaluates configurable data quality rules against PySpark DataFrames, segregating invalid records into a quarantine table.
"""

import os
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from databricks.utilities.config import META_PATH

QUARANTINE_TABLE_PATH = os.path.join(META_PATH, "quarantine_records")

QUARANTINE_SCHEMA = StructType([
    StructField("pipeline_run_id", StringType(), False),
    StructField("source_name", StringType(), False),
    StructField("record_identifier", StringType(), False),
    StructField("failed_rule", StringType(), False),
    StructField("error_message", StringType(), False),
    StructField("raw_payload", StringType(), False),
    StructField("detected_timestamp", TimestampType(), False),
    StructField("resolution_status", StringType(), False)
])

# Default Data Quality Rules per dataset
RULES_CATALOG = {
    "silver_patients": [
        {"column": "patient_id", "type": "NOT_NULL", "expr": "patient_id IS NOT NULL", "severity": "CRITICAL"},
        {"column": "date_of_birth", "type": "NOT_NULL", "expr": "date_of_birth IS NOT NULL", "severity": "ERROR"}
    ],
    "silver_encounters": [
        {"column": "encounter_id", "type": "NOT_NULL", "expr": "encounter_id IS NOT NULL", "severity": "CRITICAL"},
        {"column": "patient_id", "type": "NOT_NULL", "expr": "patient_id IS NOT NULL", "severity": "CRITICAL"},
        {"column": "discharge_timestamp", "type": "RANGE", "expr": "discharge_timestamp IS NULL OR discharge_timestamp >= admission_timestamp", "severity": "ERROR"}
    ],
    "silver_observations": [
        {"column": "observation_id", "type": "NOT_NULL", "expr": "observation_id IS NOT NULL", "severity": "CRITICAL"},
        {"column": "patient_id", "type": "NOT_NULL", "expr": "patient_id IS NOT NULL", "severity": "CRITICAL"},
        {"column": "result_value", "type": "RANGE", "expr": "result_value IS NULL OR (result_value >= -500 AND result_value <= 50000)", "severity": "ERROR"}
    ],
    "silver_claims": [
        {"column": "claim_id", "type": "NOT_NULL", "expr": "claim_id IS NOT NULL", "severity": "CRITICAL"},
        {"column": "claim_amount", "type": "RANGE", "expr": "claim_amount >= 0", "severity": "ERROR"}
    ]
}

class DataQualityEngine:
    def __init__(self, spark, dataset_name, pipeline_run_id):
        self.spark = spark
        self.dataset_name = dataset_name
        self.pipeline_run_id = pipeline_run_id
        self.rules = RULES_CATALOG.get(dataset_name, [])

    def validate(self, df, id_col="id"):
        """
        Evaluates rules against DataFrame.
        Returns: (valid_df, rejected_count)
        Writes quarantine records to Delta Lake quarantine path.
        """
        if not self.rules or df.count() == 0:
            return df, 0
            
        total_rejected = 0
        valid_df = df
        
        for rule in self.rules:
            expr_str = rule["expr"]
            column_name = rule["column"]
            severity = rule["severity"]
            
            # Identify failing records
            invalid_df = valid_df.filter(f"NOT ({expr_str})")
            invalid_count = invalid_df.count()
            
            if invalid_count > 0:
                print(f"[DATA QUALITY ALERT] Rule failed: '{expr_str}' on dataset '{self.dataset_name}'. Violations: {invalid_count}")
                total_rejected += invalid_count
                
                # Format payload into JSON for quarantine tracking
                quarantine_df = (
                    invalid_df
                    .withColumn("pipeline_run_id", F.lit(self.pipeline_run_id))
                    .withColumn("source_name", F.lit(self.dataset_name))
                    .withColumn("record_identifier", F.coalesce(F.col(id_col).cast("string"), F.lit("UNKNOWN")))
                    .withColumn("failed_rule", F.lit(f"{rule['type']}:{column_name}"))
                    .withColumn("error_message", F.lit(f"Failed expression: {expr_str} (Severity: {severity})"))
                    .withColumn("raw_payload", F.to_json(F.struct("*")))
                    .withColumn("detected_timestamp", F.current_timestamp())
                    .withColumn("resolution_status", F.lit("PENDING"))
                    .select(
                        "pipeline_run_id", "source_name", "record_identifier",
                        "failed_rule", "error_message", "raw_payload",
                        "detected_timestamp", "resolution_status"
                    )
                )
                
                # Write to quarantine Delta table
                try:
                    quarantine_df.write.format("delta").mode("append").save(QUARANTINE_TABLE_PATH)
                except Exception:
                    quarantine_df.write.format("parquet").mode("append").save(QUARANTINE_TABLE_PATH)
                
                # Keep only valid rows
                valid_df = valid_df.filter(expr_str)
                
        return valid_df, total_rejected
