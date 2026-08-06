"""
ClinicalFlow Audit & Lineage Logger
Provides audit logging capabilities writing pipeline metadata into the central pipeline_run_audit table.
"""

import os
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
from databricks.utilities.config import META_PATH, get_spark_session

AUDIT_TABLE_PATH = os.path.join(META_PATH, "pipeline_run_audit")

AUDIT_SCHEMA = StructType([
    StructField("pipeline_run_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("source_name", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("start_timestamp", TimestampType(), False),
    StructField("end_timestamp", TimestampType(), True),
    StructField("rows_read", LongType(), True),
    StructField("rows_inserted", LongType(), True),
    StructField("rows_updated", LongType(), True),
    StructField("rows_deleted", LongType(), True),
    StructField("rows_rejected", LongType(), True),
    StructField("watermark_start", StringType(), True),
    StructField("watermark_end", StringType(), True),
    StructField("execution_status", StringType(), False),
    StructField("error_code", StringType(), True),
    StructField("error_message", StringType(), True)
])

class PipelineLogger:
    def __init__(self, spark, pipeline_run_id, pipeline_name, source_name, layer):
        self.spark = spark
        self.pipeline_run_id = pipeline_run_id
        self.pipeline_name = pipeline_name
        self.source_name = source_name
        self.layer = layer
        self.start_timestamp = datetime.utcnow()

    def log_run(self, rows_read=0, rows_inserted=0, rows_updated=0, rows_deleted=0, rows_rejected=0,
                watermark_start=None, watermark_end=None, status="SUCCESS", error_code=None, error_message=None):
        """Logs run metrics to Delta Lake audit table"""
        end_timestamp = datetime.utcnow()
        
        audit_data = [(
            self.pipeline_run_id,
            self.pipeline_name,
            self.source_name,
            self.layer,
            self.start_timestamp,
            end_timestamp,
            int(rows_read),
            int(rows_inserted),
            int(rows_updated),
            int(rows_deleted),
            int(rows_rejected),
            str(watermark_start) if watermark_start else None,
            str(watermark_end) if watermark_end else None,
            status,
            error_code,
            error_message
        )]
        
        audit_df = self.spark.createDataFrame(audit_data, AUDIT_SCHEMA)
        
        # Append to Delta Lake audit path or Parquet if Delta format extension active
        try:
            audit_df.write.format("delta").mode("append").save(AUDIT_TABLE_PATH)
        except Exception:
            audit_df.write.format("parquet").mode("append").save(AUDIT_TABLE_PATH)
            
        print(f"[AUDIT LOG] {self.pipeline_name} ({self.layer}) - Status: {status} | Read: {rows_read} | Inserted: {rows_inserted} | Rejected: {rows_rejected}")
