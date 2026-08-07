"""
ClinicalFlow Metadata-Driven Bronze Ingestion Engine
Ingests multi-source raw clinical data into the Bronze Lakehouse layer based on pipeline_config metadata.
"""

import os
import uuid
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F
from databricks.utilities.config import BRONZE_PATH, BASE_DIR, get_spark_session, add_record_hash, save_df
from databricks.utilities.logger import PipelineLogger

def run_bronze_ingestion(spark=None, run_id=None):
    if spark is None:
        spark = get_spark_session("ClinicalFlow_Bronze_Ingestion")
    if run_id is None:
        run_id = f"run-{uuid.uuid4().hex[:10]}"
        
    print(f"==================================================")
    print(f"STARTING BRONZE LAYER INGESTION (Run ID: {run_id})")
    print(f"==================================================")
    
    sample_data_dir = os.path.join(BASE_DIR, "sample-data")
    fhir_bundle_path = os.path.join(sample_data_dir, "fhir_r4", "fhir_r4_synthetic_bundle.json")
    ehr_dir = os.path.join(sample_data_dir, "sql_ehr")
    claims_dir = os.path.join(sample_data_dir, "claims_csv")
    
    # 1. Ingest Synthea FHIR R4 JSON Bundle
    if os.path.exists(fhir_bundle_path):
        logger = PipelineLogger(spark, run_id, "ingest_fhir_bundle", "fhir_r4", "BRONZE")
        raw_fhir_df = spark.read.option("multiline", "true").json(fhir_bundle_path)
        
        # Explode entry array to extract individual resources
        if "entry" in raw_fhir_df.columns:
            entries_df = (
                raw_fhir_df.select(F.explode("entry").alias("entry"))
                .select("entry.resource.*")
                .withColumn("ingestion_timestamp", F.current_timestamp())
                .withColumn("pipeline_run_id", F.lit(run_id))
            )
            rows_read = entries_df.count()
            
            output_path = os.path.join(BRONZE_PATH, "fhir_raw")
            save_df(entries_df, output_path, "overwrite")
            logger.log_run(rows_read=rows_read, rows_inserted=rows_read, status="SUCCESS")
            print(f"[BRONZE FHIR] Ingested {rows_read} raw FHIR resources into {output_path}")

    # 2. Ingest SQL EHR Relational Tables (patients, encounters, providers, diagnoses, lab_results, medications)
    ehr_tables = ["patients", "encounters", "providers", "diagnoses", "lab_results", "medications"]
    for table_name in ehr_tables:
        csv_path = os.path.join(ehr_dir, f"{table_name}.csv")
        if os.path.exists(csv_path):
            logger = PipelineLogger(spark, run_id, f"ingest_ehr_{table_name}", "sql_ehr", "BRONZE")
            raw_df = spark.read.option("header", "true").csv(csv_path)
            
            raw_df = (
                raw_df
                .withColumn("ingestion_timestamp", F.current_timestamp())
                .withColumn("pipeline_run_id", F.lit(run_id))
            )
            rows_read = raw_df.count()
            
            output_path = os.path.join(BRONZE_PATH, f"ehr_{table_name}")
            save_df(raw_df, output_path, "overwrite")
            logger.log_run(rows_read=rows_read, rows_inserted=rows_read, status="SUCCESS")
            print(f"[BRONZE EHR] Ingested {rows_read} rows into ehr_{table_name}")

    # 3. Ingest External Claims CSV and Reference Tables
    claims_files = [("insurance_claims.csv", "claims"), ("facility_info.csv", "facilities")]
    for filename, target_table in claims_files:
        csv_path = os.path.join(claims_dir, filename)
        if os.path.exists(csv_path):
            logger = PipelineLogger(spark, run_id, f"ingest_{target_table}", "claims_csv", "BRONZE")
            raw_df = spark.read.option("header", "true").csv(csv_path)
            
            raw_df = (
                raw_df
                .withColumn("ingestion_timestamp", F.current_timestamp())
                .withColumn("pipeline_run_id", F.lit(run_id))
            )
            rows_read = raw_df.count()
            
            output_path = os.path.join(BRONZE_PATH, target_table)
            save_df(raw_df, output_path, "overwrite")
            logger.log_run(rows_read=rows_read, rows_inserted=rows_read, status="SUCCESS")
            print(f"[BRONZE CLAIMS] Ingested {rows_read} rows into {target_table}")

    print("BRONZE INGESTION COMPLETED SUCCESSFULLY.")
    return run_id

if __name__ == "__main__":
    run_bronze_ingestion()
