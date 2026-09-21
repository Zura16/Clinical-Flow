"""
ClinicalFlow Silver Layer: Relational EHR CDC & Claims Delta Processing Engine
Ingests relational EHR tables and Claims CSV data from Bronze, cleans schemas, calculates record hashes, and validates quality.
"""

import os
import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, DateType, TimestampType
from databricks.utilities.config import SILVER_PATH, get_spark_session, add_record_hash, save_df
from databricks.silver.bronze_reader import bronze_exists, read_bronze_current
from databricks.utilities.logger import PipelineLogger
from databricks.utilities.quality_engine import DataQualityEngine

def process_relational_to_silver(spark=None, run_id=None):
    if spark is None:
        spark = get_spark_session("ClinicalFlow_Silver_Relational")
    if run_id is None:
        run_id = f"run-{uuid.uuid4().hex[:10]}"
        
    print(f"==================================================")
    print(f"STARTING SILVER LAYER RELATIONAL PROCESSING (Run ID: {run_id})")
    print(f"==================================================")
    
    # ----------------------------------------------------
    # 1. Process EHR Patients
    # ----------------------------------------------------
    if bronze_exists("bronze_ehr_patients"):
        logger = PipelineLogger(spark, run_id, "process_silver_ehr_patients", "sql_ehr", "SILVER")
        df = read_bronze_current(spark, "bronze_ehr_patients")
        
        clean_df = (
            df.select(
                F.col("patient_id"),
                F.col("first_name"),
                F.col("last_name"),
                F.col("date_of_birth").cast(DateType()),
                F.col("gender"),
                F.col("ssn_hash"),
                F.col("address_street"),
                F.col("city"),
                F.col("state"),
                F.col("postal_code"),
                F.col("phone_number"),
                F.col("insurance_type"),
                F.col("updated_at").cast(TimestampType()).alias("source_updated_at")
            )
        )
        
        clean_df = add_record_hash(clean_df, ["patient_id", "first_name", "last_name", "date_of_birth", "address_street", "insurance_type"])
        
        dq_engine = DataQualityEngine(spark, "silver_patients", run_id)
        valid_df, rejected_count = dq_engine.validate(clean_df, id_col="patient_id")
        
        target_path = os.path.join(SILVER_PATH, "silver_ehr_patients")
        save_df(valid_df, target_path, "overwrite")
        logger.log_run(rows_read=clean_df.count(), rows_inserted=valid_df.count(), rows_rejected=rejected_count, status="SUCCESS")

    # ----------------------------------------------------
    # 2. Process EHR Encounters
    # ----------------------------------------------------
    if bronze_exists("bronze_ehr_encounters"):
        logger = PipelineLogger(spark, run_id, "process_silver_ehr_encounters", "sql_ehr", "SILVER")
        df = read_bronze_current(spark, "bronze_ehr_encounters")
        
        clean_df = (
            df.select(
                F.col("encounter_id"),
                F.col("patient_id"),
                F.col("provider_id"),
                F.col("facility_id"),
                F.col("department_id"),
                F.col("encounter_type"),
                F.col("admission_timestamp").cast(TimestampType()),
                F.col("discharge_timestamp").cast(TimestampType()),
                F.col("discharge_disposition"),
                F.col("updated_at").cast(TimestampType()).alias("source_updated_at")
            )
        )
        
        clean_df = add_record_hash(clean_df, ["encounter_id", "patient_id", "provider_id", "admission_timestamp", "discharge_timestamp"])
        
        dq_engine = DataQualityEngine(spark, "silver_encounters", run_id)
        valid_df, rejected_count = dq_engine.validate(clean_df, id_col="encounter_id")
        
        target_path = os.path.join(SILVER_PATH, "silver_ehr_encounters")
        save_df(valid_df, target_path, "overwrite")
        logger.log_run(rows_read=clean_df.count(), rows_inserted=valid_df.count(), rows_rejected=rejected_count, status="SUCCESS")

    # ----------------------------------------------------
    # 3. Process Claims CSV
    # ----------------------------------------------------
    if bronze_exists("bronze_claims"):
        logger = PipelineLogger(spark, run_id, "process_silver_claims", "claims_csv", "SILVER")
        df = read_bronze_current(spark, "bronze_claims")
        
        clean_df = (
            df.select(
                F.col("claim_id"),
                F.col("patient_id"),
                F.col("facility_id"),
                F.col("service_date").cast(DateType()),
                F.col("claim_amount").cast(DoubleType()),
                F.col("paid_amount").cast(DoubleType()),
                F.col("claim_status"),
                F.col("insurance_type")
            )
        )
        
        clean_df = add_record_hash(clean_df, ["claim_id", "patient_id", "service_date", "claim_amount"])
        
        dq_engine = DataQualityEngine(spark, "silver_claims", run_id)
        valid_df, rejected_count = dq_engine.validate(clean_df, id_col="claim_id")
        
        target_path = os.path.join(SILVER_PATH, "silver_claims")
        save_df(valid_df, target_path, "overwrite")
        logger.log_run(rows_read=clean_df.count(), rows_inserted=valid_df.count(), rows_rejected=rejected_count, status="SUCCESS")

    print("SILVER RELATIONAL PROCESSING COMPLETED SUCCESSFULLY.")

if __name__ == "__main__":
    process_relational_to_silver()
