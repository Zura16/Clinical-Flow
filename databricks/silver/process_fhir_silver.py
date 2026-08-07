"""
ClinicalFlow Silver Layer: FHIR R4 JSON Flattening, Normalization & Quality Validation Engine
Processes nested FHIR resources from Bronze, flattens them into relational schemas, calculates record hashes, and enforces quality rules.
"""

import os
import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from databricks.utilities.config import BRONZE_PATH, SILVER_PATH, get_spark_session, add_record_hash, save_df, read_df
from databricks.utilities.logger import PipelineLogger
from databricks.utilities.quality_engine import DataQualityEngine

def process_fhir_to_silver(spark=None, run_id=None):
    if spark is None:
        spark = get_spark_session("ClinicalFlow_Silver_FHIR")
    if run_id is None:
        run_id = f"run-{uuid.uuid4().hex[:10]}"
        
    print(f"==================================================")
    print(f"STARTING SILVER LAYER FHIR PROCESSING (Run ID: {run_id})")
    print(f"==================================================")
    
    bronze_fhir_path = os.path.join(BRONZE_PATH, "fhir_raw")
    if not os.path.exists(bronze_fhir_path):
        print("No Bronze FHIR data found. Skipping FHIR processing.")
        return
        
    bronze_df = read_df(spark, bronze_fhir_path)
    
    # ----------------------------------------------------
    # 1. Process FHIR Patients
    # ----------------------------------------------------
    logger_pat = PipelineLogger(spark, run_id, "process_silver_fhir_patient", "fhir_r4", "SILVER")
    patients_df = bronze_df.filter("resourceType = 'Patient'")
    
    if patients_df.count() > 0:
        flat_patients = (
            patients_df
            .select(
                F.col("id").alias("patient_id"),
                F.col("name")[0]["given"][0].alias("first_name"),
                F.col("name")[0]["family"].alias("last_name"),
                F.col("birthDate").cast("date").alias("date_of_birth"),
                F.col("gender").alias("gender"),
                F.col("address")[0]["line"][0].alias("address_street"),
                F.col("address")[0]["city"].alias("city"),
                F.col("address")[0]["state"].alias("state"),
                F.col("address")[0]["postalCode"].alias("postal_code"),
                F.lit("Commercial").alias("insurance_type"),
                F.col("meta.lastUpdated").cast("timestamp").alias("source_updated_at")
            )
        )
        
        # Add SHA-256 hash for deduplication
        flat_patients = add_record_hash(flat_patients, ["patient_id", "first_name", "last_name", "date_of_birth", "address_street", "insurance_type"])
        
        # Quality Validation
        dq_engine = DataQualityEngine(spark, "silver_patients", run_id)
        valid_patients, rejected_count = dq_engine.validate(flat_patients, id_col="patient_id")
        
        silver_pat_path = os.path.join(SILVER_PATH, "silver_fhir_patients")
        save_df(valid_patients, silver_pat_path, "overwrite")
        
        rows_inserted = valid_patients.count()
        logger_pat.log_run(rows_read=flat_patients.count(), rows_inserted=rows_inserted, rows_rejected=rejected_count, status="SUCCESS")
        print(f"[SILVER FHIR PATIENTS] Processed {rows_inserted} valid patient records into {silver_pat_path}")

    # ----------------------------------------------------
    # 2. Process FHIR Observations
    # ----------------------------------------------------
    logger_obs = PipelineLogger(spark, run_id, "process_silver_fhir_observation", "fhir_r4", "SILVER")
    obs_df = bronze_df.filter("resourceType = 'Observation'")
    
    if obs_df.count() > 0:
        flat_obs = (
            obs_df
            .select(
                F.col("id").alias("observation_id"),
                F.regexp_replace(F.col("subject.reference"), "Patient/", "").alias("patient_id"),
                F.regexp_replace(F.col("encounter.reference"), "Encounter/", "").alias("encounter_id"),
                F.col("code.coding")[0]["code"].alias("loinc_code"),
                F.col("code.coding")[0]["display"].alias("test_name"),
                F.col("valueQuantity.value").cast(DoubleType()).alias("result_value"),
                F.col("valueQuantity.unit").alias("result_unit"),
                F.col("effectiveDateTime").cast("timestamp").alias("observation_timestamp"),
                F.col("status").alias("observation_status")
            )
        )
        
        # Add SHA-256 hash for deduplication
        flat_obs = add_record_hash(flat_obs, ["observation_id", "patient_id", "loinc_code", "result_value", "observation_timestamp"])
        
        # Quality Validation
        dq_engine = DataQualityEngine(spark, "silver_observations", run_id)
        valid_obs, rejected_count = dq_engine.validate(flat_obs, id_col="observation_id")
        
        silver_obs_path = os.path.join(SILVER_PATH, "silver_fhir_observations")
        save_df(valid_obs, silver_obs_path, "overwrite")
        
        rows_inserted = valid_obs.count()
        logger_obs.log_run(rows_read=flat_obs.count(), rows_inserted=rows_inserted, rows_rejected=rejected_count, status="SUCCESS")
        print(f"[SILVER FHIR OBSERVATIONS] Processed {rows_inserted} valid observations into {silver_obs_path}")

    print("SILVER FHIR PROCESSING COMPLETED SUCCESSFULLY.")

if __name__ == "__main__":
    process_fhir_to_silver()
