"""
ClinicalFlow Gold Layer: Star Schema Fact Table Builder
Populates Gold star-schema fact tables (fact_encounter, fact_observation, fact_claim) with surrogate key lookups and late-arriving dimension handling.
"""

import os
import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, LongType
from databricks.utilities.config import SILVER_PATH, GOLD_PATH, get_spark_session, save_df, read_df
from databricks.utilities.logger import PipelineLogger

def build_gold_facts(spark=None, run_id=None):
    if spark is None:
        spark = get_spark_session("ClinicalFlow_Gold_Facts")
    if run_id is None:
        run_id = f"run-{uuid.uuid4().hex[:10]}"
        
    print(f"==================================================")
    print(f"STARTING GOLD LAYER FACTS PROCESSING (Run ID: {run_id})")
    print(f"==================================================")
    
    # Load dim_patient for surrogate key lookup
    dim_patient_path = os.path.join(GOLD_PATH, "dim_patient")
    if not os.path.exists(dim_patient_path):
        print("dim_patient not found. Skipping fact building.")
        return
        
    dim_patient = read_df(spark, dim_patient_path).filter("is_current = true")
    
    # ----------------------------------------------------
    # 1. Build fact_encounter
    # ----------------------------------------------------
    logger_enc = PipelineLogger(spark, run_id, "build_fact_encounter", "gold_warehouse", "GOLD")
    silver_enc_path = os.path.join(SILVER_PATH, "silver_ehr_encounters")
    
    if os.path.exists(silver_enc_path):
        silver_enc = read_df(spark, silver_enc_path)
        
        fact_enc = (
            silver_enc.alias("enc")
            .join(dim_patient.alias("pat"), F.col("enc.patient_id") == F.col("pat.patient_id"), "left_outer")
            .withColumn("patient_sk", F.coalesce(F.col("pat.patient_sk"), F.lit(-1))) # Late-arriving handling
            .withColumn("provider_sk", F.lit(-1))
            .withColumn("facility_sk", F.lit(-1))
            .withColumn("department_sk", F.lit(-1))
            .withColumn("admission_date_key", F.date_format("enc.admission_timestamp", "yyyyMMdd").cast(IntegerType()))
            .withColumn("discharge_date_key", F.date_format("enc.discharge_timestamp", "yyyyMMdd").cast(IntegerType()))
            .withColumn("length_of_stay_hours", 
                        F.round((F.col("enc.discharge_timestamp").cast("long") - F.col("enc.admission_timestamp").cast("long")) / 3600.0, 2))
            .withColumn("is_readmission_30d", F.lit(False))
            .select(
                "encounter_id", "patient_sk", "provider_sk", "facility_sk", "department_sk",
                "admission_date_key", "discharge_date_key", "encounter_type", "length_of_stay_hours",
                "discharge_disposition", "is_readmission_30d"
            )
        )
        
        target_path = os.path.join(GOLD_PATH, "fact_encounter")
        save_df(fact_enc, target_path, "overwrite")
        logger_enc.log_run(rows_read=silver_enc.count(), rows_inserted=fact_enc.count(), status="SUCCESS")
        print(f"[GOLD FACT_ENCOUNTER] Written {fact_enc.count()} rows into fact_encounter.")

    # ----------------------------------------------------
    # 2. Build fact_observation
    # ----------------------------------------------------
    logger_obs = PipelineLogger(spark, run_id, "build_fact_observation", "gold_warehouse", "GOLD")
    silver_obs_path = os.path.join(SILVER_PATH, "silver_fhir_observations")
    
    if os.path.exists(silver_obs_path):
        silver_obs = read_df(spark, silver_obs_path)
        
        fact_obs = (
            silver_obs.alias("obs")
            .join(dim_patient.alias("pat"), F.col("obs.patient_id") == F.col("pat.patient_id"), "left_outer")
            .withColumn("patient_sk", F.coalesce(F.col("pat.patient_sk"), F.lit(-1)))
            .withColumn("observation_date_key", F.date_format("obs.observation_timestamp", "yyyyMMdd").cast(IntegerType()))
            .withColumn("turnaround_time_minutes", F.lit(15))
            .select(
                "observation_id", "patient_sk", "encounter_id", "observation_date_key",
                "loinc_code", "test_name", "result_value", "result_unit", "turnaround_time_minutes"
            )
        )
        
        target_path = os.path.join(GOLD_PATH, "fact_observation")
        save_df(fact_obs, target_path, "overwrite")
        logger_obs.log_run(rows_read=silver_obs.count(), rows_inserted=fact_obs.count(), status="SUCCESS")
        print(f"[GOLD FACT_OBSERVATION] Written {fact_obs.count()} rows into fact_observation.")

    # ----------------------------------------------------
    # 3. Build fact_claim
    # ----------------------------------------------------
    logger_clm = PipelineLogger(spark, run_id, "build_fact_claim", "gold_warehouse", "GOLD")
    silver_clm_path = os.path.join(SILVER_PATH, "silver_claims")
    
    if os.path.exists(silver_clm_path):
        silver_clm = read_df(spark, silver_clm_path)
        
        fact_clm = (
            silver_clm.alias("clm")
            .join(dim_patient.alias("pat"), F.col("clm.patient_id") == F.col("pat.patient_id"), "left_outer")
            .withColumn("patient_sk", F.coalesce(F.col("pat.patient_sk"), F.lit(-1)))
            .withColumn("facility_sk", F.lit(-1))
            .withColumn("service_date_key", F.date_format("clm.service_date", "yyyyMMdd").cast(IntegerType()))
            .select(
                "claim_id", "patient_sk", "facility_sk", "service_date_key",
                "claim_amount", "paid_amount", "claim_status", F.col("clm.insurance_type").alias("insurance_type")
            )
        )
        
        target_path = os.path.join(GOLD_PATH, "fact_claim")
        save_df(fact_clm, target_path, "overwrite")
        logger_clm.log_run(rows_read=silver_clm.count(), rows_inserted=fact_clm.count(), status="SUCCESS")
        print(f"[GOLD FACT_CLAIM] Written {fact_clm.count()} rows into fact_claim.")

    print("GOLD FACTS PROCESSING COMPLETED SUCCESSFULLY.")

if __name__ == "__main__":
    build_gold_facts()
