"""
ClinicalFlow Gold Layer: Dimensional Model Builder with SCD Type 2
Implements Slowly Changing Dimension (SCD) Type 2 tracking for dim_patient and populates Gold star-schema dimensions.
"""

import os
import uuid
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, IntegerType, BooleanType, DateType, TimestampType
from databricks.utilities.config import SILVER_PATH, GOLD_PATH, get_spark_session, add_record_hash, save_df, read_df
from databricks.gold.silver_reader import read_silver_current
from databricks.utilities.logger import PipelineLogger

def build_dim_patient(spark, run_id):
    """
    Builds dim_patient with SCD Type 2 historical change tracking.
    Preserves address and insurance history over time.
    """
    logger = PipelineLogger(spark, run_id, "build_dim_patient_scd2", "gold_warehouse", "GOLD")
    
    # Read Silver patient datasets (EHR + FHIR)
    ehr_pat_path = os.path.join(SILVER_PATH, "silver_ehr_patients")
    fhir_pat_path = os.path.join(SILVER_PATH, "silver_fhir_patients")
    
    dfs = []
    if os.path.exists(ehr_pat_path):
        dfs.append(read_silver_current(spark, "silver_ehr_patients"))
    if os.path.exists(fhir_pat_path):
        # Typed placeholders: an untyped lit(None) is VOID, which Delta cannot store, so the
        # column would vanish from the written table and break the next run's merge.
        dfs.append(read_silver_current(spark, "silver_fhir_patients")
                   .withColumn("ssn_hash", F.lit(None).cast("string"))
                   .withColumn("phone_number", F.lit(None).cast("string")))
        
    if not dfs:
        print("No Silver patient datasets available for dim_patient.")
        return
        
    incoming_df = dfs[0]
    for extra_df in dfs[1:]:
        incoming_df = incoming_df.unionByName(extra_df, allowMissingColumns=True)
        
    # Deduplicate incoming by patient_id keeping latest source_updated_at
    w = Window.partitionBy("patient_id").orderBy(F.col("source_updated_at").desc())
    incoming_latest = (
        incoming_df
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
    )
    
    dim_patient_path = os.path.join(GOLD_PATH, "dim_patient")
    
    if not os.path.exists(dim_patient_path) or len([f for f in os.listdir(dim_patient_path) if not f.startswith(".")]) == 0:
        # Initial Population
        dim_patient = (
            incoming_latest
            .withColumn("patient_sk", F.monotonically_increasing_id() + 1)
            .withColumn("effective_start_date", F.col("source_updated_at"))
            .withColumn("effective_end_date", F.lit(None).cast(TimestampType()))
            .withColumn("is_current", F.lit(True))
            .select(
                "patient_sk", "patient_id", "first_name", "last_name", "date_of_birth",
                "gender", "address_street", "city", "state", "postal_code", "phone_number",
                "insurance_type", "effective_start_date", "effective_end_date", "is_current", "record_hash"
            )
        )
        save_df(dim_patient, dim_patient_path, "overwrite")
        logger.log_run(rows_read=incoming_latest.count(), rows_inserted=dim_patient.count(), status="SUCCESS")
        print(f"[GOLD DIM_PATIENT] Initialized dim_patient with {dim_patient.count()} records.")
    else:
        # Incremental SCD Type 2 Merge
        current_dim = read_df(spark, dim_patient_path).cache()
        current_dim.count() # Materialize in memory to prevent lazy evaluation overwrite race condition
        
        # 1. Active rows in existing dimension
        active_dim = current_dim.filter("is_current = true")
        
        # 2. Identify changed records by comparing record_hash
        joined = incoming_latest.alias("inc").join(
            active_dim.alias("curr"),
            F.col("inc.patient_id") == F.col("curr.patient_id"),
            "left_outer"
        )
        
        # Unchanged rows
        unchanged = joined.filter("curr.patient_id IS NOT NULL AND inc.record_hash = curr.record_hash").select("curr.*")
        
        # Expired version of modified rows
        expired = joined.filter("curr.patient_id IS NOT NULL AND inc.record_hash != curr.record_hash").select(
            F.col("curr.patient_sk"),
            F.col("curr.patient_id"),
            F.col("curr.first_name"),
            F.col("curr.last_name"),
            F.col("curr.date_of_birth"),
            F.col("curr.gender"),
            F.col("curr.address_street"),
            F.col("curr.city"),
            F.col("curr.state"),
            F.col("curr.postal_code"),
            F.col("curr.phone_number"),
            F.col("curr.insurance_type"),
            F.col("curr.effective_start_date"),
            F.col("inc.source_updated_at").alias("effective_end_date"),
            F.lit(False).alias("is_current"),
            F.col("curr.record_hash")
        )
        
        # New active version of modified rows & brand new patients
        new_active = joined.filter("curr.patient_id IS NULL OR inc.record_hash != curr.record_hash").select(
            (F.monotonically_increasing_id() + current_dim.count() + 1000).alias("patient_sk"),
            F.col("inc.patient_id"),
            F.col("inc.first_name"),
            F.col("inc.last_name"),
            F.col("inc.date_of_birth"),
            F.col("inc.gender"),
            F.col("inc.address_street"),
            F.col("inc.city"),
            F.col("inc.state"),
            F.col("inc.postal_code"),
            F.col("inc.phone_number"),
            F.col("inc.insurance_type"),
            F.col("inc.source_updated_at").alias("effective_start_date"),
            F.lit(None).cast(TimestampType()).alias("effective_end_date"),
            F.lit(True).alias("is_current"),
            F.col("inc.record_hash")
        )
        
        # Inactive past versions that remain unchanged
        inactive_dim = current_dim.filter("is_current = false")
        
        final_dim = unchanged.unionByName(expired).unionByName(new_active).unionByName(inactive_dim)
        save_df(final_dim, dim_patient_path, "overwrite")
        logger.log_run(rows_read=incoming_latest.count(), rows_inserted=new_active.count(), rows_updated=expired.count(), status="SUCCESS")
        print(f"[GOLD DIM_PATIENT SCD2] Processed incremental update. Current total rows: {final_dim.count()}")

def build_dim_date(spark):
    """Populates dim_date table covering 2020-2030"""
    dim_date_path = os.path.join(GOLD_PATH, "dim_date")
    if os.path.exists(dim_date_path):
        return
        
    dates_df = spark.sql("""
        SELECT 
            CAST(DATE_FORMAT(d, 'yyyyMMdd') AS INT) as date_key,
            d as full_date,
            DAYOFWEEK(d) as day_of_week,
            DATE_FORMAT(d, 'EEEE') as day_name,
            DAYOFMONTH(d) as day_of_month,
            DAYOFYEAR(d) as day_of_year,
            WEEKOFYEAR(d) as week_of_year,
            MONTH(d) as month_number,
            DATE_FORMAT(d, 'MMMM') as month_name,
            QUARTER(d) as quarter,
            YEAR(d) as year,
            CASE WHEN DAYOFWEEK(d) IN (1, 7) THEN true ELSE false END as is_weekend
        FROM (
            SELECT EXPLODE(SEQUENCE(TO_DATE('2020-01-01'), TO_DATE('2030-12-31'), INTERVAL 1 DAY)) as d
        )
    """)
    save_df(dates_df, dim_date_path, "overwrite")
    print(f"[GOLD DIM_DATE] Populated dim_date with {dates_df.count()} rows.")

def build_gold_dimensions(spark=None, run_id=None):
    if spark is None:
        spark = get_spark_session("ClinicalFlow_Gold_Dimensions")
    if run_id is None:
        run_id = f"run-{uuid.uuid4().hex[:10]}"
        
    print(f"==================================================")
    print(f"STARTING GOLD LAYER DIMENSIONS PROCESSING (Run ID: {run_id})")
    print(f"==================================================")
    
    build_dim_date(spark)
    build_dim_patient(spark, run_id)
    
    # Facilities dimension
    facilities_path = os.path.join(SILVER_PATH, "silver_facilities")
    dim_fac_path = os.path.join(GOLD_PATH, "dim_facility")
    if os.path.exists(facilities_path):
        fac_df = read_silver_current(spark, "silver_facilities")
        dim_fac = (
            fac_df
            .withColumn("facility_sk", F.monotonically_increasing_id() + 1)
            .select("facility_sk", "facility_id", "facility_name", "facility_type", "address", "city", "state", "postal_code")
        )
        save_df(dim_fac, dim_fac_path, "overwrite")
        print(f"[GOLD DIM_FACILITY] Updated dim_facility with {dim_fac.count()} rows.")

    print("GOLD DIMENSIONS PROCESSING COMPLETED SUCCESSFULLY.")

    print("GOLD DIMENSIONS PROCESSING COMPLETED SUCCESSFULLY.")

if __name__ == "__main__":
    build_gold_dimensions()
