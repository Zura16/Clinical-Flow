"""
Gold facts.

Two things make these facts rather than copies of silver:

1. **Point-in-time dimension joins.** A fact resolves to the version of the patient that was
   current *when the event happened*, not to whoever that patient is today. Joining on
   `is_current` would rewrite history every time a patient moved house.
2. **Measures are computed, never assumed.** Length of stay, lab turnaround and the readmission
   flag come from the data. Where the source cannot support a measure (a FHIR observation has no
   order time), the column is NULL and says so.

Facts merge on their business key, so a rerun updates in place. A record deleted in silver is
deleted from the fact: the dimension keeps the history, the fact reflects what exists.

    python -m databricks.gold.build_facts [--run-id ID]
"""

import argparse
import os
import uuid

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from databricks.gold.keys import UNKNOWN_SK, date_key, surrogate_key
from databricks.gold.silver_reader import read_silver_current, silver_exists
from databricks.utilities.config import GOLD_PATH, get_spark_session
from databricks.utilities.logger import PipelineLogger

READMISSION_WINDOW_DAYS = 30


def gold_path(table: str) -> str:
    return os.path.join(GOLD_PATH, table)


def patient_sk_as_of(
    spark: SparkSession, df: DataFrame, patient_id: Column, event_time: Column, source_system: str
) -> DataFrame:
    """Attach the patient_sk that was current when the event happened.

    An event with no matching version (an unknown patient, or one whose first version starts after
    the event) resolves to the unknown member rather than dropping the fact or carrying a NULL.
    """
    dim = (
        spark.read.format("delta")
        .load(gold_path("dim_patient"))
        .filter(F.col("patient_sk") != UNKNOWN_SK)
        .filter(F.col("source_system") == source_system)
        .select(
            F.col("patient_sk").alias("_dim_sk"),
            F.col("patient_id").alias("_dim_patient_id"),
            F.col("effective_start_date").alias("_dim_start"),
            # An open version runs to the end of time, so the window is always closed.
            F.coalesce(F.col("effective_end_date"), F.lit("9999-12-31 23:59:59").cast("timestamp")).alias("_dim_end"),
        )
    )
    joined = df.join(
        dim,
        (patient_id == F.col("_dim_patient_id"))
        & (event_time >= F.col("_dim_start"))
        & (event_time < F.col("_dim_end")),
        "left_outer",
    )
    return joined.withColumn("patient_sk", F.coalesce(F.col("_dim_sk"), F.lit(UNKNOWN_SK))).drop(
        "_dim_sk", "_dim_patient_id", "_dim_start", "_dim_end"
    )


def merge_fact(spark: SparkSession, df: DataFrame, table: str, key: str, run_id: str, source_name: str) -> int:
    logger = PipelineLogger(spark, run_id, f"gold:{table}", source_name, "GOLD")
    try:
        path = gold_path(table)
        rows = df.count()
        if not DeltaTable.isDeltaTable(spark, path):
            df.write.format("delta").save(path)
            logger.log_run(rows_read=rows, rows_inserted=rows, status="SUCCESS")
            print(f"[GOLD] {table}: created with {rows} rows")
            return rows

        target = DeltaTable.forPath(spark, path)
        (
            target.alias("t")
            .merge(df.alias("s"), f"t.{key} = s.{key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            # A fact whose silver record has gone is removed; the dimension keeps the history.
            .whenNotMatchedBySourceDelete()
            .execute()
        )
        metrics = target.history(1).select("operationMetrics").first()["operationMetrics"]
        inserted = int(metrics.get("numTargetRowsInserted", 0))
        updated = int(metrics.get("numTargetRowsUpdated", 0))
        deleted = int(metrics.get("numTargetRowsDeleted", 0))
        logger.log_run(
            rows_read=rows, rows_inserted=inserted, rows_updated=updated, rows_deleted=deleted, status="SUCCESS"
        )
        print(f"[GOLD] {table}: inserted {inserted}, updated {updated}, deleted {deleted}")
        return inserted + updated + deleted
    except Exception as exc:
        logger.log_run(status="FAILED", error_code=type(exc).__name__, error_message=str(exc)[:2000])
        raise


# ---------------------------------------------------------------------------
# Facts
# ---------------------------------------------------------------------------


def build_fact_encounter(spark: SparkSession, run_id: str) -> int:
    if not silver_exists("silver_ehr_encounters"):
        return 0
    encounters = read_silver_current(spark, "silver_ehr_encounters")

    # Readmission: a patient admitted within 30 days of their own previous discharge. It depends on
    # neighbouring rows, so it is computed over the whole table rather than the changed slice; at
    # scale you would restrict this to the patients touched by the batch.
    by_patient = Window.partitionBy("patient_id").orderBy("admission_timestamp")
    with_previous = encounters.withColumn("_previous_discharge", F.lag("discharge_timestamp").over(by_patient))
    days_since = F.datediff(F.col("admission_timestamp"), F.col("_previous_discharge"))

    facts = with_previous.withColumn("days_since_previous_discharge", days_since).withColumn(
        "is_readmission_30d",
        F.when(F.col("_previous_discharge").isNull(), F.lit(False)).otherwise(
            (days_since >= 0) & (days_since <= READMISSION_WINDOW_DAYS)
        ),
    )
    facts = patient_sk_as_of(spark, facts, F.col("patient_id"), F.col("admission_timestamp"), "sql_ehr")

    out = facts.select(
        F.col("encounter_id"),
        F.col("patient_sk"),
        surrogate_key(F.col("provider_id")).alias("provider_sk"),
        surrogate_key(F.col("facility_id")).alias("facility_sk"),
        surrogate_key(F.col("department_id")).alias("department_sk"),
        date_key(F.col("admission_timestamp")).alias("admission_date_key"),
        date_key(F.col("discharge_timestamp")).alias("discharge_date_key"),
        F.col("encounter_type"),
        F.col("admission_timestamp"),
        F.col("discharge_timestamp"),
        F.round(
            (F.col("discharge_timestamp").cast("long") - F.col("admission_timestamp").cast("long")) / 3600.0, 2
        ).alias("length_of_stay_hours"),
        F.col("discharge_disposition"),
        F.col("is_readmission_30d"),
        F.col("days_since_previous_discharge"),
    )
    return merge_fact(spark, out, "fact_encounter", "encounter_id", run_id, "sql_ehr")


def build_fact_observation(spark: SparkSession, run_id: str) -> int:
    """Clinical measurements from both sources in one fact, tagged by source_system.

    EHR lab results carry an order and a result time, so turnaround is real. FHIR observations
    carry only an effective time, so their turnaround is NULL: the source cannot answer it.
    """
    frames = []
    if silver_exists("silver_ehr_lab_results"):
        labs = read_silver_current(spark, "silver_ehr_lab_results")
        labs = patient_sk_as_of(spark, labs, F.col("patient_id"), F.col("result_timestamp"), "sql_ehr")
        frames.append(
            labs.select(
                F.col("lab_result_id").alias("observation_id"),
                F.lit("sql_ehr").alias("source_system"),
                F.col("patient_sk"),
                F.col("encounter_id"),
                date_key(F.col("result_timestamp")).alias("observation_date_key"),
                F.col("result_timestamp").alias("observation_timestamp"),
                F.col("loinc_code"),
                F.col("test_name"),
                F.col("result_value"),
                F.col("result_unit"),
                F.col("abnormal_flag"),
                F.round(
                    (F.col("result_timestamp").cast("long") - F.col("order_timestamp").cast("long")) / 60.0, 1
                ).alias("turnaround_minutes"),
            )
        )
    if silver_exists("silver_fhir_observations"):
        obs = read_silver_current(spark, "silver_fhir_observations")
        obs = patient_sk_as_of(spark, obs, F.col("patient_id"), F.col("observation_timestamp"), "fhir_r4")
        frames.append(
            obs.select(
                F.col("observation_id"),
                F.lit("fhir_r4").alias("source_system"),
                F.col("patient_sk"),
                F.col("encounter_id"),
                date_key(F.col("observation_timestamp")).alias("observation_date_key"),
                F.col("observation_timestamp"),
                F.col("loinc_code"),
                F.col("test_name"),
                F.col("result_value"),
                F.col("result_unit"),
                F.lit(None).cast("string").alias("abnormal_flag"),
                # No order time in FHIR Observation: unanswerable, not zero.
                F.lit(None).cast("double").alias("turnaround_minutes"),
            )
        )
    if not frames:
        return 0
    combined = frames[0]
    for extra in frames[1:]:
        combined = combined.unionByName(extra)
    return merge_fact(spark, combined, "fact_observation", "observation_id", run_id, "clinical")


def build_fact_diagnosis(spark: SparkSession, run_id: str) -> int:
    if not silver_exists("silver_ehr_diagnoses"):
        return 0
    diagnoses = read_silver_current(spark, "silver_ehr_diagnoses")
    diagnoses = patient_sk_as_of(spark, diagnoses, F.col("patient_id"), F.col("diagnosis_timestamp"), "sql_ehr")
    out = diagnoses.select(
        F.col("diagnosis_id"),
        F.col("patient_sk"),
        F.col("encounter_id"),
        surrogate_key(F.col("icd10_code")).alias("diagnosis_sk"),
        date_key(F.col("diagnosis_timestamp")).alias("diagnosis_date_key"),
        F.col("diagnosis_timestamp"),
        F.col("diagnosis_type"),
    )
    return merge_fact(spark, out, "fact_diagnosis", "diagnosis_id", run_id, "sql_ehr")


def build_fact_medication_order(spark: SparkSession, run_id: str) -> int:
    if not silver_exists("silver_ehr_medications"):
        return 0
    medications = read_silver_current(spark, "silver_ehr_medications")
    medications = patient_sk_as_of(spark, medications, F.col("patient_id"), F.col("order_timestamp"), "sql_ehr")
    out = medications.select(
        F.col("medication_order_id"),
        F.col("patient_sk"),
        F.col("encounter_id"),
        surrogate_key(F.col("rxnorm_code")).alias("medication_sk"),
        date_key(F.col("order_timestamp")).alias("order_date_key"),
        F.col("order_timestamp"),
        F.col("dosage"),
        F.col("route"),
        F.col("frequency"),
        F.col("order_status"),
    )
    return merge_fact(spark, out, "fact_medication_order", "medication_order_id", run_id, "sql_ehr")


def build_fact_claim(spark: SparkSession, run_id: str) -> int:
    if not silver_exists("silver_claims"):
        return 0
    claims = read_silver_current(spark, "silver_claims")
    claims = patient_sk_as_of(spark, claims, F.col("patient_id"), F.col("service_date").cast("timestamp"), "sql_ehr")
    out = claims.select(
        F.col("claim_id"),
        F.col("patient_sk"),
        surrogate_key(F.col("facility_id")).alias("facility_sk"),
        date_key(F.col("service_date")).alias("service_date_key"),
        F.col("service_date"),
        F.col("claim_amount"),
        F.col("paid_amount"),
        (F.col("claim_amount") - F.col("paid_amount")).alias("unpaid_amount"),
        F.col("claim_status"),
        F.col("insurance_type"),
    )
    return merge_fact(spark, out, "fact_claim", "claim_id", run_id, "claims_csv")


def build_gold_facts(spark: SparkSession | None = None, run_id: str | None = None) -> int:
    spark = spark or get_spark_session("ClinicalFlow_Gold_Facts")
    run_id = run_id or f"run-{uuid.uuid4().hex[:10]}"
    if not os.path.isdir(os.path.join(gold_path("dim_patient"), "_delta_log")):
        print("dim_patient not built yet; run build_dimensions first")
        return 0

    print(f"STARTING GOLD FACTS (run {run_id})")
    total = build_fact_encounter(spark, run_id)
    total += build_fact_observation(spark, run_id)
    total += build_fact_diagnosis(spark, run_id)
    total += build_fact_medication_order(spark, run_id)
    total += build_fact_claim(spark, run_id)
    print(f"GOLD FACTS COMPLETE (run {run_id})")
    return total


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id")
    build_gold_facts(run_id=parser.parse_args().run_id)
