"""
Gold dimensions.

dim_patient keeps history (SCD Type 2): a change closes the current version and opens a new one,
so a fact recorded last March still resolves to the patient as they were last March. Every other
dimension is Type 1 (current value only), because nothing here asks what a facility used to be called.

Every dimension carries the unknown member (-1) and a deterministic surrogate key, so a rebuild
never renumbers and a fact with a missing or late dimension still joins to something.

    python -m databricks.gold.build_dimensions [--run-id ID]
"""

import argparse
import os
import uuid

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from databricks.gold.keys import surrogate_key, with_unknown_member
from databricks.gold.silver_reader import read_silver_current, silver_exists
from databricks.utilities.config import GOLD_PATH, get_spark_session
from databricks.utilities.logger import PipelineLogger

# A natural key's FIRST version opens at the beginning of time, not at the moment the record was
# first seen. `source_updated_at` is when the row last changed, which for an unchanged record is
# often long after the events that reference it: starting there would send every earlier fact to
# the unknown member. Later versions start when the change actually happened.
BEGINNING_OF_TIME = "1900-01-01 00:00:00"


def gold_path(table: str) -> str:
    return os.path.join(GOLD_PATH, table)


def upsert_dimension(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    sk_column: str,
    run_id: str,
    source_name: str,
    unknown_overrides: dict | None = None,
) -> int:
    """Type 1 dimension: insert new members, update changed ones, keep the unknown member."""
    logger = PipelineLogger(spark, run_id, f"gold:{table}", source_name, "GOLD")
    try:
        incoming = with_unknown_member(df, spark, sk_column, unknown_overrides)
        path = gold_path(table)
        if not DeltaTable.isDeltaTable(spark, path):
            incoming.write.format("delta").save(path)
            rows = incoming.count()
            logger.log_run(rows_read=rows, rows_inserted=rows, status="SUCCESS")
            print(f"[GOLD] {table}: created with {rows} rows")
            return rows

        target = DeltaTable.forPath(spark, path)
        (
            target.alias("t")
            .merge(incoming.alias("s"), f"t.{sk_column} = s.{sk_column}")
            .whenMatchedUpdateAll(condition="t.record_hash <> s.record_hash")
            .whenNotMatchedInsertAll()
            .execute()
        )
        metrics = target.history(1).select("operationMetrics").first()["operationMetrics"]
        inserted, updated = int(metrics.get("numTargetRowsInserted", 0)), int(metrics.get("numTargetRowsUpdated", 0))
        logger.log_run(rows_read=incoming.count(), rows_inserted=inserted, rows_updated=updated, status="SUCCESS")
        print(f"[GOLD] {table}: inserted {inserted}, updated {updated}")
        return inserted + updated
    except Exception as exc:
        logger.log_run(status="FAILED", error_code=type(exc).__name__, error_message=str(exc)[:2000])
        raise


# ---------------------------------------------------------------------------
# dim_patient: SCD Type 2
# ---------------------------------------------------------------------------


def patient_source(spark: SparkSession) -> DataFrame | None:
    """EHR and FHIR patients, labelled by source system.

    They are not merged: the two systems share no identifier, so one human present in both is two
    rows here. Treating them as one person needs identity resolution, which this project does not
    do; source_system at least stops the row count from implying otherwise.
    """
    frames = []
    if silver_exists("silver_ehr_patients"):
        frames.append(
            read_silver_current(spark, "silver_ehr_patients", include_deleted=True).withColumn(
                "source_system", F.lit("sql_ehr")
            )
        )
    if silver_exists("silver_fhir_patients"):
        frames.append(
            read_silver_current(spark, "silver_fhir_patients", include_deleted=True)
            .withColumn("source_system", F.lit("fhir_r4"))
            .withColumn("ssn_hash", F.lit(None).cast("string"))
            .withColumn("phone_number", F.lit(None).cast("string"))
        )
    if not frames:
        return None
    combined = frames[0]
    for extra in frames[1:]:
        combined = combined.unionByName(extra, allowMissingColumns=True)
    return combined


def patient_versions(df: DataFrame) -> DataFrame:
    """Shape silver patients into dimension versions (without the key: see with_version_key)."""
    effective_start = F.coalesce(F.col("source_updated_at"), F.col("_updated_at"))
    return df.select(
        F.col("source_system"),
        F.col("patient_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("date_of_birth"),
        F.col("gender"),
        F.col("address_street"),
        F.col("city"),
        F.col("state"),
        F.col("postal_code"),
        F.col("phone_number"),
        F.col("insurance_type"),
        effective_start.alias("effective_start_date"),
        F.lit(None).cast("timestamp").alias("effective_end_date"),
        F.lit(True).alias("is_current"),
        F.col("_is_deleted").alias("is_deleted"),
        # The version's fingerprint. Deletion is a change like any other, so it opens a version.
        F.sha2(F.concat_ws("||", F.col("record_hash"), F.col("_is_deleted").cast("string")), 256).alias("record_hash"),
    )


def with_version_key(df: DataFrame) -> DataFrame:
    """The version's surrogate key, derived after effective_start_date is final."""
    return df.withColumn(
        "patient_sk",
        surrogate_key(F.col("source_system"), F.col("patient_id"), F.col("effective_start_date")),
    )


def build_dim_patient(spark: SparkSession, run_id: str, source: DataFrame | None = None) -> int:
    """Build dim_patient from silver, or from `source` when a caller supplies one (tests)."""
    logger = PipelineLogger(spark, run_id, "gold:dim_patient", "gold_warehouse", "GOLD")
    try:
        source = patient_source(spark) if source is None else source
        if source is None:
            logger.log_run(status="SUCCESS", error_message="no silver patients yet")
            return 0

        incoming = patient_versions(source)
        path = gold_path("dim_patient")

        if not DeltaTable.isDeltaTable(spark, path):
            # Every row here is its key's first version.
            first_versions = with_version_key(
                incoming.withColumn("effective_start_date", F.lit(BEGINNING_OF_TIME).cast("timestamp"))
            )
            initial = with_unknown_member(
                first_versions,
                spark,
                "patient_sk",
                {"is_current": True, "is_deleted": False},
            )
            initial.write.format("delta").save(path)
            rows = initial.count()
            logger.log_run(rows_read=rows, rows_inserted=rows, status="SUCCESS")
            print(f"[GOLD] dim_patient: created with {rows} versions")
            return rows

        target = DeltaTable.forPath(spark, path)
        current = (
            target.toDF()
            .filter("is_current AND patient_sk <> -1")
            .select(
                F.col("source_system").alias("c_source_system"),
                F.col("patient_id").alias("c_patient_id"),
                F.col("record_hash").alias("c_record_hash"),
            )
        )

        # A version is needed when the natural key is new, or when its content changed.
        changed = (
            incoming.join(
                current,
                (incoming["source_system"] == current["c_source_system"])
                & (incoming["patient_id"] == current["c_patient_id"]),
                "left_outer",
            )
            .filter(F.col("c_patient_id").isNull() | (F.col("record_hash") != F.col("c_record_hash")))
            # A key seen for the first time opens at the beginning of time; a change to a key
            # already present opens when the change happened.
            .withColumn(
                "effective_start_date",
                F.when(F.col("c_patient_id").isNull(), F.lit(BEGINNING_OF_TIME).cast("timestamp")).otherwise(
                    F.col("effective_start_date")
                ),
            )
            .select(incoming.columns)
        ).transform(with_version_key)
        # Sever the lineage before touching the table. `changed` is derived from dim_patient, and
        # the expire step below rewrites it: a recomputation would then see no current version for
        # these keys, treat them as brand new, restart them at the beginning of time, and re-derive
        # the key of the row just expired — so the insert would match and silently do nothing.
        changed = changed.localCheckpoint(eager=True)
        changed_count = changed.count()
        if changed_count == 0:
            logger.log_run(rows_read=incoming.count(), status="SUCCESS")
            print("[GOLD] dim_patient: no changes")
            return 0

        # 1. Close the version each change supersedes. Its end is the new version's start, so the
        #    two are contiguous and a point-in-time join cannot fall between them.
        (
            target.alias("t")
            .merge(
                changed.alias("s"), "t.source_system = s.source_system AND t.patient_id = s.patient_id AND t.is_current"
            )
            .whenMatchedUpdate(
                set={
                    "is_current": F.lit(False),
                    "effective_end_date": F.col("s.effective_start_date"),
                }
            )
            .execute()
        )

        # 2. Open the new versions. Merging on patient_sk makes this idempotent: the same version
        #    content re-derives the same key and is not inserted twice.
        inserter = DeltaTable.forPath(spark, path)
        (
            inserter.alias("t")
            .merge(changed.alias("s"), "t.patient_sk = s.patient_sk")
            .whenNotMatchedInsertAll()
            .execute()
        )
        metrics = inserter.history(1).select("operationMetrics").first()["operationMetrics"]
        inserted = int(metrics.get("numTargetRowsInserted", 0))
        if inserted != changed_count:
            raise RuntimeError(
                f"dim_patient: {changed_count} changed keys but {inserted} versions inserted; "
                "every change must open exactly one version"
            )

        logger.log_run(rows_read=incoming.count(), rows_inserted=inserted, rows_updated=changed_count, status="SUCCESS")
        print(f"[GOLD] dim_patient: {inserted} new versions")
        return inserted
    except Exception as exc:
        logger.log_run(status="FAILED", error_code=type(exc).__name__, error_message=str(exc)[:2000])
        raise


# ---------------------------------------------------------------------------
# Type 1 dimensions
# ---------------------------------------------------------------------------


def build_dim_provider(spark: SparkSession, run_id: str) -> int:
    if not silver_exists("silver_ehr_providers"):
        return 0
    df = read_silver_current(spark, "silver_ehr_providers").select(
        surrogate_key(F.col("provider_id")).alias("provider_sk"),
        "provider_id",
        "npi",
        "first_name",
        "last_name",
        "specialty",
        "department_id",
        "facility_id",
        "record_hash",
    )
    return upsert_dimension(spark, df, "dim_provider", "provider_sk", run_id, "sql_ehr")


def build_dim_facility(spark: SparkSession, run_id: str) -> int:
    if not silver_exists("silver_facilities"):
        return 0
    df = read_silver_current(spark, "silver_facilities").select(
        surrogate_key(F.col("facility_id")).alias("facility_sk"),
        "facility_id",
        "facility_name",
        "facility_type",
        "address",
        "city",
        "state",
        "postal_code",
        "record_hash",
    )
    return upsert_dimension(spark, df, "dim_facility", "facility_sk", run_id, "claims_csv")


def build_dim_diagnosis(spark: SparkSession, run_id: str) -> int:
    """One row per ICD-10 code seen in the data. The source ships no code master, so the dimension
    is derived from use and a code's description is whatever the source last called it."""
    if not silver_exists("silver_ehr_diagnoses"):
        return 0
    codes = (
        read_silver_current(spark, "silver_ehr_diagnoses")
        .filter(F.col("icd10_code").isNotNull())
        .groupBy("icd10_code")
        .agg(F.max_by("diagnosis_description", "source_updated_at").alias("diagnosis_description"))
    )
    df = codes.select(
        surrogate_key(F.col("icd10_code")).alias("diagnosis_sk"),
        F.col("icd10_code"),
        F.col("diagnosis_description"),
        F.lit("ICD-10-CM").alias("code_system"),
        F.sha2(F.concat_ws("||", "icd10_code", "diagnosis_description"), 256).alias("record_hash"),
    )
    return upsert_dimension(spark, df, "dim_diagnosis", "diagnosis_sk", run_id, "sql_ehr")


def build_dim_medication(spark: SparkSession, run_id: str) -> int:
    if not silver_exists("silver_ehr_medications"):
        return 0
    codes = (
        read_silver_current(spark, "silver_ehr_medications")
        .filter(F.col("rxnorm_code").isNotNull())
        .groupBy("rxnorm_code")
        .agg(F.max_by("medication_name", "source_updated_at").alias("medication_name"))
    )
    df = codes.select(
        surrogate_key(F.col("rxnorm_code")).alias("medication_sk"),
        F.col("rxnorm_code"),
        F.col("medication_name"),
        F.lit("RxNorm").alias("code_system"),
        F.sha2(F.concat_ws("||", "rxnorm_code", "medication_name"), 256).alias("record_hash"),
    )
    return upsert_dimension(spark, df, "dim_medication", "medication_sk", run_id, "sql_ehr")


def build_dim_department(spark: SparkSession, run_id: str) -> int:
    """Departments have no source table: the id is all the EHR carries. The dimension exists so
    facts have something to point at; department_name stays NULL rather than being invented."""
    if not silver_exists("silver_ehr_encounters"):
        return 0
    ids = (
        read_silver_current(spark, "silver_ehr_encounters")
        .filter(F.col("department_id").isNotNull())
        .select("department_id")
        .distinct()
    )
    df = ids.select(
        surrogate_key(F.col("department_id")).alias("department_sk"),
        F.col("department_id"),
        F.lit(None).cast("string").alias("department_name"),
        F.sha2(F.col("department_id"), 256).alias("record_hash"),
    )
    return upsert_dimension(spark, df, "dim_department", "department_sk", run_id, "sql_ehr")


def build_dim_date(spark: SparkSession) -> int:
    path = gold_path("dim_date")
    if DeltaTable.isDeltaTable(spark, path):
        return 0
    dates = spark.sql("""
        SELECT
            CAST(DATE_FORMAT(d, 'yyyyMMdd') AS INT) AS date_key,
            d AS full_date,
            DAYOFWEEK(d) AS day_of_week,
            DATE_FORMAT(d, 'EEEE') AS day_name,
            DAYOFMONTH(d) AS day_of_month,
            DAYOFYEAR(d) AS day_of_year,
            WEEKOFYEAR(d) AS week_of_year,
            MONTH(d) AS month_number,
            DATE_FORMAT(d, 'MMMM') AS month_name,
            QUARTER(d) AS quarter,
            YEAR(d) AS year,
            CASE WHEN DAYOFWEEK(d) IN (1, 7) THEN true ELSE false END AS is_weekend
        FROM (SELECT EXPLODE(SEQUENCE(TO_DATE('2020-01-01'), TO_DATE('2030-12-31'), INTERVAL 1 DAY)) AS d)
    """)
    with_unknown_member(dates, spark, "date_key").write.format("delta").save(path)
    rows = dates.count() + 1
    print(f"[GOLD] dim_date: created with {rows} rows")
    return rows


def build_gold_dimensions(spark: SparkSession | None = None, run_id: str | None = None) -> int:
    spark = spark or get_spark_session("ClinicalFlow_Gold_Dimensions")
    run_id = run_id or f"run-{uuid.uuid4().hex[:10]}"
    print(f"STARTING GOLD DIMENSIONS (run {run_id})")
    total = build_dim_date(spark)
    total += build_dim_patient(spark, run_id)
    total += build_dim_provider(spark, run_id)
    total += build_dim_facility(spark, run_id)
    total += build_dim_diagnosis(spark, run_id)
    total += build_dim_medication(spark, run_id)
    total += build_dim_department(spark, run_id)
    print(f"GOLD DIMENSIONS COMPLETE (run {run_id})")
    return total


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id")
    build_gold_dimensions(run_id=parser.parse_args().run_id)
