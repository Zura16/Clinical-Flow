"""
The data quality rule catalogue.

Rules live in a table (`metadata/data_quality_rule`), not in code: adding a check is a row, and
an operator can retire one by clearing active_flag without a deploy. Seeded from the list below,
which mirrors the INSERT in sql/quality/01_data_quality_framework.sql.

rule_type decides how rule_expression is read:

| rule_type   | rule_expression                        | Scope   |
|-------------|----------------------------------------|---------|
| NOT_NULL    | a boolean SQL expression               | row     |
| RANGE       | a boolean SQL expression               | row     |
| REGEX       | a boolean SQL expression (RLIKE)       | row     |
| UNIQUE      | comma-separated columns forming a grain| batch   |
| REFERENTIAL | target_table.target_column             | batch   |
| FRESHNESS   | maximum age in hours                   | dataset |

severity decides what happens to a failing row: CRITICAL and ERROR quarantine it and keep it out
of silver; WARNING records it and lets it through.

failure_threshold is the share of the batch (percent) allowed to fail before the run itself fails.
0 means a single violation fails the run. A threshold breach raises, which writes a FAILED audit
row and stops the table, rather than loading data nobody has judged.
"""

import os
from dataclasses import dataclass, asdict

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

from databricks.utilities.config import META_PATH

DATA_QUALITY_RULE_PATH = os.path.join(META_PATH, "data_quality_rule")

ROW_RULE_TYPES = {"NOT_NULL", "RANGE", "REGEX"}
RULE_TYPES = ROW_RULE_TYPES | {"UNIQUE", "REFERENTIAL", "FRESHNESS"}
SEVERITIES = {"CRITICAL", "ERROR", "WARNING"}
QUARANTINING_SEVERITIES = {"CRITICAL", "ERROR"}


@dataclass(frozen=True)
class QualityRule:
    dataset_name: str
    column_name: str
    rule_type: str
    rule_expression: str
    severity: str
    failure_threshold: float
    active_flag: bool = True

    @property
    def name(self) -> str:
        return f"{self.rule_type}:{self.column_name}"


DATA_QUALITY_RULE_SCHEMA = StructType([
    StructField("dataset_name", StringType(), False),
    StructField("column_name", StringType(), False),
    StructField("rule_type", StringType(), False),
    StructField("rule_expression", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("failure_threshold", DoubleType(), False),
    StructField("active_flag", BooleanType(), False),
])

# Thresholds are deliberately uneven: identity and grain are absolute (0), clinical plausibility
# gets a small allowance, and coding/reference checks warn rather than block.
DATA_QUALITY_RULE_SEED = [
    # --- patients (both EHR and FHIR land here)
    QualityRule("silver_patients", "patient_id", "NOT_NULL", "patient_id IS NOT NULL", "CRITICAL", 0.0),
    QualityRule("silver_patients", "patient_id", "UNIQUE", "patient_id", "CRITICAL", 0.0),
    QualityRule("silver_patients", "date_of_birth", "NOT_NULL", "date_of_birth IS NOT NULL", "ERROR", 5.0),
    QualityRule("silver_patients", "date_of_birth", "RANGE", "date_of_birth IS NULL OR date_of_birth <= current_date()", "ERROR", 0.0),

    # --- encounters
    QualityRule("silver_encounters", "encounter_id", "NOT_NULL", "encounter_id IS NOT NULL", "CRITICAL", 0.0),
    QualityRule("silver_encounters", "encounter_id", "UNIQUE", "encounter_id", "CRITICAL", 0.0),
    QualityRule("silver_encounters", "patient_id", "NOT_NULL", "patient_id IS NOT NULL", "CRITICAL", 0.0),
    QualityRule("silver_encounters", "discharge_timestamp", "RANGE",
                "discharge_timestamp IS NULL OR discharge_timestamp >= admission_timestamp", "ERROR", 1.0),
    # A patient can arrive after their encounter does, so this warns rather than blocks.
    QualityRule("silver_encounters", "patient_id", "REFERENTIAL", "silver_ehr_patients.patient_id", "WARNING", 5.0),

    # --- observations
    QualityRule("silver_observations", "observation_id", "NOT_NULL", "observation_id IS NOT NULL", "CRITICAL", 0.0),
    QualityRule("silver_observations", "observation_id", "UNIQUE", "observation_id", "CRITICAL", 0.0),
    QualityRule("silver_observations", "patient_id", "NOT_NULL", "patient_id IS NOT NULL", "CRITICAL", 0.0),
    QualityRule("silver_observations", "result_value", "RANGE",
                "result_value IS NULL OR (result_value >= -500 AND result_value <= 50000)", "ERROR", 2.0),
    QualityRule("silver_observations", "observation_timestamp", "RANGE",
                "observation_timestamp IS NULL OR observation_timestamp <= current_timestamp()", "ERROR", 0.5),
    # LOINC codes are NNNNN-N.
    QualityRule("silver_observations", "loinc_code", "REGEX",
                "loinc_code IS NULL OR loinc_code RLIKE '^[0-9]{1,5}-[0-9]$'", "WARNING", 5.0),

    # --- diagnoses
    QualityRule("silver_diagnoses", "diagnosis_id", "NOT_NULL", "diagnosis_id IS NOT NULL", "CRITICAL", 0.0),
    QualityRule("silver_diagnoses", "icd10_code", "NOT_NULL", "icd10_code IS NOT NULL", "ERROR", 0.0),
    # ICD-10-CM: a letter (not U), a digit, then a digit or A/B, optionally more.
    QualityRule("silver_diagnoses", "icd10_code", "REGEX",
                "icd10_code IS NULL OR icd10_code RLIKE '^[A-TV-Z][0-9][0-9AB]'", "WARNING", 5.0),

    # --- lab results
    QualityRule("silver_lab_results", "lab_result_id", "NOT_NULL", "lab_result_id IS NOT NULL", "CRITICAL", 0.0),
    QualityRule("silver_lab_results", "lab_result_id", "UNIQUE", "lab_result_id", "CRITICAL", 0.0),
    QualityRule("silver_lab_results", "result_value", "RANGE",
                "result_value IS NULL OR (result_value >= -500 AND result_value <= 50000)", "ERROR", 2.0),
    QualityRule("silver_lab_results", "result_timestamp", "RANGE",
                "result_timestamp IS NULL OR result_timestamp >= order_timestamp", "ERROR", 1.0),
    # The synthetic extract is historical, so this only catches a feed that has genuinely stopped.
    QualityRule("silver_lab_results", "result_timestamp", "FRESHNESS", "43800", "WARNING", 0.0),

    # --- claims
    QualityRule("silver_claims", "claim_id", "NOT_NULL", "claim_id IS NOT NULL", "CRITICAL", 0.0),
    QualityRule("silver_claims", "claim_id", "UNIQUE", "claim_id", "CRITICAL", 0.0),
    QualityRule("silver_claims", "claim_amount", "RANGE", "claim_amount >= 0", "ERROR", 1.0),
    QualityRule("silver_claims", "paid_amount", "RANGE",
                "paid_amount IS NULL OR claim_amount IS NULL OR paid_amount <= claim_amount", "WARNING", 5.0),
]

_SEEDED = False


def clear_caches() -> None:
    global _SEEDED
    _SEEDED = False


def ensure_rules(spark: SparkSession) -> None:
    """Create the rule table from the seed, refreshing rule definitions but not active_flag."""
    global _SEEDED
    if _SEEDED:
        return
    seed_df = spark.createDataFrame([asdict(r) for r in DATA_QUALITY_RULE_SEED], DATA_QUALITY_RULE_SCHEMA)
    if not DeltaTable.isDeltaTable(spark, DATA_QUALITY_RULE_PATH):
        seed_df.write.format("delta").save(DATA_QUALITY_RULE_PATH)
        _SEEDED = True
        return
    definition_columns = {c: f"s.{c}" for c in seed_df.columns if c != "active_flag"}
    (
        DeltaTable.forPath(spark, DATA_QUALITY_RULE_PATH).alias("t")
        .merge(seed_df.alias("s"),
               "t.dataset_name = s.dataset_name AND t.column_name = s.column_name AND t.rule_type = s.rule_type")
        .whenMatchedUpdate(set=definition_columns)
        .whenNotMatchedInsertAll()
        .execute()
    )
    _SEEDED = True


def load_rules(spark: SparkSession, dataset_name: str) -> list[QualityRule]:
    ensure_rules(spark)
    rows = (
        spark.read.format("delta").load(DATA_QUALITY_RULE_PATH)
        .filter((F.col("dataset_name") == dataset_name) & F.col("active_flag"))
        .orderBy("column_name", "rule_type")
        .collect()
    )
    rules = [QualityRule(**row.asDict()) for row in rows]
    for rule in rules:
        if rule.rule_type not in RULE_TYPES:
            raise ValueError(f"data_quality_rule {rule.dataset_name}.{rule.name}: unknown rule_type {rule.rule_type!r}")
        if rule.severity not in SEVERITIES:
            raise ValueError(f"data_quality_rule {rule.dataset_name}.{rule.name}: unknown severity {rule.severity!r}")
    return rules
