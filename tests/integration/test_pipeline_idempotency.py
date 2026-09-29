import os

import pytest

from databricks.bronze.ingest_raw_data import run_bronze_ingestion
from databricks.gold.build_dimensions import build_gold_dimensions
from databricks.gold.build_facts import build_gold_facts
from databricks.silver.process_fhir_silver import process_fhir_to_silver
from databricks.silver.process_relational_silver import process_relational_to_silver
from databricks.utilities import sqlserver as mssql
from databricks.utilities.config import GOLD_PATH, get_spark_session, read_df

# The EHR tables come from SQL Server, so the whole-pipeline test needs it running:
#     docker compose up -d sqlserver && python -m scripts.setup_source_db
pytestmark = pytest.mark.skipif(
    not mssql.is_available(),
    reason="SQL Server is not available, so bronze cannot ingest the EHR tables",
)


@pytest.fixture(scope="module")
def spark():
    return get_spark_session("Test_Pipeline_Idempotency")


def test_pipeline_rerun_idempotency(spark):
    """Verifies that running the end-to-end pipeline twice produces identical row counts in Gold."""
    # Run 1
    run_bronze_ingestion(spark, "test-idempotency-1")
    process_fhir_to_silver(spark, "test-idempotency-1")
    process_relational_to_silver(spark, "test-idempotency-1")
    build_gold_dimensions(spark, "test-idempotency-1")
    build_gold_facts(spark, "test-idempotency-1")

    fact_obs_path = os.path.join(GOLD_PATH, "fact_observation")
    count_run1 = read_df(spark, fact_obs_path).count()

    # Run 2 (Rerun identical pipeline)
    run_bronze_ingestion(spark, "test-idempotency-2")
    process_fhir_to_silver(spark, "test-idempotency-2")
    process_relational_to_silver(spark, "test-idempotency-2")
    build_gold_dimensions(spark, "test-idempotency-2")
    build_gold_facts(spark, "test-idempotency-2")

    count_run2 = read_df(spark, fact_obs_path).count()

    assert count_run1 == count_run2, f"Pipeline is not idempotent! Run 1 count: {count_run1}, Run 2 count: {count_run2}"
