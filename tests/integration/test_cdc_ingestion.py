"""
CDC ingestion against the local SQL Server container.

Skipped when SQL Server is not reachable, so the suite still runs without Docker:
    docker compose up -d sqlserver && python -m scripts.setup_source_db
"""

import os

import pytest

from databricks.bronze.ingest_raw_data import CDC_DELETE, CDC_INSERT, CDC_SNAPSHOT, CDC_UPDATE_AFTER, ingest_table
from databricks.silver.bronze_reader import current_state
from databricks.utilities import sqlserver as mssql
from databricks.utilities.config import BRONZE_PATH, get_spark_session
from databricks.utilities.control import SourceConfig, get_watermark
from scripts.simulate_source_changes import apply_changes, wait_for_capture

pytestmark = pytest.mark.skipif(
    not mssql.is_available() or not mssql.scalar("SELECT DB_ID('ehr_source')", database="master"),
    reason="SQL Server with the ehr_source database is not available",
)


@pytest.fixture(scope="module")
def spark():
    return get_spark_session("Test_CDC_Ingestion")


@pytest.fixture(scope="module")
def cfg():
    return SourceConfig("sql_ehr", "patients", "bronze_cdc_test_patients", "CDC",
                        "_cdc_lsn", "patient_id", "dbo.patients")


def bronze(spark, cfg):
    return spark.read.format("delta").load(os.path.join(BRONZE_PATH, cfg.destination_table))


def test_cdc_snapshot_then_changes(spark, cfg):
    source_rows = mssql.scalar("SELECT COUNT(*) FROM dbo.patients")

    # 1. First load has no position to resume from: it snapshots the table.
    assert ingest_table(spark, cfg, "cdc-run-a") == source_rows
    assert bronze(spark, cfg).filter(f"_cdc_operation = {CDC_SNAPSHOT}").count() == source_rows
    watermark_after_snapshot = get_watermark(spark, cfg)
    assert len(watermark_after_snapshot) == mssql.LSN_HEX_LENGTH

    # 2. With nothing changed, the next run lands nothing.
    assert ingest_table(spark, cfg, "cdc-run-b") == 0

    # 3. Change the source: 3 updates, 1 insert, 1 delete (5 patient-level changes).
    changed = apply_changes(patients=3)
    wait_for_capture()
    assert ingest_table(spark, cfg, "cdc-run-c") == 5
    assert get_watermark(spark, cfg) > watermark_after_snapshot

    landed = bronze(spark, cfg).filter("_pipeline_run_id = 'cdc-run-c'")
    operations = {r["_cdc_operation"]: r["count"] for r in landed.groupBy("_cdc_operation").count().collect()}
    assert operations == {CDC_UPDATE_AFTER: 3, CDC_INSERT: 1, CDC_DELETE: 1}

    # 4. Current state matches the source exactly: the insert is present, the delete is gone,
    #    and the updates show their new values.
    current = current_state(bronze(spark, cfg), cfg)
    assert current.count() == mssql.scalar("SELECT COUNT(*) FROM dbo.patients")
    assert current.filter(f"patient_id = '{changed['deleted']}'").count() == 0
    assert current.filter(f"patient_id = '{changed['inserted']}'").count() == 1
    for patient_id in changed["updated"]:
        address = current.filter(f"patient_id = '{patient_id}'").first()["address_street"]
        assert address.startswith("Moved "), f"{patient_id} kept its old address {address!r}"

    # 5. The delete is still in bronze as evidence, even though current state drops it.
    assert bronze(spark, cfg).filter(
        f"patient_id = '{changed['deleted']}' AND _cdc_operation = {CDC_DELETE}").count() == 1
