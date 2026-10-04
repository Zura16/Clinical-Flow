"""
FileIncremental bronze loads against a throwaway directory of FHIR bundles: a file is read once,
every row of a new file lands whatever its timestamps say, and an already-ingested file is not
opened again.
"""

import json
import os

import pytest
from pyspark.sql import functions as F

from databricks.bronze.ingest_raw_data import ingest_table
from databricks.silver.bronze_reader import current_state
from databricks.utilities.config import BRONZE_PATH, get_spark_session
from databricks.utilities.control import SourceConfig
from databricks.utilities.file_log import FILE_INGEST_LOG_PATH, files_to_ingest
from databricks.utilities.logger import AUDIT_TABLE_PATH


@pytest.fixture(scope="module")
def spark():
    return get_spark_session("Test_Bronze_File_Incremental")


@pytest.fixture(scope="module")
def source_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("fhir_source")


@pytest.fixture(scope="module")
def cfg(source_dir):
    return SourceConfig(
        "fhir_r4",
        "Observation",
        "bronze_test_fhir_files",
        "FileIncremental",
        "meta_lastUpdated",
        "resource_id",
        str(source_dir / "*.json"),
    )


def observation(resource_id: str, last_updated: str, value: float) -> dict:
    return {
        "resourceType": "Observation",
        "id": resource_id,
        "meta": {"lastUpdated": last_updated},
        "valueQuantity": {"value": value},
    }


def write_bundle(path, *resources: dict) -> None:
    path.write_text(json.dumps({"resourceType": "Bundle", "entry": [{"resource": r} for r in resources]}))


def landed_by_run(spark, cfg) -> dict[str, int]:
    df = spark.read.format("delta").load(os.path.join(BRONZE_PATH, cfg.destination_table))
    return {r["_pipeline_run_id"]: r["count"] for r in df.groupBy("_pipeline_run_id").count().collect()}


def current_values(spark, cfg) -> dict[str, float]:
    bronze = spark.read.format("delta").load(os.path.join(BRONZE_PATH, cfg.destination_table))
    return {
        r["resource_id"]: json.loads(r["resource_json"])["valueQuantity"]["value"]
        for r in current_state(bronze, cfg).collect()
    }


def test_file_incremental_lifecycle(spark, cfg, source_dir):
    bundle_a = source_dir / "bundle_a.json"
    bundle_b = source_dir / "bundle_b.json"
    bundle_c = source_dir / "bundle_c.json"

    # 1. First load: only this table's resource type lands; the Patient in the bundle is not ours.
    write_bundle(
        bundle_a,
        observation("o1", "2024-06-01T00:00:00Z", 1.0),
        observation("o2", "2024-06-02T00:00:00Z", 2.0),
        {"resourceType": "Patient", "id": "p1", "meta": {"lastUpdated": "2024-06-01T00:00:00Z"}},
    )
    assert ingest_table(spark, cfg, "run-1") == 2

    # 2. A late export: every timestamp in it is older than anything already landed. A row-level
    #    watermark would drop all of it silently. Both rows land. The stale copy of o1 is kept in
    #    bronze as history but does not become o1's current state; o3 is new and appears.
    write_bundle(
        bundle_b,
        observation("o3", "2023-01-01T00:00:00Z", 3.0),
        observation("o1", "2024-01-01T00:00:00Z", -1.0),
    )
    assert ingest_table(spark, cfg, "run-2") == 2
    assert current_values(spark, cfg) == {"o1": 1.0, "o2": 2.0, "o3": 3.0}

    # 3. Nothing new: an ingested file is not opened at all. Proven by replacing bundle_a with
    #    same-size junk and restoring its mtime: if it were read, parsing would fail the run.
    original = bundle_a.read_bytes()
    stat = bundle_a.stat()
    bundle_a.write_bytes(b" " * len(original))
    os.utime(bundle_a, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    assert ingest_table(spark, cfg, "run-3") == 0
    bundle_a.write_bytes(original)
    os.utime(bundle_a, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    assert "run-3" not in landed_by_run(spark, cfg)

    # 4. A file rewritten in place (new size or mtime) is read again, all of it: o2 is updated,
    #    o1 re-lands unchanged, which current-state resolution absorbs.
    write_bundle(
        bundle_a,
        observation("o1", "2024-06-01T00:00:00Z", 1.0),
        observation("o2", "2024-07-01T00:00:00Z", 20.0),
    )
    assert ingest_table(spark, cfg, "run-4") == 2
    assert current_values(spark, cfg) == {"o1": 1.0, "o2": 20.0, "o3": 3.0}

    # 5. An unparseable file fails the run loudly and is not counted as ingested...
    bundle_c.write_text("{not json")
    with pytest.raises(ValueError, match="could not be parsed"):
        ingest_table(spark, cfg, "run-5")
    failed = (
        spark.read.format("delta")
        .load(AUDIT_TABLE_PATH)
        .filter((F.col("pipeline_run_id") == "run-5") & (F.col("pipeline_name") == f"bronze:{cfg.destination_table}"))
        .select("execution_status")
        .collect()
    )
    assert [r["execution_status"] for r in failed] == ["FAILED"]

    # ...so once it is fixed, retrying the run reads it, and a second retry is skipped.
    write_bundle(bundle_c, observation("o4", "2024-08-01T00:00:00Z", 4.0))
    assert ingest_table(spark, cfg, "run-5") == 1
    assert ingest_table(spark, cfg, "run-5") == 1
    assert landed_by_run(spark, cfg) == {"run-1": 2, "run-2": 2, "run-4": 2, "run-5": 1}
    assert current_values(spark, cfg) == {"o1": 1.0, "o2": 20.0, "o3": 3.0, "o4": 4.0}

    # 6. The file log: unique on its grain (the retry of run-5 did not duplicate its entry), and
    #    every file now present is committed for this table exactly as it is on disk.
    log = (
        spark.read.format("delta")
        .load(FILE_INGEST_LOG_PATH)
        .filter(F.col("destination_table") == cfg.destination_table)
    )
    grain = ["file_path", "file_size", "file_modification_time", "pipeline_run_id"]
    assert log.count() == log.select(*grain).distinct().count()
    assert {(r["file_path"].rsplit("/", 1)[-1], r["pipeline_run_id"]) for r in log.collect()} == {
        ("bundle_a.json", "run-1"),
        ("bundle_b.json", "run-2"),
        ("bundle_a.json", "run-4"),
        ("bundle_c.json", "run-5"),
    }
    pending, total = files_to_ingest(spark, cfg.source_location, cfg.destination_table)
    assert (pending, total) == ([], 3)
