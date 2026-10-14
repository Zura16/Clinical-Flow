"""
pipeline_config is reconciled with the seed only when a definition differs: an unchanged seed
writes no new table version, an operator's active_flag is not drift, a changed definition is.
"""

import dataclasses

import pytest
from pyspark.sql import functions as F

from databricks.utilities import control
from databricks.utilities.config import get_spark_session


@pytest.fixture(scope="module")
def spark():
    return get_spark_session("Test_Pipeline_Config_Seed")


def table_version(spark) -> int:
    return spark.sql(f"DESCRIBE HISTORY delta.`{control.PIPELINE_CONFIG_PATH}`").selectExpr("max(version) v").first().v


def seeded(spark) -> None:
    control.clear_caches()
    control.ensure_pipeline_config(spark)


def row(spark, source_table: str):
    return (
        spark.read.format("delta")
        .load(control.PIPELINE_CONFIG_PATH)
        .filter(F.col("source_table") == source_table)
        .first()
    )


def test_seed_merges_only_on_definition_drift(spark, monkeypatch):
    seeded(spark)
    version = table_version(spark)

    # 1. Unchanged seed: no MERGE, so no new version.
    seeded(spark)
    assert table_version(spark) == version

    # 2. An operator turns a table off. That is not drift, and the next run leaves it off.
    from delta.tables import DeltaTable

    DeltaTable.forPath(spark, control.PIPELINE_CONFIG_PATH).update(
        "source_table = 'Practitioner'", {"active_flag": F.lit(False)}
    )
    version = table_version(spark)
    seeded(spark)
    assert table_version(spark) == version
    assert row(spark, "Practitioner")["active_flag"] is False

    # 3. A definition changes in the seed: it is merged in, and active_flag is still left alone.
    changed = [
        dataclasses.replace(c, data_quality_threshold=42.0) if c.source_table == "Practitioner" else c
        for c in control.PIPELINE_CONFIG_SEED
    ]
    monkeypatch.setattr(control, "PIPELINE_CONFIG_SEED", changed)
    seeded(spark)
    assert table_version(spark) == version + 1
    assert row(spark, "Practitioner")["data_quality_threshold"] == 42.0
    assert row(spark, "Practitioner")["active_flag"] is False
