"""
Silver layer: SQL Server EHR tables (via CDC) and the external claims/facility feeds.

The work happens in silver_engine; this module says which tables and provides the CLI.

    python -m databricks.silver.process_relational_silver [--run-id ID]
"""

import argparse
import uuid

from pyspark.sql import SparkSession

from databricks.silver.silver_engine import process_specs
from databricks.silver.specs import EHR_SPECS, REFERENCE_SPECS
from databricks.utilities.config import get_spark_session


def process_relational_to_silver(spark: SparkSession | None = None, run_id: str | None = None) -> int:
    spark = spark or get_spark_session("ClinicalFlow_Silver_Relational")
    run_id = run_id or f"run-{uuid.uuid4().hex[:10]}"
    print(f"STARTING SILVER RELATIONAL PROCESSING (run {run_id})")
    rows = process_specs(spark, EHR_SPECS + REFERENCE_SPECS, run_id)
    print(f"SILVER RELATIONAL PROCESSING COMPLETE (run {run_id})")
    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id")
    process_relational_to_silver(run_id=parser.parse_args().run_id)
