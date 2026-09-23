"""
Silver layer: FHIR R4 resources.

Flattens the raw FHIR JSON bronze landed into relational silver tables, incrementally.
The work happens in silver_engine; this module says which tables and provides the CLI.

    python -m databricks.silver.process_fhir_silver [--run-id ID]
"""

import argparse
import uuid

from pyspark.sql import SparkSession

from databricks.silver.silver_engine import process_specs
from databricks.silver.specs import FHIR_SPECS
from databricks.utilities.config import get_spark_session


def process_fhir_to_silver(spark: SparkSession | None = None, run_id: str | None = None) -> int:
    spark = spark or get_spark_session("ClinicalFlow_Silver_FHIR")
    run_id = run_id or f"run-{uuid.uuid4().hex[:10]}"
    print(f"STARTING SILVER FHIR PROCESSING (run {run_id})")
    rows = process_specs(spark, FHIR_SPECS, run_id)
    print(f"SILVER FHIR PROCESSING COMPLETE (run {run_id})")
    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id")
    process_fhir_to_silver(run_id=parser.parse_args().run_id)
