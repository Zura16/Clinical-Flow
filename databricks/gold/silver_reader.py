"""
Gold's view of silver: current records only.

Silver soft-deletes, so every gold read filters `_is_deleted`. Dimensions and facts are built
from what exists now; the deleted rows stay in silver (and their change history stays in bronze)
so the deletion remains auditable.
"""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from databricks.utilities.config import SILVER_PATH


def read_silver_current(spark: SparkSession, table: str) -> DataFrame:
    df = spark.read.format("delta").load(os.path.join(SILVER_PATH, table))
    if "_is_deleted" in df.columns:
        df = df.filter(~F.col("_is_deleted"))
    return df
