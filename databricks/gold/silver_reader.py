"""
Gold's view of silver: current records only, unless a caller says otherwise.

Silver soft-deletes, so every gold read filters `_is_deleted` by default. Facts are built from what
exists now; the deleted rows stay in silver (and their change history in bronze) so the deletion
remains auditable. dim_patient asks for them deliberately, because a deletion is a change worth
keeping a version for.
"""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from databricks.utilities.config import SILVER_PATH


def silver_exists(table: str) -> bool:
    return os.path.isdir(os.path.join(SILVER_PATH, table, "_delta_log"))


def read_silver_current(spark: SparkSession, table: str, include_deleted: bool = False) -> DataFrame:
    df = spark.read.format("delta").load(os.path.join(SILVER_PATH, table))
    if not include_deleted and "_is_deleted" in df.columns:
        df = df.filter(~F.col("_is_deleted"))
    return df
