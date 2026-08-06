"""
ClinicalFlow PySpark Lakehouse Configuration & Utilities
Defines paths, Spark session instantiation, helper functions, and Delta Lake settings.
"""

import os
import hashlib
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, StructType, StructField, DoubleType, LongType, IntegerType, DateType, BooleanType

# Base Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LAKEHOUSE_PATH = os.path.join(BASE_DIR, "delta_lakehouse")

BRONZE_PATH = os.path.join(LAKEHOUSE_PATH, "bronze")
SILVER_PATH = os.path.join(LAKEHOUSE_PATH, "silver")
GOLD_PATH = os.path.join(LAKEHOUSE_PATH, "gold")
META_PATH = os.path.join(LAKEHOUSE_PATH, "metadata")

os.makedirs(BRONZE_PATH, exist_ok=True)
os.makedirs(SILVER_PATH, exist_ok=True)
os.makedirs(GOLD_PATH, exist_ok=True)
os.makedirs(META_PATH, exist_ok=True)

def get_spark_session(app_name="ClinicalFlow_Lakehouse"):
    """Instantiates a PySpark Session"""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )

# Hashing helper function for record deduplication and idempotency
def add_record_hash(df, columns_to_hash, output_col="record_hash"):
    """Generates SHA-256 hash across specified record columns"""
    cols = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns_to_hash]
    return df.withColumn(output_col, F.sha2(F.concat_ws("||", *cols), 256))

def save_df(df, path, mode="overwrite"):
    """Saves DataFrame using Delta Lake format with Parquet fallback for offline environments"""
    try:
        df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(path)
    except Exception:
        df.write.format("parquet").mode(mode).save(path)

def read_df(spark, path):
    """Reads DataFrame using Delta Lake format with Parquet fallback for offline environments"""
    try:
        return spark.read.format("delta").load(path)
    except Exception:
        return spark.read.format("parquet").load(path)

# PHI Masking helper functions
def mask_ssn(ssn_col):
    """Masks Social Security Number into SHA-256 digest"""
    return F.sha2(F.coalesce(ssn_col.cast("string"), F.lit("")), 256)

def mask_name(name_col):
    """Masks Patient First/Last Name for PII protection when required"""
    return F.concat(F.substring(name_col, 1, 1), F.lit("***"))
