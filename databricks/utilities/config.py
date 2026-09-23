"""
ClinicalFlow PySpark Lakehouse Configuration & Utilities
Defines paths, Spark session instantiation, helper functions, and Delta Lake settings.
"""

import os
import hashlib
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, StructType, StructField, DoubleType, LongType, IntegerType, DateType, BooleanType

# Base Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# CLINICALFLOW_LAKEHOUSE lets tests point every layer at a throwaway directory.
# It must be set before this module is imported (tests/conftest.py does that).
# Spark packages fetched on session start: Delta comes from configure_spark_with_delta_pip,
# the SQL Server JDBC driver is added here so bronze can read CDC changes.
MSSQL_JDBC_PACKAGE = "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11"

LAKEHOUSE_PATH = os.environ.get("CLINICALFLOW_LAKEHOUSE", os.path.join(BASE_DIR, "delta_lakehouse"))

BRONZE_PATH = os.path.join(LAKEHOUSE_PATH, "bronze")
SILVER_PATH = os.path.join(LAKEHOUSE_PATH, "silver")
GOLD_PATH = os.path.join(LAKEHOUSE_PATH, "gold")
META_PATH = os.path.join(LAKEHOUSE_PATH, "metadata")

os.makedirs(BRONZE_PATH, exist_ok=True)
os.makedirs(SILVER_PATH, exist_ok=True)
os.makedirs(GOLD_PATH, exist_ok=True)
os.makedirs(META_PATH, exist_ok=True)

def get_spark_session(app_name="ClinicalFlow_Lakehouse"):
    """Instantiates a local PySpark session with Delta Lake enabled.

    configure_spark_with_delta_pip adds the delta-spark jar matching the installed pip package,
    so the JVM side and Python side can't drift apart.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        # Timestamps are stored and compared in UTC; without this, Spark renders them in the
        # machine's local zone and watermarks shift by the UTC offset.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        # The session zone governs Spark; user.timezone governs the JVM (and anything, such as a
        # JDBC driver, that materialises a zone-less value into a java type).
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder, extra_packages=[MSSQL_JDBC_PACKAGE]).getOrCreate()

# Hashing helper function for record deduplication and idempotency
def add_record_hash(df, columns_to_hash, output_col="record_hash"):
    """Generates SHA-256 hash across specified record columns"""
    cols = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns_to_hash]
    return df.withColumn(output_col, F.sha2(F.concat_ws("||", *cols), 256))

def save_df(df, path, mode="overwrite"):
    """Saves a DataFrame as a Delta table. Errors propagate; there is no format fallback."""
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(path)

def read_df(spark, path):
    """Reads a Delta table. Errors propagate; there is no format fallback."""
    return spark.read.format("delta").load(path)

# PHI Masking helper functions
def mask_ssn(ssn_col):
    """Masks Social Security Number into SHA-256 digest"""
    return F.sha2(F.coalesce(ssn_col.cast("string"), F.lit("")), 256)

def mask_name(name_col):
    """Masks Patient First/Last Name for PII protection when required"""
    return F.concat(F.substring(name_col, 1, 1), F.lit("***"))
