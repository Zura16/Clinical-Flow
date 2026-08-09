import pytest
from pyspark.sql import functions as F
from databricks.utilities.config import get_spark_session, add_record_hash

@pytest.fixture(scope="module")
def spark():
    return get_spark_session("Test_SCD2_Logic")

def test_scd2_hash_comparison(spark):
    initial_data = [("pat-100", "Main St", "Commercial")]
    df1 = spark.createDataFrame(initial_data, ["patient_id", "address_street", "insurance_type"])
    df1_hashed = add_record_hash(df1, ["patient_id", "address_street", "insurance_type"])
    
    updated_data = [("pat-100", "New Oak Way", "Commercial")]
    df2 = spark.createDataFrame(updated_data, ["patient_id", "address_street", "insurance_type"])
    df2_hashed = add_record_hash(df2, ["patient_id", "address_street", "insurance_type"])
    
    hash1 = df1_hashed.first()["record_hash"]
    hash2 = df2_hashed.first()["record_hash"]
    
    assert hash1 != hash2, "Record hashes must change when patient address changes to trigger SCD Type 2 row expiration."
