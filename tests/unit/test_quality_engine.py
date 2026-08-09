import pytest
from databricks.utilities.config import get_spark_session
from databricks.utilities.quality_engine import DataQualityEngine

@pytest.fixture(scope="module")
def spark():
    return get_spark_session("Test_Quality_Engine")

def test_data_quality_range_validation(spark):
    data = [
        ("obs-1", "pat-1", 100.0), # Valid
        ("obs-2", "pat-2", -9999.0) # Invalid: below -500
    ]
    df = spark.createDataFrame(data, ["observation_id", "patient_id", "result_value"])
    
    engine = DataQualityEngine(spark, "silver_observations", "test-run-1")
    valid_df, rejected_count = engine.validate(df, id_col="observation_id")
    
    assert valid_df.count() == 1
    assert rejected_count == 1
    assert valid_df.first()["observation_id"] == "obs-1"
