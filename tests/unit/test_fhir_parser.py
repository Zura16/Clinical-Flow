import pytest

from databricks.utilities.config import add_record_hash, get_spark_session


@pytest.fixture(scope="module")
def spark():
    session = get_spark_session("Test_FHIR_Parser")
    yield session
    # Clean up session if needed


def test_fhir_record_hashing(spark):
    data = [("pat-001", "John", "Doe", "1980-01-01")]
    df = spark.createDataFrame(data, ["patient_id", "first_name", "last_name", "date_of_birth"])

    hashed_df = add_record_hash(df, ["patient_id", "first_name", "last_name", "date_of_birth"])
    assert "record_hash" in hashed_df.columns

    row = hashed_df.first()
    assert row["record_hash"] is not None
    assert len(row["record_hash"]) == 64  # SHA-256 length
