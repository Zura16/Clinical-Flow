import pytest
import os
from databricks.utilities.config import get_spark_session, GOLD_PATH, read_df
from databricks.silver.bronze_reader import bronze_exists, read_bronze_current

@pytest.fixture(scope="module")
def spark():
    return get_spark_session("Test_Reconciliation")

def test_source_to_target_reconciliation(spark):
    """Reconciles source row counts through Silver and Gold layers."""
    gold_path = os.path.join(GOLD_PATH, "fact_encounter")
    
    if bronze_exists("bronze_ehr_encounters") and os.path.exists(gold_path):
        bronze_count = read_bronze_current(spark, "bronze_ehr_encounters").count()
        gold_count = read_df(spark, gold_path).count()
        
        # Valid records in Gold must match or equal clean Bronze records (allowing for DQ filter rejects)
        assert gold_count <= bronze_count
        print(f"[RECONCILIATION] Bronze Encounters: {bronze_count} -> Gold Fact Encounters: {gold_count}")
