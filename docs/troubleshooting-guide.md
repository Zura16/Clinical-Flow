# ClinicalFlow Troubleshooting & Recovery Guide

## Scenario 1: Malformed Input Data Violation
### Symptoms:
- Rejection rate alert triggered in `pipeline_run_audit`.
- High count of `PENDING` records in `quarantine_records`.

### Troubleshooting & Resolution Steps:
1. Query `quarantine_records` to identify failed rule and raw payload:
   ```sql
   SELECT failed_rule, error_message, raw_payload
   FROM clinicalflow_meta.dbo.quarantine_records
   WHERE resolution_status = 'PENDING';
   ```
2. If the data quality rule expression was over-restrictive (e.g. range bounds), update `data_quality_rule` in SQL Server metadata DB.
3. Replay the quarantine partition using `failure_simulation.py`:
   ```bash
   python databricks/utilities/failure_simulation.py
   ```
4. Confirm `resolution_status` updates to `REPLAYED` and verify zero record duplication in Gold tables.

## Scenario 2: Schema Drift Detection
### Symptoms:
- PySpark ingestion fails with `AnalysisException: Found duplicate / missing column`.

### Resolution Steps:
1. Review raw JSON / CSV payload changes.
2. Enable `mergeSchema` option in Delta Lake append calls if column additions are intentional:
   ```python
   df.write.format("delta").mode("append").option("mergeSchema", "true").save(target_path)
   ```
