
# ClinicalFlow Pipeline Operations Runbook

## 1. Overview
This runbook provides step-by-step procedures for operating, backfilling, monitoring, and recovering ClinicalFlow pipelines.

## 2. Daily Pipeline Execution
The pipeline runs daily via Azure Data Factory schedule trigger `trg_daily_clinicalflow_ingestion`.

### Manual Triggering via CLI / Python:
```bash
python -m databricks.bronze.ingest_raw_data
python -m databricks.silver.process_fhir_silver
python -m databricks.silver.process_relational_silver
python -m databricks.gold.build_dimensions
python -m databricks.gold.build_facts
```

## 3. Restarting a Failed Bronze Run
Every bronze attempt writes a row per table to `pipeline_run_audit` (`SUCCESS`, `FAILED` or `SKIPPED`).

1. Find the failure: `execution_status = 'FAILED'` rows for the run give the table, `error_code` and `error_message`.
2. Fix the cause (source file, config row, code).
3. Rerun **with the same run ID**:
   ```bash
   python -m databricks.bronze.ingest_raw_data --run-id <run_id>            # whole run
   python -m databricks.bronze.ingest_raw_data --run-id <run_id> --source sql_ehr --table patients   # one table
   ```
   Tables the run already landed are skipped (`SKIPPED`), and their partitions are left untouched. Failed tables resume from their current watermark, which a failed attempt never advances. Nothing is landed twice.
4. Rerun silver and gold. They read current state from bronze.

Do **not** edit `watermark_state` by hand to "re-pull" data. Once a run has succeeded, its bronze partition is the only copy of those source rows as they were. Re-extracting would land whatever the source holds today, not what it held then.

To disable a source, set `active_flag = false` on its `pipeline_config` row. Reseeding never overwrites an existing row.

Date-range backfill from the source *(planned)*.

## 4. Runbook SLA & Metrics Monitoring
- **Ingestion SLA**: Bronze landing within 30 minutes of extraction.
- **Data Quality SLA**: Rejection rate must remain below 5.0%.
- **Quarantine Review**: Alerts trigger when `quarantine_records` count increases by > 100 records in a single run.
