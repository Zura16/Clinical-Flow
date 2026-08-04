# ClinicalFlow Pipeline Operations Runbook

## 1. Overview
This runbook provides step-by-step procedures for operating, backfilling, monitoring, and recovering ClinicalFlow pipelines.

## 2. Daily Pipeline Execution
The pipeline runs daily via Azure Data Factory schedule trigger `trg_daily_clinicalflow_ingestion`.

### Manual Triggering via CLI / Python:
```bash
python databricks/bronze/ingest_raw_data.py
python databricks/silver/process_fhir_silver.py
python databricks/silver/process_relational_silver.py
python databricks/gold/build_dimensions.py
python databricks/gold/build_facts.py
```

## 3. Backfill Procedure
To reprocess historical data for a specific date range:
1. Update `watermark_start` and `watermark_end` in `pipeline_config`.
2. Trigger `pl_metadata_driven_ingestion` with parameter `IsBackfill = true`.
3. Verify that zero duplicate rows are created in Gold using Delta MERGE.

## 4. Runbook SLA & Metrics Monitoring
- **Ingestion SLA**: Bronze landing within 30 minutes of extraction.
- **Data Quality SLA**: Rejection rate must remain below 5.0%.
- **Quarantine Review**: Alerts trigger when `quarantine_records` count increases by > 100 records in a single run.
