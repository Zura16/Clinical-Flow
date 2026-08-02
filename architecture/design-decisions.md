# ClinicalFlow Architectural Design Decisions & Technical Rationale

This document outlines key technical and architectural decisions engineered into ClinicalFlow.

## 1. Why Medallion Architecture (Bronze -> Silver -> Gold)?
- **Bronze (Raw)**: Preserves exact immutable source records without transformation. Essential for compliance, auditing, and reprocessing when business logic changes.
- **Silver (Validated)**: Provides clean, flattened, normalized, and validated records. Encapsulates PII/PHI masking and data quality rule evaluation.
- **Gold (Analytics)**: Optimized for fast query performance via star-schema dimensions and facts, pre-computed surrogate keys, and SCD Type 2 history.

## 2. Metadata-Driven Pipeline Execution
Hardcoding individual ingestion pipelines for every source table creates massive maintenance overhead.
ClinicalFlow uses a central `pipeline_config` table read by Azure Data Factory and PySpark:
- Adding a new EHR table only requires inserting a configuration row into `pipeline_config`.
- Parameters specify `ingestion_type` (CDC, Watermark, Full), `watermark_column`, and `data_quality_threshold`.

## 3. Idempotency Architecture
Rerunning pipelines must produce identical end-states without creating duplicate rows or corrupting surrogate keys.
- **SHA-256 Record Hashes**: Computed at the Silver layer across key attributes.
- **Delta MERGE**: Used for upserting incoming records into Silver and Gold layers based on business keys and `record_hash`.
- **Surrogate Keys**: Derived deterministically or managed via SCD2 state matching.

## 4. Slowly Changing Dimension (SCD) Type 2 for `dim_patient`
Healthcare patients frequently change addresses and insurance coverage. Simply overwriting patient dimension rows (SCD Type 1) corrupts historical analytical reports (e.g. historical claims billing accuracy).
- `effective_start_date` & `effective_end_date`: Mark historical validity windows.
- `is_current`: Boolean flag indicating active version.
- When address or insurance changes: the current version row is expired (`is_current = false`), and a new version is inserted (`is_current = true`).

## 5. Quarantine & Data Quality Rule Engine
Dropping bad records silently is bad data engineering. ClinicalFlow routes non-compliant records to `quarantine_records` containing:
- Full raw JSON payload
- Pipeline run ID and detected timestamp
- Failed rule expression and severity level
- Resolution status (`PENDING`, `REPLAYED`, `IGNORED`)
