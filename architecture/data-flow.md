# ClinicalFlow Architecture & Data Flow Walkthrough

ClinicalFlow is built around the **Medallion Architecture (Bronze -> Silver -> Gold)**, powered by **Azure Data Factory (ADF)** for metadata-driven orchestration, **PySpark / Delta Lake** for scalable distributed processing, and **SQL Server Data Warehouse** for star-schema analytics.

```
+-----------------------------------------------------------------------------------+
|                                  DATA SOURCES                                     |
| Synthea FHIR R4 JSON        SQL Server EHR DB (CDC)         Claims & Facility CSV |
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                             AZURE DATA FACTORY (ADF)                              |
|                       Reads pipeline_config metadata table                        |
|                       Dynamic Parameterization & Scheduling                       |
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                                BRONZE LAYER (RAW)                                 |
|               Immutable landing zone stored in Delta Lake format                  |
|               Includes raw payloads, ingestion timestamps, and run IDs            |
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                               SILVER LAYER (VALIDATED)                            |
|               FHIR R4 JSON flattening & code normalization (LOINC/ICD10)           |
|               SHA-256 Record Hashing for Deduplication & Idempotency               |
|               Data Quality Engine evaluates rules (NOT NULL, RANGE, REGEX)        |
|               Invalid records routed to quarantine_records table                  |
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                                GOLD LAYER (ANALYTICS)                             |
|               Star Schema Dimensional Model (dim_patient, dim_date, etc.)         |
|               SCD Type 2 Change Tracking for dim_patient (Address & Insurance)   |
|               Late-Arriving Dimension Handling (Surrogate Key = -1)               |
|               Fact Tables (fact_encounter, fact_observation, fact_claim)          |
+-----------------------------------------------------------------------------------+
                                          |
                        +-----------------+-----------------+
                        |                                   |
                        v                                   v
+---------------------------------------+ +---------------------------------------+
|  DATA QUALITY & PIPELINE OBSERVABILITY| |     CLINICAL OPERATIONS ANALYTICS     |
|  - Rejection Rates & Quarantine Logs  | |  - Department Encounters & Avg LOS    |
|  - Pipeline Execution Audit & Latency | |  - 30-Day Readmission Analytics       |
+---------------------------------------+ +---------------------------------------+
```

## Layer Specifications

### 1. Bronze Layer (Raw Ingestion)
- **Format**: Delta Lake / ADLS Gen2
- **Retention**: Immutable historical raw payload
- **Metadata**: Injected `ingestion_timestamp` and `pipeline_run_id`

### 2. Silver Layer (Cleaned & Validated Detail)
- **FHIR Processing**: Flattens nested JSON structures into tabular schema.
- **Deduplication**: Computes SHA-256 `record_hash` across immutable business key attributes.
- **Data Quality**: Evaluates rule expressions from `data_quality_rule`.
- **Quarantine**: Bad records (e.g. out-of-range lab results) are written to `quarantine_records` with error context instead of being dropped.

### 3. Gold Layer (Dimensional Warehouse)
- **Schema**: Star Schema.
- **Dimensions**: `dim_patient` (SCD Type 2), `dim_provider`, `dim_facility`, `dim_diagnosis`, `dim_medication`, `dim_date`, `dim_department`.
- **Facts**: `fact_encounter`, `fact_observation`, `fact_medication_order`, `fact_diagnosis`, `fact_claim`.
- **Idempotency**: All MERGE operations use business keys and record hashes to ensure zero row duplication on pipeline reruns.
