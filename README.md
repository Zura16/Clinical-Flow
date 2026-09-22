# ClinicalFlow: A Fault-Tolerant Epic/FHIR Healthcare Lakehouse

![ClinicalFlow Architecture](architecture/data-flow.md)

ClinicalFlow is a production-grade, metadata-driven healthcare data engineering platform designed to ingest, process, validate, normalize, and warehouse multi-source clinical data (Synthea FHIR R4 JSON, SQL Server EHR relational database, and External Claims CSVs).

Engineered around **Medallion Architecture (Bronze -> Silver -> Gold)**, Delta Lake transactional storage, and SQL Data Warehousing, the platform features **Slowly Changing Dimensions (SCD Type 2)**, **Data Quality & Quarantine Framework**, **Audit Observability**, and **Controlled Failure Recovery**.

---

## 🌟 Key Engineering Features & Highlights

> **Status:** under active rebuild. Items marked *(planned)* are designed but not implemented yet; the ordered plan lives in [CLAUDE.md](CLAUDE.md#fix-plan-gap-analysis-of-2026-09-21).

1. **Metadata-Driven Ingestion Engine**:
   - Central control table (`pipeline_config`) drives bronze ingestion: adding a source is a new row, not new code. Operators can disable a table with `active_flag`.
   - Per-table watermarks in `watermark_state`, advanced only after the bronze write and its audit row commit. SQL Server CDC *(planned — fix plan step 2)*.

2. **Healthcare Data Normalization & PII Masking**:
   - Bronze lands every FHIR R4 resource as its original JSON text (no schema inference). Silver flattens `Patient` and `Observation` with explicit schemas; `Encounter`, `Condition`, `MedicationRequest`, `Practitioner` *(planned)*.
   - SSN hashing helper exists; end-to-end PHI masking demonstration *(planned)*.

3. **Data Quality Framework & Quarantine Isolation**:
   - NOT NULL and RANGE rules route failing rows to `quarantine_records` with the raw payload and error context instead of dropping them. Rules currently live in code; loading them from `data_quality_rule`, more rule types, and run-failing thresholds *(planned — step 5)*.

4. **Idempotency & Restartability**:
   - Bronze is append-only, partitioned by `_ingest_date` / `_pipeline_run_id`. Rerunning a run ID skips tables it already landed; a failed run ID resumes from the current watermark. Proven by `tests/integration/test_bronze_incremental.py`.
   - Silver and gold still rebuild in full each run; key-based Delta `MERGE` *(planned — step 4)*.

5. **Star Schema Data Warehouse & SCD Type 2**:
   - Built today: `dim_patient` (SCD Type 2), `dim_date`, `fact_encounter`, `fact_observation`, `fact_claim`.
   - Stable surrogate keys, point-in-time fact joins, unknown-member rows, and `dim_provider`, `dim_facility`, `dim_diagnosis`, `dim_medication`, `dim_department`, `fact_medication_order`, `fact_diagnosis` *(planned — step 6)*.

6. **Controlled Failure Recovery & Observability**:
   - Every bronze table attempt writes a `SUCCESS`, `FAILED` or `SKIPPED` row to `pipeline_run_audit` with row counts and watermark window.
   - Failure-and-recovery demonstration that injects a real failure and replays only the failed partition *(planned — step 7)*.

---

## 📂 Repository Structure

```
clinicalflow/
├── README.md
├── architecture/
│   ├── data-flow.md
│   └── design-decisions.md
├── adf/
│   ├── pipelines/
│   │   ├── pl_metadata_driven_ingestion.json
│   │   └── pl_databricks_medallion_orchestrator.json
│   ├── datasets/
│   └── linked-services/
├── databricks/
│   ├── bronze/
│   │   └── ingest_raw_data.py
│   ├── silver/
│   │   ├── process_fhir_silver.py
│   │   └── process_relational_silver.py
│   ├── gold/
│   │   ├── build_dimensions.py
│   │   └── build_facts.py
│   └── utilities/
│       ├── config.py
│       ├── logger.py
│       ├── quality_engine.py
│       └── failure_simulation.py
├── sql/
│   ├── source/
│   │   └── 01_ehr_source_schema.sql
│   ├── quality/
│   │   └── 01_data_quality_framework.sql
│   └── warehouse/
│       └── 01_gold_star_schema.sql
├── tests/
│   ├── unit/
│   │   ├── test_fhir_parser.py
│   │   ├── test_quality_engine.py
│   │   └── test_scd2_logic.py
│   ├── integration/
│   │   └── test_pipeline_idempotency.py
│   └── reconciliation/
│       └── test_reconciliation.py
├── infrastructure/
│   └── terraform/
│       ├── main.tf
│       └── variables.tf
├── dashboards/
│   ├── clinical_operations_queries.sql
│   ├── data_quality_queries.sql
│   └── pipeline_observability_queries.sql
├── sample-data/
│   └── generate_clinical_data.py
├── docs/
│   ├── data-dictionary.md
│   ├── pipeline-runbook.md
│   ├── troubleshooting-guide.md
│   └── source-to-target-mapping.md
├── docker-compose.yml
└── requirements.txt
```

---

## 🚀 Quickstart & Execution Guide

### 1. Generate Synthetic Data
Generate FHIR R4 bundles, EHR extracts, and Claims CSV feeds (defaults: ~366k records):
```bash
python sample-data/generate_clinical_data.py            # --ehr-patients / --fhir-patients / --claims
```

### 1b. Start the SQL Server source and load it
Requires Docker and an `.env` with `MSSQL_SA_PASSWORD` (see `.env.example`):
```bash
docker compose up -d sqlserver
python -m scripts.setup_source_db                       # schema + CDC + BULK INSERT (--reset to rebuild)
python -m scripts.simulate_source_changes               # optional: inserts/updates/deletes for CDC to capture
```

### 2. Run End-to-End Medallion Pipeline
Ingest raw data into Bronze, clean/validate into Silver, and build Gold dimensions & facts:
```bash
python -m databricks.bronze.ingest_raw_data
python -m databricks.silver.process_fhir_silver
python -m databricks.silver.process_relational_silver
python -m databricks.gold.build_dimensions
python -m databricks.gold.build_facts
```

### 3. Run Controlled Failure & Recovery Demonstration
Current demo: quarantines injected bad lab values and reruns the pipeline (being rebuilt in step 7):
```bash
python -m databricks.utilities.failure_simulation
```

### 4. Run PyTest Unit & Integration Test Suite
Unit and integration tests (they run against a temporary lakehouse, never `delta_lakehouse/`):
```bash
python -m pytest tests/ -v
```

---

## 📊 Analytics & Observability Dashboards

SQL queries for Power BI / Databricks SQL are provided in `dashboards/`:
- **Clinical Operations**: Encounters by department, Average Length of Stay (LOS), Emergency vs. Inpatient volume, 30-Day Readmission rate.
- **Data Quality**: Rejection rates by dataset, Quarantine distribution, Rule failure frequencies.
- **Pipeline Operations**: Execution success/failure metrics, run duration, CDC latency tracking.
