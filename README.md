# ClinicalFlow: A Fault-Tolerant Epic/FHIR Healthcare Lakehouse

![ClinicalFlow Architecture](architecture/data-flow.md)

ClinicalFlow is a production-grade, metadata-driven healthcare data engineering platform designed to ingest, process, validate, normalize, and warehouse multi-source clinical data (Synthea FHIR R4 JSON, SQL Server EHR relational database, and External Claims CSVs).

Engineered around **Medallion Architecture (Bronze -> Silver -> Gold)**, Delta Lake transactional storage, and SQL Data Warehousing, the platform features **Slowly Changing Dimensions (SCD Type 2)**, **Data Quality & Quarantine Framework**, **Audit Observability**, and **Controlled Failure Recovery**.

---

## 🌟 Key Engineering Features & Highlights

1. **Metadata-Driven Ingestion Engine**:
   - Central control table (`pipeline_config`) dynamically drives ADF and PySpark extraction behavior without hardcoded individual pipelines.
   - Configurable watermark and Change Data Capture (CDC) processing.

2. **Healthcare Data Normalization & PII Masking**:
   - Flattens nested FHIR R4 JSON resources (`Patient`, `Encounter`, `Observation`, `Condition`, `MedicationRequest`).
   - Normalizes clinical coding standards (LOINC, ICD-10-CM, RxNorm).
   - Protects sensitive health information (PHI/PII) using SHA-256 hashing.

3. **Data Quality Framework & Quarantine Isolation**:
   - Configurable rules table (`data_quality_rule`) evaluating constraints (NOT NULL, RANGE, REGEX).
   - Bad records (e.g. malformed lab values) are isolated in `quarantine_records` with raw JSON payload and error context—never silently dropped.

4. **100% Idempotency & Record Hashing**:
   - SHA-256 record hashes generated for all incoming records.
   - MERGE-based upserts guarantee that rerunning pipelines produces identical final outputs with **zero row duplication**.

5. **Star Schema Data Warehouse & SCD Type 2**:
   - **Dimensions**: `dim_patient` (SCD Type 2 historical change tracking), `dim_provider`, `dim_facility`, `dim_diagnosis`, `dim_medication`, `dim_date`, `dim_department`.
   - **Facts**: `fact_encounter`, `fact_observation`, `fact_medication_order`, `fact_diagnosis`, `fact_claim`.
   - Handles late-arriving dimensions gracefully using surrogate key `-1` ("UNKNOWN").

6. **Controlled Failure Recovery & Observability**:
   - Detailed pipeline run metrics logged to `pipeline_run_audit`.
   - Complete failure simulation script (`failure_simulation.py`) demonstrating malformed data injection, quarantine capture, rule fix, partition replay, and idempotency verification.

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
Generate realistic FHIR R4 JSON bundles, EHR SQL transactional tables, and Claims CSV feeds:
```bash
python sample-data/generate_clinical_data.py
```

### 2. Run End-to-End Medallion Pipeline
Ingest raw data into Bronze, clean/validate into Silver, and build Gold dimensions & facts:
```bash
python databricks/bronze/ingest_raw_data.py
python databricks/silver/process_fhir_silver.py
python databricks/silver/process_relational_silver.py
python databricks/gold/build_dimensions.py
python databricks/gold/build_facts.py
```

### 3. Run Controlled Failure & Recovery Demonstration
Demonstrate system resilience under bad data injection, quarantine isolation, rule correction, and idempotent partition replay:
```bash
python databricks/utilities/failure_simulation.py
```

### 4. Run PyTest Unit & Integration Test Suite
Execute comprehensive test coverage for FHIR parsing, Data Quality Engine, SCD Type 2 logic, idempotency, and reconciliation:
```bash
python -m pytest tests/ -v
```

---

## 📊 Analytics & Observability Dashboards

SQL queries for Power BI / Databricks SQL are provided in `dashboards/`:
- **Clinical Operations**: Encounters by department, Average Length of Stay (LOS), Emergency vs. Inpatient volume, 30-Day Readmission rate.
- **Data Quality**: Rejection rates by dataset, Quarantine distribution, Rule failure frequencies.
- **Pipeline Operations**: Execution success/failure metrics, run duration, CDC latency tracking.
