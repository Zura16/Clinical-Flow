# ClinicalFlow Data Dictionary

## 1. Metadata Control Tables (`clinicalflow_meta`)

### `pipeline_config`
| Column Name | Data Type | Constraint | Description |
|---|---|---|---|
| `config_id` | INT | PRIMARY KEY | Configuration ID |
| `source_name` | VARCHAR(100) | NOT NULL | Source system identifier (`sql_ehr`, `fhir_r4`, `claims_csv`) |
| `source_table` | VARCHAR(100) | NOT NULL | Source entity name |
| `destination_table` | VARCHAR(100) | NOT NULL | Bronze Delta table target |
| `ingestion_type` | VARCHAR(30) | NOT NULL | Load mechanism (`Full`, `Watermark`, `CDC`, `FileIncremental`) |
| `watermark_column` | VARCHAR(100) | NULL | Column used for incremental watermark tracking; for `FileIncremental`, the record's own version timestamp (orders versions in silver, filters nothing in bronze) |
| `primary_key_columns` | VARCHAR(500) | NOT NULL | Primary key(s) |
| `active_flag` | BIT | DEFAULT 1 | Active status flag |

### `file_ingest_log`
Which source files each `FileIncremental` bronze table has read. **Grain:** one row per
(`destination_table`, `file_path`, `file_size`, `file_modification_time`, `pipeline_run_id`).
A file counts as ingested only when its run has a `SUCCESS` row in `pipeline_run_audit` for
`bronze:<destination_table>`; entries from runs that never succeeded are ignored.

| Column Name | Data Type | Constraint | Description |
|---|---|---|---|
| `destination_table` | VARCHAR(100) | PK | Bronze table that read the file |
| `file_path` | VARCHAR(1000) | PK | File URI as the filesystem lists it |
| `file_size` | BIGINT | PK | Size in bytes when read |
| `file_modification_time` | DATETIME2 | PK | mtime when read; a changed size or mtime makes the file pending again |
| `pipeline_run_id` | VARCHAR(100) | PK | Run that read it (joins to `pipeline_run_audit`) |
| `logged_at` | DATETIME2 | NOT NULL | When the entry was written |

### `quarantine_records`
| Column Name | Data Type | Constraint | Description |
|---|---|---|---|
| `quarantine_id` | INT | PRIMARY KEY | Quarantine record ID |
| `pipeline_run_id` | VARCHAR(100) | NOT NULL | Execution pipeline run GUID |
| `source_name` | VARCHAR(100) | NOT NULL | Source entity name |
| `record_identifier` | VARCHAR(255) | NOT NULL | Source primary key value |
| `failed_rule` | VARCHAR(255) | NOT NULL | Rule type and target column |
| `error_message` | VARCHAR(1000) | NOT NULL | Failure details |
| `raw_payload` | NVARCHAR(MAX) | NOT NULL | Original record JSON payload |
| `resolution_status` | VARCHAR(30) | DEFAULT 'PENDING' | Status (`PENDING`, `REPLAYED`, `IGNORED`) |

---

## 2. Gold Data Warehouse Tables (`clinicalflow_dw`)

### `dim_patient` (SCD Type 2)
| Column Name | Data Type | Description |
|---|---|---|
| `patient_sk` | BIGINT | Primary Key (Surrogate Key) |
| `patient_id` | VARCHAR(64) | Business Key (Patient GUID) |
| `first_name` | VARCHAR(100) | Patient First Name |
| `last_name` | VARCHAR(100) | Patient Last Name |
| `date_of_birth` | DATE | Birth Date |
| `gender` | VARCHAR(20) | Patient Gender |
| `address_street` | VARCHAR(255) | Street Address |
| `insurance_type` | VARCHAR(50) | Primary Insurance Provider |
| `effective_start_date` | DATETIME2 | SCD2 Validity Start Timestamp |
| `effective_end_date` | DATETIME2 | SCD2 Validity End Timestamp (NULL if current) |
| `is_current` | BIT | Current version indicator (1 = Current, 0 = Past) |
| `record_hash` | VARCHAR(64) | SHA-256 Hash of attributes |

### `fact_encounter`
| Column Name | Data Type | Description |
|---|---|---|
| `encounter_fact_id` | BIGINT | Primary Key |
| `encounter_id` | VARCHAR(64) | Business Key |
| `patient_sk` | BIGINT | Foreign Key to `dim_patient` |
| `admission_date_key` | INT | Foreign Key to `dim_date` (YYYYMMDD) |
| `discharge_date_key` | INT | Foreign Key to `dim_date` (YYYYMMDD) |
| `encounter_type` | VARCHAR(50) | Encounter Type (Inpatient, Outpatient, Emergency) |
| `length_of_stay_hours` | NUMERIC(10,2)| Length of Stay in Hours |
| `is_readmission_30d` | BIT | Flag indicating 30-day readmission |
