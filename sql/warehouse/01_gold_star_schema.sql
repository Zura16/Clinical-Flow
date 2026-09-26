-- ===============================================================================
-- ClinicalFlow: Gold Data Warehouse Star Schema & SCD Type 2
-- Description: Dimensional warehouse model supporting analytical reporting and clinical operations.
-- ===============================================================================

CREATE DATABASE clinicalflow_dw;
GO
USE clinicalflow_dw;
GO

-- ===============================================================================
-- DIMENSION TABLES
-- ===============================================================================

-- 1. Patient Dimension (SCD Type 2)
CREATE TABLE dbo.dim_patient (
    patient_sk             BIGINT NOT NULL PRIMARY KEY, -- xxhash64 of the natural key (see databricks/gold/keys.py)
    -- The natural key is (source_system, patient_id): EHR and FHIR share no identifier, so one
    -- human present in both is two members here until identity resolution exists.
    source_system           VARCHAR(30) NOT NULL,
    patient_id              VARCHAR(64) NOT NULL,
    first_name              VARCHAR(100) NULL,
    last_name               VARCHAR(100) NULL,
    date_of_birth           DATE NULL,
    gender                  VARCHAR(20) NULL,
    address_street          VARCHAR(255) NULL,
    city                    VARCHAR(100) NULL,
    state                   VARCHAR(50) NULL,
    postal_code             VARCHAR(20) NULL,
    phone_number            VARCHAR(30) NULL,
    insurance_type          VARCHAR(50) NULL,
    -- A member's FIRST version opens at 1900-01-01 so that facts predating our first sight of the
    -- record still resolve; later versions open when the change happened. Windows are contiguous:
    -- one version's end is the next one's start.
    effective_start_date    DATETIME2 NOT NULL,
    effective_end_date      DATETIME2 NULL,
    is_current              BIT NOT NULL DEFAULT 1,
    is_deleted              BIT NOT NULL DEFAULT 0, -- deleted at source; the version records when
    record_hash             VARCHAR(64) NOT NULL,
    CONSTRAINT uq_dim_patient_version UNIQUE (source_system, patient_id, effective_start_date)
);
CREATE INDEX idx_dim_patient_id ON dbo.dim_patient(source_system, patient_id, is_current);
CREATE INDEX idx_dim_patient_window ON dbo.dim_patient(patient_id, effective_start_date, effective_end_date);

-- 2. Provider Dimension
CREATE TABLE dbo.dim_provider (
    provider_sk             BIGINT NOT NULL PRIMARY KEY, -- xxhash64 of the natural key (see databricks/gold/keys.py)
    provider_id             VARCHAR(64) NOT NULL UNIQUE,
    npi                     VARCHAR(10) NOT NULL,
    first_name              VARCHAR(100) NOT NULL,
    last_name               VARCHAR(100) NOT NULL,
    specialty               VARCHAR(100) NOT NULL,
    department_id           VARCHAR(64) NOT NULL,
    facility_id             VARCHAR(64) NOT NULL,
    created_at              DATETIME2 DEFAULT GETUTCDATE()
);

-- 3. Facility Dimension
CREATE TABLE dbo.dim_facility (
    facility_sk             BIGINT NOT NULL PRIMARY KEY, -- xxhash64 of the natural key (see databricks/gold/keys.py)
    facility_id             VARCHAR(64) NOT NULL UNIQUE,
    facility_name           VARCHAR(200) NOT NULL,
    facility_type           VARCHAR(50) NOT NULL,
    address                 VARCHAR(255) NULL,
    city                    VARCHAR(100) NULL,
    state                   VARCHAR(50) NULL,
    postal_code             VARCHAR(20) NULL,
    created_at              DATETIME2 DEFAULT GETUTCDATE()
);

-- 4. Diagnosis Reference Dimension
CREATE TABLE dbo.dim_diagnosis (
    diagnosis_sk             BIGINT NOT NULL PRIMARY KEY, -- xxhash64 of the natural key (see databricks/gold/keys.py)
    icd10_code              VARCHAR(20) NOT NULL UNIQUE,
    diagnosis_description   VARCHAR(255) NOT NULL,
    category                VARCHAR(100) NULL,
    created_at              DATETIME2 DEFAULT GETUTCDATE()
);

-- 5. Medication Reference Dimension
CREATE TABLE dbo.dim_medication (
    medication_sk             BIGINT NOT NULL PRIMARY KEY, -- xxhash64 of the natural key (see databricks/gold/keys.py)
    rxnorm_code             VARCHAR(20) NOT NULL UNIQUE,
    medication_name         VARCHAR(255) NOT NULL,
    drug_class              VARCHAR(100) NULL,
    created_at              DATETIME2 DEFAULT GETUTCDATE()
);

-- 6. Date Dimension
CREATE TABLE dbo.dim_date (
    date_key                INT PRIMARY KEY, -- YYYYMMDD
    full_date               DATE NOT NULL,
    day_of_week             INT NOT NULL,
    day_name                VARCHAR(20) NOT NULL,
    day_of_month            INT NOT NULL,
    day_of_year             INT NOT NULL,
    week_of_year            INT NOT NULL,
    month_number            INT NOT NULL,
    month_name              VARCHAR(20) NOT NULL,
    quarter                 INT NOT NULL,
    year                    INT NOT NULL,
    is_weekend              BIT NOT NULL
);

-- 7. Department Dimension
CREATE TABLE dbo.dim_department (
    department_sk             BIGINT NOT NULL PRIMARY KEY, -- xxhash64 of the natural key (see databricks/gold/keys.py)
    department_id           VARCHAR(64) NOT NULL UNIQUE,
    department_name         VARCHAR(100) NOT NULL,
    created_at              DATETIME2 DEFAULT GETUTCDATE()
);

-- Unknown member (-1) per dimension: a fact whose dimension is missing or late still joins to a
-- real row instead of carrying a NULL. Surrogate keys are deterministic hashes of the natural key,
-- so rebuilding a dimension never renumbers it and the facts stay valid.
INSERT INTO dbo.dim_patient (patient_sk, source_system, patient_id, first_name, last_name, effective_start_date, is_current, is_deleted, record_hash)
VALUES (-1, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', '1900-01-01', 1, 0, 'UNKNOWN');

INSERT INTO dbo.dim_provider (provider_sk, provider_id, npi, first_name, last_name, specialty, department_id, facility_id)
VALUES (-1, 'UNKNOWN', '0000000000', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN');

INSERT INTO dbo.dim_facility (facility_sk, facility_id, facility_name, facility_type)
VALUES (-1, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN');

-- ===============================================================================
-- FACT TABLES
-- ===============================================================================

-- Facts are keyed by their business key: that is the grain, and it makes a rerun a MERGE rather
-- than an append. Measures are computed from the data; where a source cannot support one (a FHIR
-- observation has no order time, so no turnaround) the column is NULL rather than a filler value.

-- 1. Encounter Fact
CREATE TABLE dbo.fact_encounter (
    encounter_id                  VARCHAR(64) NOT NULL PRIMARY KEY,
    patient_sk                    BIGINT NOT NULL,   -- the version current at admission, not today's
    provider_sk                   BIGINT NOT NULL,
    facility_sk                   BIGINT NOT NULL,
    department_sk                 BIGINT NOT NULL,
    admission_date_key            INT NOT NULL,
    discharge_date_key            INT NULL,
    encounter_type                VARCHAR(50) NOT NULL,
    admission_timestamp           DATETIME2 NOT NULL,
    discharge_timestamp           DATETIME2 NULL,
    length_of_stay_hours          NUMERIC(10, 2) NULL,
    discharge_disposition         VARCHAR(100) NULL,
    is_readmission_30d            BIT NOT NULL,      -- admitted within 30 days of own prior discharge
    days_since_previous_discharge INT NULL,
    FOREIGN KEY (patient_sk) REFERENCES dbo.dim_patient(patient_sk),
    FOREIGN KEY (provider_sk) REFERENCES dbo.dim_provider(provider_sk),
    FOREIGN KEY (facility_sk) REFERENCES dbo.dim_facility(facility_sk),
    FOREIGN KEY (department_sk) REFERENCES dbo.dim_department(department_sk),
    FOREIGN KEY (admission_date_key) REFERENCES dbo.dim_date(date_key)
);

-- 2. Observation Fact: EHR lab results and FHIR observations, tagged by source_system.
CREATE TABLE dbo.fact_observation (
    observation_id          VARCHAR(64) NOT NULL PRIMARY KEY,
    source_system           VARCHAR(30) NOT NULL,
    patient_sk              BIGINT NOT NULL,
    encounter_id            VARCHAR(64) NULL,
    observation_date_key    INT NOT NULL,
    observation_timestamp   DATETIME2 NULL,
    loinc_code              VARCHAR(20) NULL,
    test_name               VARCHAR(150) NULL,
    result_value            NUMERIC(18, 4) NULL,
    result_unit             VARCHAR(50) NULL,
    abnormal_flag           VARCHAR(10) NULL,  -- EHR only; FHIR does not carry it
    turnaround_minutes      NUMERIC(10, 1) NULL, -- order to result; NULL when the source has no order time
    FOREIGN KEY (patient_sk) REFERENCES dbo.dim_patient(patient_sk),
    FOREIGN KEY (observation_date_key) REFERENCES dbo.dim_date(date_key)
);

-- 3. Medication Order Fact
CREATE TABLE dbo.fact_medication_order (
    medication_order_id     VARCHAR(64) NOT NULL PRIMARY KEY,
    patient_sk              BIGINT NOT NULL,
    encounter_id            VARCHAR(64) NULL,
    medication_sk           BIGINT NOT NULL,
    order_date_key          INT NOT NULL,
    order_timestamp         DATETIME2 NULL,
    dosage                  VARCHAR(50) NULL,
    route                   VARCHAR(50) NULL,
    frequency               VARCHAR(50) NULL,
    order_status            VARCHAR(20) NULL,
    FOREIGN KEY (patient_sk) REFERENCES dbo.dim_patient(patient_sk),
    FOREIGN KEY (medication_sk) REFERENCES dbo.dim_medication(medication_sk),
    FOREIGN KEY (order_date_key) REFERENCES dbo.dim_date(date_key)
);

-- 4. Diagnosis Fact
CREATE TABLE dbo.fact_diagnosis (
    diagnosis_id            VARCHAR(64) NOT NULL PRIMARY KEY,
    patient_sk              BIGINT NOT NULL,
    encounter_id            VARCHAR(64) NULL,
    diagnosis_sk            BIGINT NOT NULL,
    diagnosis_date_key      INT NOT NULL,
    diagnosis_timestamp     DATETIME2 NULL,
    diagnosis_type          VARCHAR(50) NULL,
    FOREIGN KEY (patient_sk) REFERENCES dbo.dim_patient(patient_sk),
    FOREIGN KEY (diagnosis_sk) REFERENCES dbo.dim_diagnosis(diagnosis_sk),
    FOREIGN KEY (diagnosis_date_key) REFERENCES dbo.dim_date(date_key)
);

-- 5. Claim Fact
CREATE TABLE dbo.fact_claim (
    claim_id                VARCHAR(64) NOT NULL PRIMARY KEY,
    patient_sk              BIGINT NOT NULL,
    facility_sk             BIGINT NOT NULL,
    service_date_key        INT NOT NULL,
    service_date            DATE NULL,
    claim_amount            NUMERIC(18, 2) NULL,
    paid_amount             NUMERIC(18, 2) NULL,
    unpaid_amount           NUMERIC(18, 2) NULL,
    claim_status            VARCHAR(30) NULL,
    insurance_type          VARCHAR(50) NULL,
    FOREIGN KEY (patient_sk) REFERENCES dbo.dim_patient(patient_sk),
    FOREIGN KEY (facility_sk) REFERENCES dbo.dim_facility(facility_sk),
    FOREIGN KEY (service_date_key) REFERENCES dbo.dim_date(date_key)
);
GO
