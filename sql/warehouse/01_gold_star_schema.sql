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
    patient_sk              BIGINT IDENTITY(1,1) PRIMARY KEY,
    patient_id              VARCHAR(64) NOT NULL,
    first_name              VARCHAR(100) NOT NULL,
    last_name               VARCHAR(100) NOT NULL,
    date_of_birth           DATE NOT NULL,
    gender                  VARCHAR(20) NOT NULL,
    address_street          VARCHAR(255) NULL,
    city                    VARCHAR(100) NULL,
    state                   VARCHAR(50) NULL,
    postal_code             VARCHAR(20) NULL,
    phone_number            VARCHAR(30) NULL,
    insurance_type          VARCHAR(50) NULL,
    effective_start_date    DATETIME2 NOT NULL,
    effective_end_date      DATETIME2 NULL,
    is_current              BIT NOT NULL DEFAULT 1,
    record_hash             VARCHAR(64) NOT NULL,
    created_at              DATETIME2 DEFAULT GETUTCDATE()
);
CREATE INDEX idx_dim_patient_id ON dbo.dim_patient(patient_id, is_current);

-- 2. Provider Dimension
CREATE TABLE dbo.dim_provider (
    provider_sk             BIGINT IDENTITY(1,1) PRIMARY KEY,
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
    facility_sk             BIGINT IDENTITY(1,1) PRIMARY KEY,
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
    diagnosis_sk            BIGINT IDENTITY(1,1) PRIMARY KEY,
    icd10_code              VARCHAR(20) NOT NULL UNIQUE,
    diagnosis_description   VARCHAR(255) NOT NULL,
    category                VARCHAR(100) NULL,
    created_at              DATETIME2 DEFAULT GETUTCDATE()
);

-- 5. Medication Reference Dimension
CREATE TABLE dbo.dim_medication (
    medication_sk           BIGINT IDENTITY(1,1) PRIMARY KEY,
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
    department_sk           BIGINT IDENTITY(1,1) PRIMARY KEY,
    department_id           VARCHAR(64) NOT NULL UNIQUE,
    department_name         VARCHAR(100) NOT NULL,
    created_at              DATETIME2 DEFAULT GETUTCDATE()
);

-- Seed Unknown Member rows for late-arriving dimensions
SET IDENTITY_INSERT dbo.dim_patient ON;
INSERT INTO dbo.dim_patient (patient_sk, patient_id, first_name, last_name, date_of_birth, gender, effective_start_date, is_current, record_hash)
VALUES (-1, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', '1900-01-01', 'UNKNOWN', '1900-01-01', 1, 'UNKNOWN');
SET IDENTITY_INSERT dbo.dim_patient OFF;

SET IDENTITY_INSERT dbo.dim_provider ON;
INSERT INTO dbo.dim_provider (provider_sk, provider_id, npi, first_name, last_name, specialty, department_id, facility_id)
VALUES (-1, 'UNKNOWN', '0000000000', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN');
SET IDENTITY_INSERT dbo.dim_provider OFF;

SET IDENTITY_INSERT dbo.dim_facility ON;
INSERT INTO dbo.dim_facility (facility_sk, facility_id, facility_name, facility_type)
VALUES (-1, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN');
SET IDENTITY_INSERT dbo.dim_facility OFF;

-- ===============================================================================
-- FACT TABLES
-- ===============================================================================

-- 1. Encounter Fact
CREATE TABLE dbo.fact_encounter (
    encounter_fact_id       BIGINT IDENTITY(1,1) PRIMARY KEY,
    encounter_id            VARCHAR(64) NOT NULL UNIQUE,
    patient_sk              BIGINT NOT NULL,
    provider_sk             BIGINT NOT NULL,
    facility_sk             BIGINT NOT NULL,
    department_sk           BIGINT NOT NULL,
    admission_date_key      INT NOT NULL,
    discharge_date_key      INT NULL,
    encounter_type          VARCHAR(50) NOT NULL,
    length_of_stay_hours    NUMERIC(10, 2) NULL,
    discharge_disposition   VARCHAR(100) NULL,
    is_readmission_30d      BIT DEFAULT 0,
    created_at              DATETIME2 DEFAULT GETUTCDATE(),
    FOREIGN KEY (patient_sk) REFERENCES dbo.dim_patient(patient_sk),
    FOREIGN KEY (provider_sk) REFERENCES dbo.dim_provider(provider_sk),
    FOREIGN KEY (facility_sk) REFERENCES dbo.dim_facility(facility_sk),
    FOREIGN KEY (admission_date_key) REFERENCES dbo.dim_date(date_key)
);

-- 2. Observation Fact
CREATE TABLE dbo.fact_observation (
    observation_fact_id     BIGINT IDENTITY(1,1) PRIMARY KEY,
    observation_id          VARCHAR(64) NOT NULL UNIQUE,
    patient_sk              BIGINT NOT NULL,
    encounter_id            VARCHAR(64) NULL,
    observation_date_key    INT NOT NULL,
    loinc_code              VARCHAR(20) NOT NULL,
    test_name               VARCHAR(150) NOT NULL,
    result_value            NUMERIC(18, 4) NULL,
    result_unit             VARCHAR(50) NULL,
    abnormal_flag           VARCHAR(10) NULL,
    turnaround_time_minutes INT NULL,
    created_at              DATETIME2 DEFAULT GETUTCDATE(),
    FOREIGN KEY (patient_sk) REFERENCES dbo.dim_patient(patient_sk),
    FOREIGN KEY (observation_date_key) REFERENCES dbo.dim_date(date_key)
);

-- 3. Medication Order Fact
CREATE TABLE dbo.fact_medication_order (
    medication_fact_id      BIGINT IDENTITY(1,1) PRIMARY KEY,
    medication_order_id     VARCHAR(64) NOT NULL UNIQUE,
    patient_sk              BIGINT NOT NULL,
    provider_sk             BIGINT NOT NULL,
    encounter_id            VARCHAR(64) NOT NULL,
    medication_sk           BIGINT NOT NULL,
    order_date_key          INT NOT NULL,
    dosage                  VARCHAR(50) NOT NULL,
    route                   VARCHAR(50) NOT NULL,
    order_status            VARCHAR(20) NOT NULL,
    created_at              DATETIME2 DEFAULT GETUTCDATE(),
    FOREIGN KEY (patient_sk) REFERENCES dbo.dim_patient(patient_sk),
    FOREIGN KEY (provider_sk) REFERENCES dbo.dim_provider(provider_sk)
);

-- 4. Diagnosis Fact
CREATE TABLE dbo.fact_diagnosis (
    diagnosis_fact_id       BIGINT IDENTITY(1,1) PRIMARY KEY,
    diagnosis_id            VARCHAR(64) NOT NULL UNIQUE,
    patient_sk              BIGINT NOT NULL,
    encounter_id            VARCHAR(64) NOT NULL,
    diagnosis_sk            BIGINT NOT NULL,
    diagnosis_date_key      INT NOT NULL,
    diagnosis_type          VARCHAR(50) NOT NULL,
    created_at              DATETIME2 DEFAULT GETUTCDATE(),
    FOREIGN KEY (patient_sk) REFERENCES dbo.dim_patient(patient_sk),
    FOREIGN KEY (diagnosis_sk) REFERENCES dbo.dim_diagnosis(diagnosis_sk)
);

-- 5. Claim Fact
CREATE TABLE dbo.fact_claim (
    claim_fact_id           BIGINT IDENTITY(1,1) PRIMARY KEY,
    claim_id                VARCHAR(64) NOT NULL UNIQUE,
    patient_sk              BIGINT NOT NULL,
    facility_sk             BIGINT NOT NULL,
    service_date_key        INT NOT NULL,
    claim_amount            NUMERIC(18, 2) NOT NULL,
    paid_amount             NUMERIC(18, 2) NOT NULL,
    claim_status            VARCHAR(30) NOT NULL,
    insurance_type          VARCHAR(50) NOT NULL,
    created_at              DATETIME2 DEFAULT GETUTCDATE(),
    FOREIGN KEY (patient_sk) REFERENCES dbo.dim_patient(patient_sk),
    FOREIGN KEY (facility_sk) REFERENCES dbo.dim_facility(facility_sk)
);
GO
