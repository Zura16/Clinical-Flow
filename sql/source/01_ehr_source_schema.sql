-- ===============================================================================
-- ClinicalFlow: EHR SQL Server Source Schema
-- Description: Defines transactional EHR tables representing Epic Clarity/Caboodle inspired source schemas.
-- ===============================================================================

CREATE DATABASE ehr_source;
GO
USE ehr_source;
GO

-- Enable Change Data Capture (CDC) on the database
EXEC sys.sp_cdc_enable_db;
GO

-- 1. Patients Table
CREATE TABLE dbo.patients (
    patient_id VARCHAR(64) PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    date_of_birth DATE NOT NULL,
    gender VARCHAR(20) NOT NULL,
    ssn_hash VARCHAR(64) NULL,
    address_street VARCHAR(255) NULL,
    city VARCHAR(100) NULL,
    state VARCHAR(50) NULL,
    postal_code VARCHAR(20) NULL,
    phone_number VARCHAR(30) NULL,
    insurance_type VARCHAR(50) NULL,
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2 DEFAULT GETUTCDATE(),
    is_deleted BIT DEFAULT 0
);

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'patients',
    @role_name     = NULL,
    @supports_net_changes = 1;
GO

-- 2. Providers Table
CREATE TABLE dbo.providers (
    provider_id VARCHAR(64) PRIMARY KEY,
    npi VARCHAR(10) NOT NULL UNIQUE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    specialty VARCHAR(100) NOT NULL,
    department_id VARCHAR(64) NOT NULL,
    facility_id VARCHAR(64) NOT NULL,
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2 DEFAULT GETUTCDATE()
);

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'providers',
    @role_name     = NULL;
GO

-- 3. Encounters Table
CREATE TABLE dbo.encounters (
    encounter_id VARCHAR(64) PRIMARY KEY,
    patient_id VARCHAR(64) NOT NULL,
    provider_id VARCHAR(64) NOT NULL,
    facility_id VARCHAR(64) NOT NULL,
    encounter_type VARCHAR(50) NOT NULL, -- Inpatient, Outpatient, Emergency
    admission_timestamp DATETIME2 NOT NULL,
    discharge_timestamp DATETIME2 NULL,
    discharge_disposition VARCHAR(100) NULL,
    department_id VARCHAR(64) NOT NULL,
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2 DEFAULT GETUTCDATE(),
    FOREIGN KEY (patient_id) REFERENCES dbo.patients(patient_id),
    FOREIGN KEY (provider_id) REFERENCES dbo.providers(provider_id)
);

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'encounters',
    @role_name     = NULL;
GO

-- 4. Diagnoses Table
CREATE TABLE dbo.diagnoses (
    diagnosis_id VARCHAR(64) PRIMARY KEY,
    encounter_id VARCHAR(64) NOT NULL,
    patient_id VARCHAR(64) NOT NULL,
    icd10_code VARCHAR(20) NOT NULL,
    diagnosis_description VARCHAR(255) NOT NULL,
    diagnosis_type VARCHAR(50) NOT NULL, -- Primary, Secondary, Admitting
    diagnosis_timestamp DATETIME2 NOT NULL,
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2 DEFAULT GETUTCDATE(),
    FOREIGN KEY (encounter_id) REFERENCES dbo.encounters(encounter_id)
);

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'diagnoses',
    @role_name     = NULL;
GO

-- 5. Lab Results Table
CREATE TABLE dbo.lab_results (
    lab_result_id VARCHAR(64) PRIMARY KEY,
    encounter_id VARCHAR(64) NOT NULL,
    patient_id VARCHAR(64) NOT NULL,
    loinc_code VARCHAR(20) NOT NULL,
    test_name VARCHAR(150) NOT NULL,
    result_value NUMERIC(18, 4) NULL,
    result_unit VARCHAR(50) NULL,
    reference_range VARCHAR(50) NULL,
    abnormal_flag VARCHAR(10) NULL, -- Normal, High, Low, Critical
    result_status VARCHAR(20) NOT NULL, -- Final, Preliminary, Corrected
    order_timestamp DATETIME2 NOT NULL,
    result_timestamp DATETIME2 NOT NULL,
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2 DEFAULT GETUTCDATE()
);

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'lab_results',
    @role_name     = NULL;
GO

-- 6. Medications Table
CREATE TABLE dbo.medications (
    medication_order_id VARCHAR(64) PRIMARY KEY,
    encounter_id VARCHAR(64) NOT NULL,
    patient_id VARCHAR(64) NOT NULL,
    rxnorm_code VARCHAR(20) NOT NULL,
    medication_name VARCHAR(255) NOT NULL,
    dosage VARCHAR(50) NOT NULL,
    route VARCHAR(50) NOT NULL,
    frequency VARCHAR(50) NOT NULL,
    order_status VARCHAR(20) NOT NULL, -- Active, Completed, Stopped
    order_timestamp DATETIME2 NOT NULL,
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2 DEFAULT GETUTCDATE()
);

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'medications',
    @role_name     = NULL;
GO
