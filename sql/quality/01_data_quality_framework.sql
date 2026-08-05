-- ===============================================================================
-- ClinicalFlow: Data Quality, Metadata Control, Quarantine & Audit Tables
-- Description: Defines metadata-driven control, quality rules, quarantine, and audit log tables.
-- ===============================================================================

CREATE DATABASE clinicalflow_meta;
GO
USE clinicalflow_meta;
GO

-- 1. Ingestion Control Table (Metadata-Driven Pipeline Configuration)
CREATE TABLE dbo.pipeline_config (
    config_id               INT IDENTITY(1,1) PRIMARY KEY,
    source_name             VARCHAR(100) NOT NULL,
    source_table            VARCHAR(100) NOT NULL,
    destination_table       VARCHAR(100) NOT NULL,
    ingestion_type          VARCHAR(30) NOT NULL, -- Full, Watermark, CDC
    watermark_column        VARCHAR(100) NULL,
    primary_key_columns     VARCHAR(500) NOT NULL,
    load_frequency          VARCHAR(30) DEFAULT 'Daily',
    active_flag             BIT DEFAULT 1,
    data_quality_threshold  DECIMAL(5,2) DEFAULT 95.00,
    created_at              DATETIME2 DEFAULT GETUTCDATE(),
    updated_at              DATETIME2 DEFAULT GETUTCDATE()
);

-- Seed metadata rules for active sources
INSERT INTO dbo.pipeline_config (source_name, source_table, destination_table, ingestion_type, watermark_column, primary_key_columns, active_flag, data_quality_threshold)
VALUES
('sql_ehr', 'patients', 'bronze_ehr_patients', 'CDC', 'updated_at', 'patient_id', 1, 98.00),
('sql_ehr', 'encounters', 'bronze_ehr_encounters', 'CDC', 'updated_at', 'encounter_id', 1, 98.00),
('sql_ehr', 'providers', 'bronze_ehr_providers', 'Watermark', 'updated_at', 'provider_id', 1, 99.00),
('sql_ehr', 'diagnoses', 'bronze_ehr_diagnoses', 'CDC', 'updated_at', 'diagnosis_id', 1, 95.00),
('sql_ehr', 'lab_results', 'bronze_ehr_lab_results', 'CDC', 'updated_at', 'lab_result_id', 1, 95.00),
('sql_ehr', 'medications', 'bronze_ehr_medications', 'CDC', 'updated_at', 'medication_order_id', 1, 95.00),
('fhir_r4', 'Observation', 'bronze_fhir_observation', 'Watermark', 'meta_lastUpdated', 'id', 1, 95.00),
('fhir_r4', 'Patient', 'bronze_fhir_patient', 'Watermark', 'meta_lastUpdated', 'id', 1, 98.00),
('fhir_r4', 'Condition', 'bronze_fhir_condition', 'Watermark', 'meta_lastUpdated', 'id', 1, 95.00),
('fhir_r4', 'MedicationRequest', 'bronze_fhir_medication_request', 'Watermark', 'meta_lastUpdated', 'id', 1, 95.00),
('claims_csv', 'insurance_claims.csv', 'bronze_claims', 'Full', NULL, 'claim_id', 1, 95.00),
('claims_csv', 'facility_info.csv', 'bronze_facilities', 'Full', NULL, 'facility_id', 1, 99.00);

-- 2. Data Quality Rules Table
CREATE TABLE dbo.data_quality_rule (
    rule_id              INT IDENTITY(1,1) PRIMARY KEY,
    dataset_name         VARCHAR(100) NOT NULL,
    column_name          VARCHAR(100) NOT NULL,
    rule_type            VARCHAR(50) NOT NULL, -- NOT_NULL, REFERENTIAL, RANGE, UNIQUE, FRESHNESS, REGEX
    rule_expression      VARCHAR(1000) NOT NULL,
    severity             VARCHAR(20) NOT NULL, -- ERROR, WARNING, CRITICAL
    failure_threshold    DECIMAL(5,2) DEFAULT 5.00,
    active_flag          BIT DEFAULT 1,
    created_at           DATETIME2 DEFAULT GETUTCDATE()
);

-- Seed data quality rules
INSERT INTO dbo.data_quality_rule (dataset_name, column_name, rule_type, rule_expression, severity, failure_threshold, active_flag)
VALUES
('silver_patients', 'patient_id', 'NOT_NULL', 'patient_id IS NOT NULL', 'CRITICAL', 0.00, 1),
('silver_encounters', 'patient_id', 'NOT_NULL', 'patient_id IS NOT NULL', 'CRITICAL', 0.00, 1),
('silver_encounters', 'discharge_timestamp', 'RANGE', 'discharge_timestamp IS NULL OR discharge_timestamp >= admission_timestamp', 'ERROR', 1.00, 1),
('silver_observations', 'result_value', 'RANGE', 'result_value IS NULL OR (result_value >= -500 AND result_value <= 50000)', 'ERROR', 2.00, 1),
('silver_observations', 'observation_timestamp', 'RANGE', 'observation_timestamp <= CURRENT_TIMESTAMP()', 'ERROR', 0.50, 1),
('silver_claims', 'claim_amount', 'RANGE', 'claim_amount >= 0', 'ERROR', 1.00, 1),
('silver_diagnoses', 'icd10_code', 'NOT_NULL', 'icd10_code IS NOT NULL', 'ERROR', 0.00, 1);

-- 3. Quarantine Table for Failed Records
CREATE TABLE dbo.quarantine_records (
    quarantine_id        INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_run_id      VARCHAR(100) NOT NULL,
    source_name          VARCHAR(100) NOT NULL,
    record_identifier    VARCHAR(255) NOT NULL,
    failed_rule          VARCHAR(255) NOT NULL,
    error_message        VARCHAR(1000) NOT NULL,
    raw_payload          NVARCHAR(MAX) NOT NULL,
    detected_timestamp   DATETIME2 DEFAULT GETUTCDATE(),
    resolution_status    VARCHAR(30) DEFAULT 'PENDING' -- PENDING, REPLAYED, IGNORED
);

-- 4. Pipeline Run Audit & Lineage Table
CREATE TABLE dbo.pipeline_run_audit (
    audit_id             INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_run_id      VARCHAR(100) NOT NULL,
    pipeline_name        VARCHAR(100) NOT NULL,
    source_name          VARCHAR(100) NOT NULL,
    layer                VARCHAR(20) NOT NULL, -- BRONZE, SILVER, GOLD
    start_timestamp      DATETIME2 NOT NULL,
    end_timestamp        DATETIME2 NULL,
    rows_read            BIGINT DEFAULT 0,
    rows_inserted        BIGINT DEFAULT 0,
    rows_updated         BIGINT DEFAULT 0,
    rows_deleted         BIGINT DEFAULT 0,
    rows_rejected        BIGINT DEFAULT 0,
    watermark_start      VARCHAR(100) NULL,
    watermark_end        VARCHAR(100) NULL,
    execution_status     VARCHAR(30) NOT NULL, -- RUNNING, SUCCESS, FAILED
    error_code           VARCHAR(50) NULL,
    error_message        VARCHAR(2000) NULL,
    created_at           DATETIME2 DEFAULT GETUTCDATE()
);
GO
