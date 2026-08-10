-- ===============================================================================
-- ClinicalFlow: Data Quality & Quarantine Dashboard Queries
-- Description: Monitoring data quality, completeness, and quarantine record distribution.
-- ===============================================================================

USE clinicalflow_meta;

-- 1. Quarantine Record Distribution by Source & Failed Rule
SELECT 
    source_name,
    failed_rule,
    COUNT(quarantine_id) AS total_quarantined_records,
    MAX(detected_timestamp) AS last_detected_timestamp,
    resolution_status
FROM dbo.quarantine_records
GROUP BY source_name, failed_rule, resolution_status
ORDER BY total_quarantined_records DESC;

-- 2. Data Freshness and Rejection Percentage per Dataset
SELECT 
    source_name,
    layer,
    COUNT(audit_id) AS total_runs,
    SUM(rows_read) AS total_rows_read,
    SUM(rows_inserted) AS total_rows_inserted,
    SUM(rows_rejected) AS total_rows_rejected,
    ROUND(CAST(SUM(rows_rejected) AS FLOAT) / NULLIF(SUM(rows_read), 0) * 100, 2) AS rejection_rate_pct
FROM dbo.pipeline_run_audit
GROUP BY source_name, layer
ORDER BY rejection_rate_pct DESC;
