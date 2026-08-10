-- ===============================================================================
-- ClinicalFlow: Pipeline Observability Dashboard Queries
-- Description: Tracking pipeline run history, execution status, latency, and duration.
-- ===============================================================================

USE clinicalflow_meta;

-- 1. Pipeline Execution Status Summary
SELECT 
    pipeline_name,
    layer,
    execution_status,
    COUNT(audit_id) AS total_executions,
    AVG(DATEDIFF(second, start_timestamp, end_timestamp)) AS avg_duration_seconds,
    MAX(end_timestamp) AS last_run_timestamp
FROM dbo.pipeline_run_audit
GROUP BY pipeline_name, layer, execution_status
ORDER BY last_run_timestamp DESC;

-- 2. Recent Pipeline Failures and Errors
SELECT 
    pipeline_run_id,
    pipeline_name,
    source_name,
    layer,
    start_timestamp,
    error_code,
    error_message
FROM dbo.pipeline_run_audit
WHERE execution_status = 'FAILED'
ORDER BY start_timestamp DESC;
