-- ===============================================================================
-- ClinicalFlow: Clinical Operations Analytical Queries
-- Description: Queries for Power BI / Databricks SQL analytics dashboards.
-- ===============================================================================

USE clinicalflow_dw;

-- 1. Encounters by Department and Type
SELECT 
    d.department_name,
    fe.encounter_type,
    COUNT(fe.encounter_fact_id) AS total_encounters,
    AVG(fe.length_of_stay_hours) AS avg_length_of_stay_hours
FROM dbo.fact_encounter fe
JOIN dbo.dim_department d ON fe.department_sk = d.department_sk
GROUP BY d.department_name, fe.encounter_type
ORDER BY total_encounters DESC;

-- 2. Emergency vs. Inpatient Volume & 30-Day Readmissions
SELECT 
    dd.year,
    dd.month_name,
    fe.encounter_type,
    COUNT(fe.encounter_fact_id) AS total_encounters,
    SUM(CAST(fe.is_readmission_30d AS INT)) AS total_30d_readmissions,
    ROUND(SUM(CAST(fe.is_readmission_30d AS FLOAT)) / COUNT(fe.encounter_fact_id) * 100, 2) AS readmission_rate_pct
FROM dbo.fact_encounter fe
JOIN dbo.dim_date dd ON fe.admission_date_key = dd.date_key
GROUP BY dd.year, dd.month_name, fe.encounter_type
ORDER BY dd.year, dd.month_name;

-- 3. Lab Result Turnaround Time by Test Type
SELECT 
    fo.loinc_code,
    fo.test_name,
    COUNT(fo.observation_fact_id) AS total_tests,
    AVG(fo.turnaround_time_minutes) AS avg_turnaround_minutes
FROM dbo.fact_observation fo
GROUP BY fo.loinc_code, fo.test_name
ORDER BY total_tests DESC;
