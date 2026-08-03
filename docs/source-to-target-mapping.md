# ClinicalFlow Source-to-Target Mapping Matrix

| Source System | Source Field | Target Table | Target Column | Transformation / Rule |
|---|---|---|---|---|
| Synthea FHIR R4 JSON | `Patient.id` | `silver_fhir_patients` | `patient_id` | Direct mapping |
| Synthea FHIR R4 JSON | `Patient.name[0].given[0]` | `silver_fhir_patients` | `first_name` | PII Masked if configured |
| Synthea FHIR R4 JSON | `Patient.birthDate` | `silver_fhir_patients` | `date_of_birth` | Cast to `DATE` |
| Synthea FHIR R4 JSON | `Observation.code.coding[0].code` | `silver_fhir_observations` | `loinc_code` | LOINC System validation |
| Synthea FHIR R4 JSON | `Observation.valueQuantity.value` | `silver_fhir_observations` | `result_value` | Data Quality Range Check (-500 to 50000) |
| SQL Server EHR DB | `dbo.patients.patient_id` | `dim_patient` | `patient_id` | Business key lookup |
| SQL Server EHR DB | `dbo.patients.address_street` | `dim_patient` | `address_street` | SCD Type 2 tracked change |
| SQL Server EHR DB | `dbo.encounters.admission_timestamp` | `fact_encounter` | `admission_date_key` | Date formatting to `YYYYMMDD` |
| SQL Server EHR DB | `dbo.encounters.discharge_timestamp` | `fact_encounter` | `length_of_stay_hours` | Computed difference `(discharge - admission) / 3600` |
| External Claims CSV | `insurance_claims.csv.claim_amount` | `fact_claim` | `claim_amount` | Cast to `NUMERIC(18,2)`, Quality range check `>= 0` |
