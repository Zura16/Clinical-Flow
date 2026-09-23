"""
What each silver table is: its bronze source, its key, and how its columns are derived.

Adding a silver table means adding a spec here. The engine handles the incremental read, the
collapse, the merge and the audit trail identically for all of them.

Column expressions are SQL over the bronze row (or, for FHIR, over the parsed resource). Bronze
keeps every column as text, so casts live here: silver is where types become real.
"""

from databricks.silver.silver_engine import SilverSpec

# Explicit FHIR R4 schemas: only the fields silver uses, so an upstream addition or rename cannot
# silently change a column's type.
FHIR_PATIENT_SCHEMA = (
    "id STRING, meta STRUCT<lastUpdated: STRING>, gender STRING, birthDate STRING, "
    "name ARRAY<STRUCT<family: STRING, given: ARRAY<STRING>>>, "
    "address ARRAY<STRUCT<line: ARRAY<STRING>, city: STRING, state: STRING, postalCode: STRING>>"
)
FHIR_OBSERVATION_SCHEMA = (
    "id STRING, status STRING, effectiveDateTime STRING, "
    "subject STRUCT<reference: STRING>, encounter STRUCT<reference: STRING>, "
    "code STRUCT<coding: ARRAY<STRUCT<system: STRING, code: STRING, display: STRING>>>, "
    "valueQuantity STRUCT<value: DOUBLE, unit: STRING>"
)
FHIR_ENCOUNTER_SCHEMA = (
    "id STRING, status STRING, class STRUCT<code: STRING, display: STRING>, "
    "subject STRUCT<reference: STRING>, "
    "period STRUCT<start: STRING, end: STRING>"
)
FHIR_CONDITION_SCHEMA = (
    "id STRING, onsetDateTime STRING, recordedDate STRING, "
    "subject STRUCT<reference: STRING>, encounter STRUCT<reference: STRING>, "
    "code STRUCT<coding: ARRAY<STRUCT<system: STRING, code: STRING, display: STRING>>>, "
    "clinicalStatus STRUCT<coding: ARRAY<STRUCT<code: STRING>>>"
)

EHR_SPECS = [
    SilverSpec(
        name="silver_ehr_patients",
        bronze_table="bronze_ehr_patients",
        key_columns=["patient_id"],
        columns={
            "patient_id": "patient_id",
            "first_name": "first_name",
            "last_name": "last_name",
            "date_of_birth": "CAST(date_of_birth AS DATE)",
            "gender": "gender",
            "ssn_hash": "ssn_hash",
            "address_street": "address_street",
            "city": "city",
            "state": "state",
            "postal_code": "postal_code",
            "phone_number": "phone_number",
            "insurance_type": "insurance_type",
            "source_updated_at": "CAST(updated_at AS TIMESTAMP)",
        },
        hash_columns=["patient_id", "first_name", "last_name", "date_of_birth", "address_street", "insurance_type"],
        dq_dataset="silver_patients",
        # The source also soft-deletes with its own flag; both paths land in _is_deleted.
        deleted_expr="COALESCE(CAST(is_deleted AS BOOLEAN), false)",
    ),
    SilverSpec(
        name="silver_ehr_encounters",
        bronze_table="bronze_ehr_encounters",
        key_columns=["encounter_id"],
        columns={
            "encounter_id": "encounter_id",
            "patient_id": "patient_id",
            "provider_id": "provider_id",
            "facility_id": "facility_id",
            "department_id": "department_id",
            "encounter_type": "encounter_type",
            "admission_timestamp": "CAST(admission_timestamp AS TIMESTAMP)",
            "discharge_timestamp": "CAST(discharge_timestamp AS TIMESTAMP)",
            "discharge_disposition": "discharge_disposition",
            "source_updated_at": "CAST(updated_at AS TIMESTAMP)",
        },
        hash_columns=["encounter_id", "patient_id", "provider_id", "admission_timestamp", "discharge_timestamp"],
        dq_dataset="silver_encounters",
    ),
    SilverSpec(
        name="silver_ehr_providers",
        bronze_table="bronze_ehr_providers",
        key_columns=["provider_id"],
        columns={
            "provider_id": "provider_id",
            "npi": "npi",
            "first_name": "first_name",
            "last_name": "last_name",
            "specialty": "specialty",
            "department_id": "department_id",
            "facility_id": "facility_id",
            "source_updated_at": "CAST(updated_at AS TIMESTAMP)",
        },
        hash_columns=["provider_id", "npi", "first_name", "last_name", "specialty", "department_id"],
    ),
    SilverSpec(
        name="silver_ehr_diagnoses",
        bronze_table="bronze_ehr_diagnoses",
        key_columns=["diagnosis_id"],
        columns={
            "diagnosis_id": "diagnosis_id",
            "encounter_id": "encounter_id",
            "patient_id": "patient_id",
            "icd10_code": "icd10_code",
            "diagnosis_description": "diagnosis_description",
            "diagnosis_type": "diagnosis_type",
            "diagnosis_timestamp": "CAST(diagnosis_timestamp AS TIMESTAMP)",
            "source_updated_at": "CAST(updated_at AS TIMESTAMP)",
        },
        hash_columns=["diagnosis_id", "encounter_id", "icd10_code", "diagnosis_type"],
        dq_dataset="silver_diagnoses",
    ),
    SilverSpec(
        name="silver_ehr_lab_results",
        bronze_table="bronze_ehr_lab_results",
        key_columns=["lab_result_id"],
        columns={
            "lab_result_id": "lab_result_id",
            "encounter_id": "encounter_id",
            "patient_id": "patient_id",
            "loinc_code": "loinc_code",
            "test_name": "test_name",
            "result_value": "CAST(result_value AS DOUBLE)",
            "result_unit": "result_unit",
            "reference_range": "reference_range",
            "abnormal_flag": "abnormal_flag",
            "result_status": "result_status",
            "order_timestamp": "CAST(order_timestamp AS TIMESTAMP)",
            "result_timestamp": "CAST(result_timestamp AS TIMESTAMP)",
            "source_updated_at": "CAST(updated_at AS TIMESTAMP)",
        },
        hash_columns=["lab_result_id", "loinc_code", "result_value", "result_status", "result_timestamp"],
        dq_dataset="silver_lab_results",
    ),
    SilverSpec(
        name="silver_ehr_medications",
        bronze_table="bronze_ehr_medications",
        key_columns=["medication_order_id"],
        columns={
            "medication_order_id": "medication_order_id",
            "encounter_id": "encounter_id",
            "patient_id": "patient_id",
            "rxnorm_code": "rxnorm_code",
            "medication_name": "medication_name",
            "dosage": "dosage",
            "route": "route",
            "frequency": "frequency",
            "order_status": "order_status",
            "order_timestamp": "CAST(order_timestamp AS TIMESTAMP)",
            "source_updated_at": "CAST(updated_at AS TIMESTAMP)",
        },
        hash_columns=["medication_order_id", "rxnorm_code", "dosage", "frequency", "order_status"],
    ),
]

FHIR_SPECS = [
    SilverSpec(
        name="silver_fhir_patients",
        bronze_table="bronze_fhir_patient",
        key_columns=["patient_id"],
        resource_schema=FHIR_PATIENT_SCHEMA,
        columns={
            "patient_id": "id",
            "first_name": "name[0].given[0]",
            "last_name": "name[0].family",
            "date_of_birth": "CAST(birthDate AS DATE)",
            "gender": "gender",
            "address_street": "address[0].line[0]",
            "city": "address[0].city",
            "state": "address[0].state",
            "postal_code": "address[0].postalCode",
            # The FHIR feed carries no coverage information; inventing one would be a fabricated value.
            "insurance_type": "CAST(NULL AS STRING)",
            "source_updated_at": "CAST(meta.lastUpdated AS TIMESTAMP)",
        },
        hash_columns=["patient_id", "first_name", "last_name", "date_of_birth", "address_street"],
        dq_dataset="silver_patients",
    ),
    SilverSpec(
        name="silver_fhir_observations",
        bronze_table="bronze_fhir_observation",
        key_columns=["observation_id"],
        resource_schema=FHIR_OBSERVATION_SCHEMA,
        columns={
            "observation_id": "id",
            "patient_id": "REPLACE(subject.reference, 'Patient/', '')",
            "encounter_id": "REPLACE(encounter.reference, 'Encounter/', '')",
            "loinc_code": "code.coding[0].code",
            "code_system": "code.coding[0].system",
            "test_name": "code.coding[0].display",
            "result_value": "CAST(valueQuantity.value AS DOUBLE)",
            "result_unit": "valueQuantity.unit",
            "observation_timestamp": "CAST(effectiveDateTime AS TIMESTAMP)",
            "observation_status": "status",
        },
        hash_columns=["observation_id", "patient_id", "loinc_code", "result_value", "observation_timestamp"],
        dq_dataset="silver_observations",
    ),
    SilverSpec(
        name="silver_fhir_encounters",
        bronze_table="bronze_fhir_encounter",
        key_columns=["encounter_id"],
        resource_schema=FHIR_ENCOUNTER_SCHEMA,
        columns={
            "encounter_id": "id",
            "patient_id": "REPLACE(subject.reference, 'Patient/', '')",
            "encounter_class": "class.code",
            "encounter_status": "status",
            "period_start": "CAST(period.start AS TIMESTAMP)",
            "period_end": "CAST(period.end AS TIMESTAMP)",
        },
        hash_columns=["encounter_id", "patient_id", "encounter_class", "period_start", "period_end"],
    ),
    SilverSpec(
        name="silver_fhir_conditions",
        bronze_table="bronze_fhir_condition",
        key_columns=["condition_id"],
        resource_schema=FHIR_CONDITION_SCHEMA,
        columns={
            "condition_id": "id",
            "patient_id": "REPLACE(subject.reference, 'Patient/', '')",
            "encounter_id": "REPLACE(encounter.reference, 'Encounter/', '')",
            "icd10_code": "code.coding[0].code",
            "code_system": "code.coding[0].system",
            "condition_description": "code.coding[0].display",
            "clinical_status": "clinicalStatus.coding[0].code",
            "onset_timestamp": "CAST(onsetDateTime AS TIMESTAMP)",
        },
        hash_columns=["condition_id", "patient_id", "icd10_code", "clinical_status", "onset_timestamp"],
    ),
]

REFERENCE_SPECS = [
    SilverSpec(
        name="silver_claims",
        bronze_table="bronze_claims",
        key_columns=["claim_id"],
        columns={
            "claim_id": "claim_id",
            "patient_id": "patient_id",
            "facility_id": "facility_id",
            "service_date": "CAST(service_date AS DATE)",
            "claim_amount": "CAST(claim_amount AS DOUBLE)",
            "paid_amount": "CAST(paid_amount AS DOUBLE)",
            "claim_status": "claim_status",
            "insurance_type": "insurance_type",
        },
        hash_columns=["claim_id", "patient_id", "service_date", "claim_amount", "claim_status"],
        dq_dataset="silver_claims",
    ),
    SilverSpec(
        name="silver_facilities",
        bronze_table="bronze_facilities",
        key_columns=["facility_id"],
        columns={
            "facility_id": "facility_id",
            "facility_name": "facility_name",
            "facility_type": "facility_type",
            "address": "address",
            "city": "city",
            "state": "state",
            "postal_code": "postal_code",
        },
        hash_columns=["facility_id", "facility_name", "facility_type", "city", "state"],
    ),
]

ALL_SPECS = EHR_SPECS + FHIR_SPECS + REFERENCE_SPECS
