#!/usr/bin/env python3
"""
ClinicalFlow Synthetic Data Generator
Generates multi-source realistic healthcare datasets:
1. Synthea-style FHIR R4 JSON bundles (Patient, Encounter, Observation, Condition, MedicationRequest, Practitioner)
2. SQL EHR relational transactional datasets (patients, encounters, providers, diagnoses, lab_results, medications)
3. External Claims and Reference CSV files (insurance_claims.csv, facility_info.csv)
"""

import os
import json
import uuid
import random
import pandas as pd
from datetime import datetime, timedelta

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FHIR_DIR = os.path.join(DATA_DIR, "fhir_r4")
OUTPUT_EHR_DIR = os.path.join(DATA_DIR, "sql_ehr")
OUTPUT_CLAIMS_DIR = os.path.join(DATA_DIR, "claims_csv")

os.makedirs(OUTPUT_FHIR_DIR, exist_ok=True)
os.makedirs(OUTPUT_EHR_DIR, exist_ok=True)
os.makedirs(OUTPUT_CLAIMS_DIR, exist_ok=True)

# Reference Code Sets
LOINC_CODES = [
    ("883-9", "ABO and Rh group [Type] in Blood", "4.5", "5.5", "10^6/uL"),
    ("718-7", "Hemoglobin [Mass/volume] in Blood", "12.0", "17.5", "g/dL"),
    ("4544-3", "Hematocrit [Volume Fraction] of Blood", "36.0", "50.0", "%"),
    ("2345-7", "Glucose [Mass/volume] in Serum or Plasma", "70.0", "99.0", "mg/dL"),
    ("2160-0", "Creatinine [Mass/volume] in Serum or Plasma", "0.6", "1.3", "mg/dL"),
    ("2093-3", "Cholesterol [Mass/volume] in Serum or Plasma", "125.0", "200.0", "mg/dL"),
    ("2085-9", "HDL Cholesterol", "40.0", "60.0", "mg/dL"),
    ("13457-7", "LDL Cholesterol", "50.0", "100.0", "mg/dL")
]

ICD10_CODES = [
    ("E11.9", "Type 2 diabetes mellitus without complications", "Endocrine"),
    ("I10", "Essential (primary) hypertension", "Cardiovascular"),
    ("J44.9", "Chronic obstructive pulmonary disease, unspecified", "Respiratory"),
    ("E78.5", "Hyperlipidemia, unspecified", "Endocrine"),
    ("K21.9", "Gastro-esophageal reflux disease without esophagitis", "Gastrointestinal"),
    ("M54.50", "Low back pain, unspecified", "Musculoskeletal"),
    ("F41.1", "Generalized anxiety disorder", "Mental Health"),
    ("J06.9", "Acute upper respiratory infection, unspecified", "Respiratory")
]

RXNORM_CODES = [
    ("860975", "Metformin hydrochloride 500 MG Oral Tablet", "500mg", "Oral", "BID"),
    ("197361", "Amlodipine 5 MG Oral Tablet", "5mg", "Oral", "Daily"),
    ("311354", "Lisinopril 10 MG Oral Tablet", "10mg", "Oral", "Daily"),
    ("617314", "Atorvastatin 20 MG Oral Tablet", "20mg", "Oral", "Daily"),
    ("198211", "Omeprazole 20 MG Delayed Release Oral Capsule", "20mg", "Oral", "Daily"),
    ("310965", "Levothyroxine Sodium 0.05 MG Oral Tablet", "50mcg", "Oral", "Daily")
]

FIRST_NAMES = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"]
CITIES = ["Seattle", "Portland", "San Francisco", "Los Angeles", "Chicago", "Boston", "New York", "Austin", "Denver", "Phoenix"]
STATES = ["WA", "OR", "CA", "IL", "MA", "NY", "TX", "CO", "AZ"]
SPECIALTIES = ["Internal Medicine", "Cardiology", "Family Practice", "Pulmonology", "Endocrinology", "Emergency Medicine", "Orthopedics"]
FACILITY_NAMES = ["Main Street Medical Center", "Valley General Hospital", "St. Jude Regional Hospital", "Northwest Health Pavilion", "Mercy Care Center"]

def random_date(start_year=1950, end_year=2024):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 1, 1)
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

def generate_fhir_r4_bundle(num_patients=1000, num_obs_per_patient=10):
    """Generates Synthea-like FHIR R4 JSON resource bundles"""
    print(f"Generating {num_patients} FHIR R4 Patient records and associated resources...")
    resources = []
    
    for i in range(num_patients):
        patient_id = f"fhir-pat-{i+1:06d}"
        fn = random.choice(FIRST_NAMES)
        ln = random.choice(LAST_NAMES)
        dob = random_date(1950, 2020).strftime("%Y-%m-%d")
        gender = random.choice(["male", "female"])
        
        # FHIR Patient
        pat_res = {
            "resourceType": "Patient",
            "id": patient_id,
            "meta": {"lastUpdated": datetime.utcnow().isoformat() + "Z"},
            "identifier": [{"system": "http://hospital.epic.sim/patients", "value": patient_id}],
            "name": [{"use": "official", "family": ln, "given": [fn]}],
            "gender": gender,
            "birthDate": dob,
            "address": [{"line": [f"{random.randint(100, 9999)} Oak St"], "city": random.choice(CITIES), "state": random.choice(STATES), "postalCode": f"{random.randint(10000, 99999)}"}]
        }
        resources.append(pat_res)
        
        # Encounters
        for j in range(random.randint(1, 4)):
            enc_id = f"fhir-enc-{patient_id}-{j+1}"
            enc_start = random_date(2023, 2026)
            enc_end = enc_start + timedelta(hours=random.randint(2, 72))
            
            enc_res = {
                "resourceType": "Encounter",
                "id": enc_id,
                "meta": {"lastUpdated": enc_start.isoformat() + "Z"},
                "status": "finished",
                "class": {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": random.choice(["IMP", "AMB", "EMER"])},
                "subject": {"reference": f"Patient/{patient_id}"},
                "period": {"start": enc_start.isoformat() + "Z", "end": enc_end.isoformat() + "Z"}
            }
            resources.append(enc_res)
            
            # Observations
            for k in range(num_obs_per_patient):
                obs_id = f"fhir-obs-{enc_id}-{k+1}"
                loinc, name, min_val, max_val, unit = random.choice(LOINC_CODES)
                obs_val = round(random.uniform(float(min_val)*0.8, float(max_val)*1.2), 2)
                obs_time = enc_start + timedelta(minutes=random.randint(5, 120))
                
                obs_res = {
                    "resourceType": "Observation",
                    "id": obs_id,
                    "meta": {"lastUpdated": obs_time.isoformat() + "Z"},
                    "status": "final",
                    "code": {
                        "coding": [{"system": "http://loinc.org", "code": loinc, "display": name}],
                        "text": name
                    },
                    "subject": {"reference": f"Patient/{patient_id}"},
                    "encounter": {"reference": f"Encounter/{enc_id}"},
                    "effectiveDateTime": obs_time.isoformat() + "Z",
                    "valueQuantity": {"value": obs_val, "unit": unit, "system": "http://unitsofmeasure.org"}
                }
                resources.append(obs_res)
                
            # Conditions
            icd, desc, _ = random.choice(ICD10_CODES)
            cond_res = {
                "resourceType": "Condition",
                "id": f"fhir-cond-{enc_id}",
                "meta": {"lastUpdated": enc_start.isoformat() + "Z"},
                "clinicalStatus": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-clinical", "code": "active"}]},
                "code": {"coding": [{"system": "http://hl7.org/fhir/sid/icd-10-cm", "code": icd, "display": desc}]},
                "subject": {"reference": f"Patient/{patient_id}"},
                "encounter": {"reference": f"Encounter/{enc_id}"},
                "onsetDateTime": enc_start.isoformat() + "Z"
            }
            resources.append(cond_res)

    bundle = {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": [{"resource": r} for r in resources]
    }
    
    file_path = os.path.join(OUTPUT_FHIR_DIR, "fhir_r4_synthetic_bundle.json")
    with open(file_path, "w") as f:
        json.dump(bundle, f, indent=2)
    print(f"Saved FHIR R4 bundle to {file_path} with {len(resources)} total resources.")

def generate_ehr_sql_csvs(num_patients=5000):
    """Generates relational SQL EHR transactional datasets"""
    print(f"Generating EHR relational datasets ({num_patients} patients)...")
    
    patients = []
    providers = []
    encounters = []
    diagnoses = []
    lab_results = []
    medications = []
    
    # Providers
    for p in range(50):
        prov_id = f"PRV-{p+1:04d}"
        providers.append({
            "provider_id": prov_id,
            "npi": f"{1000000000 + p + 1}",
            "first_name": random.choice(FIRST_NAMES),
            "last_name": random.choice(LAST_NAMES),
            "specialty": random.choice(SPECIALTIES),
            "department_id": f"DEPT-{random.randint(101, 110)}",
            "facility_id": f"FAC-{random.randint(1, 5)}",
            "created_at": "2023-01-01 00:00:00",
            "updated_at": "2023-01-01 00:00:00"
        })
        
    for i in range(num_patients):
        pat_id = f"EHR-PAT-{i+1:06d}"
        fn = random.choice(FIRST_NAMES)
        ln = random.choice(LAST_NAMES)
        dob = random_date(1950, 2020).strftime("%Y-%m-%d")
        gender = random.choice(["Male", "Female"])
        phone = f"555-{random.randint(100,999):03d}-{random.randint(1000,9999):04d}"
        ins = random.choice(["Medicare", "Medicaid", "Commercial", "Uninsured"])
        created_time = random_date(2023, 2025)
        
        patients.append({
            "patient_id": pat_id,
            "first_name": fn,
            "last_name": ln,
            "date_of_birth": dob,
            "gender": gender,
            "ssn_hash": f"hash-{uuid.uuid4().hex[:16]}",
            "address_street": f"{random.randint(100, 9999)} Elm St",
            "city": random.choice(CITIES),
            "state": random.choice(STATES),
            "postal_code": f"{random.randint(10000, 99999)}",
            "phone_number": phone,
            "insurance_type": ins,
            "created_at": created_time.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": created_time.strftime("%Y-%m-%d %H:%M:%S"),
            "is_deleted": 0
        })
        
        for j in range(random.randint(1, 3)):
            enc_id = f"ENC-{pat_id}-{j+1}"
            prov = random.choice(providers)
            adm_time = created_time + timedelta(days=random.randint(1, 60))
            dis_time = adm_time + timedelta(hours=random.randint(4, 96))
            enc_type = random.choice(["Inpatient", "Outpatient", "Emergency"])
            
            encounters.append({
                "encounter_id": enc_id,
                "patient_id": pat_id,
                "provider_id": prov["provider_id"],
                "facility_id": prov["facility_id"],
                "encounter_type": enc_type,
                "admission_timestamp": adm_time.strftime("%Y-%m-%d %H:%M:%S"),
                "discharge_timestamp": dis_time.strftime("%Y-%m-%d %H:%M:%S"),
                "discharge_disposition": random.choice(["Home", "Skilled Nursing", "AMA", "Expired"]),
                "department_id": prov["department_id"],
                "created_at": adm_time.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_at": adm_time.strftime("%Y-%m-%d %H:%M:%S")
            })
            
            # Diagnoses
            icd, desc, _ = random.choice(ICD10_CODES)
            diagnoses.append({
                "diagnosis_id": f"DX-{enc_id}",
                "encounter_id": enc_id,
                "patient_id": pat_id,
                "icd10_code": icd,
                "diagnosis_description": desc,
                "diagnosis_type": random.choice(["Primary", "Secondary", "Admitting"]),
                "diagnosis_timestamp": adm_time.strftime("%Y-%m-%d %H:%M:%S"),
                "created_at": adm_time.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_at": adm_time.strftime("%Y-%m-%d %H:%M:%S")
            })
            
            # Lab results
            for k in range(random.randint(1, 4)):
                loinc, name, min_val, max_val, unit = random.choice(LOINC_CODES)
                val = round(random.uniform(float(min_val)*0.8, float(max_val)*1.2), 2)
                lab_time = adm_time + timedelta(minutes=random.randint(10, 240))
                
                lab_results.append({
                    "lab_result_id": f"LAB-{enc_id}-{k+1}",
                    "encounter_id": enc_id,
                    "patient_id": pat_id,
                    "loinc_code": loinc,
                    "test_name": name,
                    "result_value": val,
                    "result_unit": unit,
                    "reference_range": f"{min_val}-{max_val}",
                    "abnormal_flag": random.choice(["Normal", "Normal", "High", "Low"]),
                    "result_status": "Final",
                    "order_timestamp": adm_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "result_timestamp": lab_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "created_at": lab_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "updated_at": lab_time.strftime("%Y-%m-%d %H:%M:%S")
                })
                
            # Medications
            rx, rx_name, dose, route, freq = random.choice(RXNORM_CODES)
            medications.append({
                "medication_order_id": f"MED-{enc_id}",
                "encounter_id": enc_id,
                "patient_id": pat_id,
                "rxnorm_code": rx,
                "medication_name": rx_name,
                "dosage": dose,
                "route": route,
                "frequency": freq,
                "order_status": "Active",
                "order_timestamp": adm_time.strftime("%Y-%m-%d %H:%M:%S"),
                "created_at": adm_time.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_at": adm_time.strftime("%Y-%m-%d %H:%M:%S")
            })
            
    pd.DataFrame(patients).to_csv(os.path.join(OUTPUT_EHR_DIR, "patients.csv"), index=False)
    pd.DataFrame(providers).to_csv(os.path.join(OUTPUT_EHR_DIR, "providers.csv"), index=False)
    pd.DataFrame(encounters).to_csv(os.path.join(OUTPUT_EHR_DIR, "encounters.csv"), index=False)
    pd.DataFrame(diagnoses).to_csv(os.path.join(OUTPUT_EHR_DIR, "diagnoses.csv"), index=False)
    pd.DataFrame(lab_results).to_csv(os.path.join(OUTPUT_EHR_DIR, "lab_results.csv"), index=False)
    pd.DataFrame(medications).to_csv(os.path.join(OUTPUT_EHR_DIR, "medications.csv"), index=False)
    
    print(f"Saved EHR CSV tables in {OUTPUT_EHR_DIR}")

def generate_claims_csv(num_claims=3000):
    """Generates external Claims and Reference CSV files"""
    print(f"Generating {num_claims} Claims records and Facility reference data...")
    
    facilities = []
    for f in range(5):
        facilities.append({
            "facility_id": f"FAC-{f+1}",
            "facility_name": FACILITY_NAMES[f],
            "facility_type": random.choice(["General Hospital", "Academic Medical Center", "Urgent Care"]),
            "address": f"{random.randint(100, 9999)} Hospital Way",
            "city": random.choice(CITIES),
            "state": random.choice(STATES),
            "postal_code": f"{random.randint(10000, 99999)}"
        })
    pd.DataFrame(facilities).to_csv(os.path.join(OUTPUT_CLAIMS_DIR, "facility_info.csv"), index=False)
    
    claims = []
    for c in range(num_claims):
        svc_date = random_date(2023, 2026)
        claim_amt = round(random.uniform(150.0, 15000.0), 2)
        paid_amt = round(claim_amt * random.uniform(0.7, 0.95), 2)
        
        claims.append({
            "claim_id": f"CLM-{c+1:07d}",
            "patient_id": f"EHR-PAT-{random.randint(1, 5000):06d}",
            "facility_id": f"FAC-{random.randint(1, 5)}",
            "service_date": svc_date.strftime("%Y-%m-%d"),
            "claim_amount": claim_amt,
            "paid_amount": paid_amt,
            "claim_status": random.choice(["Paid", "Approved", "Denied", "Pending"]),
            "insurance_type": random.choice(["Medicare", "Medicaid", "Commercial"])
        })
    pd.DataFrame(claims).to_csv(os.path.join(OUTPUT_CLAIMS_DIR, "insurance_claims.csv"), index=False)
    print(f"Saved Claims and Facilities CSVs in {OUTPUT_CLAIMS_DIR}")

if __name__ == "__main__":
    generate_fhir_r4_bundle(num_patients=500, num_obs_per_patient=5)
    generate_ehr_sql_csvs(num_patients=1000)
    generate_claims_csv(num_claims=1500)
    print("All synthetic healthcare data generation completed successfully!")
