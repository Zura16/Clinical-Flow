#!/usr/bin/env python3
"""
ClinicalFlow Daily Staggered Git Committer & Pusher
Schedule 10% of total repository files each day, creating a distinct commit for EVERY individual file.
Repository target: https://github.com/Zura16/Clinical-Flow.git
"""

import os
import sys
import json
import subprocess

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE_FILE = os.path.join(REPO_DIR, ".git_schedule_state.json")

# Schedule breakdown: 35 files mapped across 10 Days (~10% per day), each file having its own individual commit.
FILE_SCHEDULE = [
    # Day 1: Core Repo Configuration & Environment (10%)
    {
        "day": 1,
        "files": [
            (".gitignore", "chore: add .gitignore for workspace artifacts"),
            ("requirements.txt", "build: add python dependencies configuration"),
            ("docker-compose.yml", "ci: add local SQL Server and PySpark docker compose testbed"),
            ("infrastructure/terraform/variables.tf", "infra: add Terraform input variables for Azure deployment"),
            ("scripts/daily_pusher.py", "ci: add daily staggered git commit and push automation script")
        ]
    },
    # Day 2: Infrastructure & Documentation (10%)
    {
        "day": 2,
        "files": [
            ("infrastructure/terraform/main.tf", "infra: add Terraform main module provisioning ADLS Gen2, Azure SQL DB, ADF, and Databricks"),
            ("README.md", "docs: add comprehensive platform architecture and execution documentation"),
            ("architecture/design-decisions.md", "docs: add technical rationale for Medallion architecture and SCD Type 2")
        ]
    },
    # Day 3: Architecture & Data Mapping Documentation (10%)
    {
        "day": 3,
        "files": [
            ("architecture/data-flow.md", "docs: add Medallion data flow architecture diagram and layer specifications"),
            ("docs/data-dictionary.md", "docs: add complete metadata and warehouse data dictionary"),
            ("docs/source-to-target-mapping.md", "docs: add source to target field mapping matrix")
        ]
    },
    # Day 4: Runbook & Source EHR Database Schema (10%)
    {
        "day": 4,
        "files": [
            ("docs/pipeline-runbook.md", "docs: add operational pipeline runbook and SLA guidelines"),
            ("docs/troubleshooting-guide.md", "docs: add troubleshooting and failure recovery guide"),
            ("sql/source/01_ehr_source_schema.sql", "feat(sql): add transactional EHR source database schema with CDC")
        ]
    },
    # Day 5: Quality Metadata & Star Schema DW (10%)
    {
        "day": 5,
        "files": [
            ("sql/quality/01_data_quality_framework.sql", "feat(sql): add metadata control, data quality rule engine, quarantine, and audit tables"),
            ("sql/warehouse/01_gold_star_schema.sql", "feat(sql): add Gold star schema data warehouse and SCD Type 2 patient dimension"),
            ("sample-data/generate_clinical_data.py", "feat(data): add synthetic FHIR R4 JSON, EHR SQL, and Claims CSV data generator")
        ]
    },
    # Day 6: ADF Orchestration & Databricks Config (10%)
    {
        "day": 6,
        "files": [
            ("adf/pipelines/pl_metadata_driven_ingestion.json", "feat(adf): add metadata driven dynamic ingestion ADF pipeline"),
            ("adf/pipelines/pl_databricks_medallion_orchestrator.json", "feat(adf): add Medallion orchestrator ADF pipeline"),
            ("databricks/utilities/config.py", "feat(pyspark): add lakehouse configuration, hashing, and storage abstractions"),
            ("databricks/utilities/logger.py", "feat(pyspark): add structured pipeline run audit logger")
        ]
    },
    # Day 7: Data Quality Engine & Silver Processing (10%)
    {
        "day": 7,
        "files": [
            ("databricks/utilities/quality_engine.py", "feat(pyspark): add dynamic data quality rule engine and quarantine router"),
            ("databricks/bronze/ingest_raw_data.py", "feat(pyspark): add metadata-driven Bronze raw ingestion engine"),
            ("databricks/silver/process_fhir_silver.py", "feat(pyspark): add nested FHIR R4 JSON flattener and silver processor")
        ]
    },
    # Day 8: Silver Relational & Gold Dimension/Fact Builders (10%)
    {
        "day": 8,
        "files": [
            ("databricks/silver/process_relational_silver.py", "feat(pyspark): add EHR relational CDC and Claims silver processor"),
            ("databricks/gold/build_dimensions.py", "feat(pyspark): add Gold star schema dimension builder with SCD Type 2 patient tracking"),
            ("databricks/gold/build_facts.py", "feat(pyspark): add Gold star schema fact table builder with late-arriving dimension handling"),
            ("databricks/utilities/failure_simulation.py", "feat(pyspark): add controlled failure injection and recovery demonstration script")
        ]
    },
    # Day 9: Unit & Integration Test Suite (10%)
    {
        "day": 9,
        "files": [
            ("tests/unit/test_fhir_parser.py", "test: add FHIR R4 parser and record hashing unit tests"),
            ("tests/unit/test_quality_engine.py", "test: add Data Quality engine and quarantine routing unit tests"),
            ("tests/unit/test_scd2_logic.py", "test: add SCD Type 2 record hash comparison unit tests"),
            ("tests/integration/test_pipeline_idempotency.py", "test: add pipeline rerun idempotency integration test")
        ]
    },
    # Day 10: Reconciliation Tests & Analytical Dashboards (10%)
    {
        "day": 10,
        "files": [
            ("tests/reconciliation/test_reconciliation.py", "test: add source to target row count reconciliation test"),
            ("dashboards/clinical_operations_queries.sql", "feat(analytics): add Clinical Operations SQL analytical queries"),
            ("dashboards/data_quality_queries.sql", "feat(analytics): add Data Quality and Quarantine metrics SQL queries"),
            ("dashboards/pipeline_observability_queries.sql", "feat(analytics): add Pipeline Observability and Latency SQL queries")
        ]
    }
]

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {"current_day": 0, "completed_days": []}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def run_cmd(cmd):
    print(f"Executing: {cmd}")
    res = subprocess.run(cmd, shell=True, cwd=REPO_DIR, text=True, capture_output=True)
    if res.returncode != 0:
        print(f"Error: {res.stderr}")
    else:
        print(f"Output: {res.stdout.strip()}")
    return res

def push_day_batch(day_num):
    day_config = next((d for d in FILE_SCHEDULE if d["day"] == day_num), None)
    if not day_config:
        print(f"Day {day_num} not found in schedule configuration.")
        return False

    print(f"\n=======================================================")
    print(f"    RUNNING DAY {day_num} STAGGERED GIT COMMIT & PUSH    ")
    print(f"=======================================================\n")

    for file_path, commit_msg in day_config["files"]:
        abs_path = os.path.join(REPO_DIR, file_path)
        if os.path.exists(abs_path):
            run_cmd(f"git add '{file_path}'")
            run_cmd(f"git commit -m '{commit_msg}'")
        else:
            print(f"Warning: File {file_path} not found.")

    # Push all commits for this day to GitHub origin main
    res = run_cmd("git push origin main")
    return res.returncode == 0

if __name__ == "__main__":
    state = load_state()
    next_day = state["current_day"] + 1
    
    if len(sys.argv) > 1:
        try:
            next_day = int(sys.argv[1])
        except ValueError:
            pass

    if next_day > len(FILE_SCHEDULE):
        print("All 10 days of staggered file commits have already been pushed!")
        sys.exit(0)

    success = push_day_batch(next_day)
    if success:
        state["current_day"] = next_day
        state["completed_days"].append(next_day)
        save_state(state)
        print(f"\nSUCCESS: Day {next_day} commits successfully pushed to GitHub!")
    else:
        print(f"\nFAIL: Failed to push Day {next_day} to remote repository.")
