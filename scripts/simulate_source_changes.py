#!/usr/bin/env python3
"""
Apply inserts, updates and deletes to the EHR source so CDC has something to capture.

    python -m scripts.simulate_source_changes [--patients 5]

Used by the demo and by tests: run it between two bronze runs to show that the second run lands
only the changed rows, carries deletes, and does not re-read the unchanged ones.
"""

import argparse
import time
import uuid

from databricks.utilities import sqlserver as mssql


def apply_changes(patients: int = 5) -> dict:
    """Move some patients, admit a new one, and delete one. Returns what changed."""
    movers = [r["patient_id"] for r in mssql.query(
        f"SELECT TOP {patients} patient_id FROM dbo.patients ORDER BY patient_id")]
    if not movers:
        raise RuntimeError("no patients in the source database; run scripts.setup_source_db first")

    new_id = f"EHR-PAT-NEW-{uuid.uuid4().hex[:8].upper()}"
    # Delete a patient who actually has clinical records, so the erasure has to cascade.
    doomed = mssql.scalar(
        "SELECT TOP 1 p.patient_id FROM dbo.patients p "
        "WHERE EXISTS (SELECT 1 FROM dbo.encounters e WHERE e.patient_id = p.patient_id) "
        "ORDER BY p.patient_id DESC"
    )

    ids = "', '".join(movers)
    mssql.execute(
        f"UPDATE dbo.patients SET address_street = CONCAT('Moved ', address_street), "
        f"updated_at = SYSUTCDATETIME() WHERE patient_id IN ('{ids}')"
    )
    mssql.execute(
        "INSERT INTO dbo.patients (patient_id, first_name, last_name, date_of_birth, gender, "
        "address_street, city, state, postal_code, insurance_type, created_at, updated_at, is_deleted) "
        f"VALUES ('{new_id}', 'New', 'Arrival', '1990-05-05', 'Female', '1 Admission Way', 'Seattle', "
        "'WA', '98101', 'Commercial', SYSUTCDATETIME(), SYSUTCDATETIME(), 0)"
    )
    # A hard delete, the way a real erasure has to happen: children first, because the foreign
    # keys refuse to orphan clinical records. The deletes flow through five CDC captures.
    for statement in [
        f"DELETE FROM dbo.lab_results WHERE patient_id = '{doomed}'",
        f"DELETE FROM dbo.medications WHERE patient_id = '{doomed}'",
        f"DELETE FROM dbo.diagnoses WHERE patient_id = '{doomed}'",
        f"DELETE FROM dbo.encounters WHERE patient_id = '{doomed}'",
        f"DELETE FROM dbo.patients WHERE patient_id = '{doomed}'",
    ]:
        mssql.execute(statement)

    return {"updated": movers, "inserted": new_id, "deleted": doomed}


def wait_for_capture(timeout_seconds: int = 90) -> str:
    """Wait for the capture job to move the max LSN past where it was before the changes."""
    before = mssql.max_lsn()
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        current = mssql.max_lsn()
        if current and current != before:
            return current
        time.sleep(3)
    return mssql.max_lsn()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--patients", type=int, default=5, help="how many patients to move")
    args = parser.parse_args()

    if not mssql.is_available():
        raise SystemExit("cannot reach SQL Server; is `docker compose up -d sqlserver` running?")
    changed = apply_changes(args.patients)
    print(f"updated {len(changed['updated'])} patients, inserted {changed['inserted']}, deleted {changed['deleted']}")
    print(f"capture caught up to LSN {wait_for_capture()}")
