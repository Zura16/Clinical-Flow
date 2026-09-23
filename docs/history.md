# History

Dated journal of what landed. Newest last.

## 2026-09-21

- Gap analysis of the existing scaffold against the ClinicalFlow spec; fix plan recorded in `CLAUDE.md`.
- Added `CLAUDE.md`, `docs/decisions.md`, `docs/history.md`.
- Removed the automated staggered-commit bot.
- Enabled Delta Lake for real (was silently writing Parquet); removed format fallbacks. All 21 tables verified to have `_delta_log`; 5/5 tests pass (34 s → 144 s).
- Fix plan step 3 (bronze) landed:
  - Tests run in a temp lakehouse (`CLINICALFLOW_LAKEHOUSE`), and the Spark session is pinned to UTC.
  - `pipeline_config` and `watermark_state` control tables (`databricks/utilities/control.py`). The SQL DDL gains `source_location` and `watermark_state`.
  - Bronze rewritten: append-only, partitioned by run, 14 config-driven tables, raw FHIR JSON, per-table watermarks, loud failure on bad watermarks, `FAILED`/`SKIPPED` audit rows.
  - Silver reads current state from bronze through `bronze_reader.py`. FHIR silver uses explicit schemas.
  - Finding: the first replay design lost bronze history when the source had changed. Replaced with skip-if-succeeded / retry-if-failed (see decisions).
  - ADF ingestion pipeline passes source, table and RunId.
  - README and runbook corrected to match the code, with unbuilt features marked *(planned)*.
  - Tests: 12 pass (6:33). Real lakehouse rebuilt end to end; a second bronze run lands 0 rows on all 12 watermark tables.

## 2026-09-22

- Fix plan step 2 (real sources) landed:
  - SQL Server 2022 in Docker with the SQL Agent and CDC enabled on all six EHR tables; `scripts/setup_source_db.py` applies the DDL and BULK INSERTs the extracts.
  - Bronze reads CDC through Spark JDBC with LSN watermarks: snapshot on first load, then inserts/updates/deletes. Retention gaps fail loudly.
  - Silver resolves CDC current state by log position and drops deleted keys; bronze keeps the delete rows.
  - `scripts/simulate_source_changes.py` applies inserts/updates/deletes (cascading through the FK chain) for demos and tests.
  - Generator scaled to ~366k records and FHIR split across 22 bundle files.
  - Verified: bronze current state reconciles exactly to SQL Server for all six tables (e.g. patients bronze 1,020 rows / current 1,001 = source 1,001 after an insert and a cascading delete).
  - Finding: the second bronze run (0 new rows) is *slower* than the first, because file-based watermark sources still read everything. See decisions.
  - Tests: 13 pass; the CDC test skips when SQL Server isn't running.
- Fix plan step 4 (silver) landed:
  - Incremental silver: stage watermarks over bronze `_ingested_at`, batch collapse, Delta MERGE with a version guard.
  - Soft deletes from both CDC operation 1 and `WHEN NOT MATCHED BY SOURCE` on full snapshots; gold reads current rows through `silver_reader`.
  - Silver coverage 5 -> 12 tables via declarative specs; `dim_facility` builds for the first time.
  - Measured: 20,000-row first load, then 7 rows after 5 source changes; 0 rows when nothing changed.
  - Fixed: untyped `lit(None)` placeholders in `dim_patient` (VOID columns vanish on write, breaking the next run).
  - Before that: profiled and cut idle-run cost ~20% (config seeding was re-running a MERGE per lookup). The predicted win (skipping empty writes) did nothing; the decisions log records both.
  - Tests: 17 pass (7 integration, 10 unit).
- Fix plan step 5 (data quality) landed:
  - Rules moved into `data_quality_rule` (27 seeded); added UNIQUE, REFERENTIAL, REGEX, FRESHNESS.
  - Severity decides the row (quarantine vs warn), threshold decides the run (raises before merge).
  - Quarantine keyed by sha256(run, dataset, record, rule) and merged: reruns no longer duplicate.
  - New `data_quality_result` table records every rule's outcome, passes included.
  - Rules skip soft-deleted rows (found on real data: the flagged rows were a deleted patient's own deletions).
  - **Found a daylight-saving bug via a quality rule**: JDBC was reading zone-less SQL Server timestamps in the JVM's local zone, shifting every EHR timestamp and breaking one row across the 2024-03-10 DST boundary. CDC reads now convert date/time columns to ISO text in SQL. Silver timestamps now match the source exactly. See decisions.
  - Tests: 23 pass (~16 min).
