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

## 2026-09-26

- Fix plan step 6 (gold) landed:
  - Deterministic `xxhash64` surrogate keys (stable across rebuilds); unknown member (-1) in every dimension.
  - New dimensions: provider, diagnosis, medication, department; `dim_facility` now has a source.
  - dim_patient is real SCD2: contiguous version windows, first version opens at 1900-01-01, deletions open a version, `source_system` distinguishes EHR from FHIR members.
  - Facts join point-in-time and merge on business key; new `fact_diagnosis` and `fact_medication_order`; `fact_observation` covers EHR labs + FHIR observations.
  - Fabricated measures removed: turnaround computed from order-to-result (232 distinct values, was hard-coded 15), readmission computed from each patient's previous discharge.
  - Verified at ~316k fact rows: 0 orphans, 0 NULL keys, 0 unknown members; a moved patient's old encounters still resolve to the pre-move version.
  - Three bugs found and fixed (first-version dating, an SCD2 lineage trap that inserted nothing, a test clobbering shared silver). See decisions.
  - Tests: 30 pass (~10 min). The whole-pipeline test now skips when SQL Server is down.
- Fix plan step 7 (failure and recovery) landed:
  - `pipeline_alert` table + `raise_alert`: printed alert, recorded row, optional webhook; wired into the bronze and silver failure paths. Alerting can never mask the failure it reports.
  - `failure_simulation.py` rewritten: corrupts 500 lab results in SQL Server, CDC carries them, silver fails a real threshold, evidence is shown (FAILED audit, alert, quarantine, rule results), the source is repaired, the stage replays, and six checks verify nothing was duplicated or lost. Self-cleaning on abort.
  - Recovery needs no replay command: the stage watermark does not advance on failure, so a rerun reprocesses the same bronze batch.
  - `troubleshooting-guide.md` rewritten as five real on-call scenarios (threshold breach, bronze failure, CDC retention gap, schema drift, timestamp confusion).
  - New integration test covers fail -> evidence -> fix -> replay -> no duplicates without needing SQL Server.

## 2026-09-28

- Fix plan step 8 (CI, lint, maintenance) landed locally, pending its first GitHub run:
  - `pyproject.toml` (ruff + black, 120 cols), `requirements-dev.txt`; repo is ruff- and black-clean. Lint fixes removed dead imports and three unused parameters; no behaviour change (AST-checked).
  - `.github/workflows/ci.yml`: lint + full suite on a small generated dataset; SQL Server tests skip with reasons. Simulated locally in a clean copy: 29 passed, 2 skipped, 2:25.
  - `.github/workflows/nightly-integration.yml`: SQL Server service container with Agent (CDC), loader falls back to client-side inserts when `/data` is not mounted, then the full suite and the failure demo.
  - `scripts/maintain_lakehouse.py`: OPTIMIZE always, VACUUM only on `--vacuum`. Active files 481 -> 75, row counts identical on all 45 tables. Bronze does not compact (partitioned by run ID) - see decisions.

## 2026-09-29

- Fix plan step 8 proven on GitHub, completing the fix plan:
  - `ci.yml` green on the PR (29 passed, 2 skipped with reasons, 5:55 of tests).
  - `nightly-integration.yml` green end to end: SQL Server container, client-side load fallback, 31 tests with 0 skipped, full lakehouse build, failure demo with all nine checks passing.
  - Fixed along the way: the demo now refuses to start without a built lakehouse; the nightly builds it first; the jar cache now points at Spark 4's `~/.ivy2.5.2`.
  - Troubleshooting scenario 6: a weak SA password kills the service container before any step runs.

## 2026-10-03

- FileIncremental ingestion for FHIR (first post-fix-plan item):
  - Found: a late FHIR export with older `meta_lastUpdated` values landed 0 rows with a SUCCESS audit row. Reproduced as a failing test before the fix.
  - `metadata/file_ingest_log` + `databricks/utilities/file_log.py`: files listed with `binaryFile` (no content read), pending = not committed for this table, committed = run has a SUCCESS audit row.
  - Silver versions FileIncremental rows by `meta_lastUpdated` (mutation-verified test); `current_state` rejects unknown types; `ingest_table` rejects unknown types instead of crashing.
  - Real lakehouse: migration run re-landed all FHIR once (silver inserted 0, updated 0, counts identical); second run read 0 of 22 files, 51 s -> 31 s.
