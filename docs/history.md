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
