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
