# CLAUDE.md — ClinicalFlow

Auto-loaded every session. This file is the operative context: goals, hard rules, commands, and current status. Deep detail lives in the docs — read them on demand, don't guess:

| Doc | What it holds | Read it when… |
|---|---|---|
| [docs/decisions.md](docs/decisions.md) | Every non-obvious choice + *why* + what changes at scale | Before re-deciding anything; when asked "why did we…" |
| [docs/history.md](docs/history.md) | Dated journal of what landed | You need to know how/when something was built |
| [architecture/design-decisions.md](architecture/design-decisions.md) | High-level rationale (medallion, SCD2) | Framing the architecture |
| [architecture/data-flow.md](architecture/data-flow.md) | Layer-by-layer data flow | Tracing a record end to end |
| [docs/data-dictionary.md](docs/data-dictionary.md) · [docs/source-to-target-mapping.md](docs/source-to-target-mapping.md) | Column definitions and source→target lineage | Adding or changing a column |
| [docs/pipeline-runbook.md](docs/pipeline-runbook.md) · [docs/troubleshooting-guide.md](docs/troubleshooting-guide.md) | Ops procedures | Running, replaying, or debugging |

---

## What ClinicalFlow is

A **fault-tolerant, Epic-inspired FHIR healthcare lakehouse** built as a portfolio project for an entry-level data engineering role (Python, SQL Server, ADF, Databricks/PySpark, data warehousing, healthcare/Epic). It ingests synthetic clinical data from three sources, processes incremental changes, validates quality, keeps warehouse history, and supports monitoring, failure recovery and analytics.

```
FHIR R4 JSON ─┐
SQL Server EHR (CDC) ─┼─► BRONZE (append-only, partitioned) ─► SILVER (PySpark: parse, validate, dedupe, mask, MERGE)
Claims / reference CSV ┘                                        ─► GOLD (star schema, SCD2 dim_patient, facts) ─► SQL dashboards
Cross-cutting: pipeline_config (metadata) · data_quality_rule + quarantine · pipeline_run_audit · ADF JSON · Terraform (Azure) · pytest
```

**The hard part is the point:** a metadata-driven, incremental, idempotent pipeline that correctly handles inserts, updates, deletes, duplicates, late-arriving data, schema changes, failures and replay. Dashboards stay modest.

## Working contract

1. **Explain while doing.** For every non-trivial choice: the pattern, the tradeoff, the alternative rejected, and what changes at real scale. Never land code the developer can't defend in an interview.
2. **Flag production-grade vs. simplified** explicitly (local Spark instead of Databricks, local Delta instead of ADLS, ADF JSON that is authored but not deployed…). Naming the gap *is* the skill.
3. **No silent shortcuts.** Skipped error handling, idempotency or tests must be called out and logged in `decisions.md`.
4. **Small, reviewable changes.** One concept per commit, conventional-commit prefix + imperative mood (`fix(silver): merge on business key instead of overwrite`).
5. **Plan first for anything spanning multiple layers.** Ask before big architectural moves.
6. **Bank the finding.** Surprising or negative results go in `decisions.md` honestly.
7. **End every explanation with the interview version**: one or two sentences the developer would say to an interviewer.

**Mode:** Claude implements, explains each change, and the developer reviews every diff. Switch to *coach mode* (developer writes the core logic, Claude gives hints one level at a time: concept → shape → targeted snippet → full solution only on request) whenever the developer asks.

## Session protocol

- **Start:** read *Current status* below. Regenerate data / rebuild the lakehouse if the task needs it (commands below).
- **During:** work the *Fix plan* in order; one commit per concept; prove idempotency and reconcile counts before calling anything done.
- **End:** run tests + lint, update *Current status* (≤ 15 lines) and `docs/history.md`, log decisions, list uncommitted work. Say out loud if containers are left running.

## Hard rules (non-negotiable)

1. **Idempotency by design, not by full refresh.** Re-running any step must never duplicate or corrupt data. Mechanisms: bronze is append-only and partitioned by `ingest_date`/`pipeline_run_id` (a replay of the same run ID replaces its own partition only); silver/gold use Delta `MERGE` on business key + `record_hash`; watermarks advance only after a successful commit. Blanket `mode("overwrite")` is not an idempotency strategy.
2. **Fail loudly, never drop silently.** No `try: delta except: parquet` fallbacks. Explicit schemas for FHIR/CSV reads (no inference in silver). Bad records go to `quarantine_records`, never `/dev/null`. Every stage wraps its work so a crash writes a `FAILED` audit row. Reconcile every stage: `bronze rows = silver rows + quarantined rows (+ deduped)`.
3. **No fabricated values.** A metric that isn't computed from data (e.g. a hard-coded `turnaround_time_minutes = 15`) is a bug. If data to compute it doesn't exist, the column is NULL and documented.
4. **Honesty in claims.** "Epic-inspired / Epic-compatible FHIR", never "Epic experience". Synthetic data only (Synthea or the local generator) — never real PHI. README claims must match what the code does; update both together. Git history is real work only — no automated or backdated commits.
5. **PHI line:** SSNs are hashed before silver; names/addresses stay out of any shareable mart; masking is demonstrated, not assumed.
6. **No secrets in git.** Passwords and connection strings come from `.env` (gitignored) or environment variables; `docker-compose.yml` references variables, never literals.
7. **Surrogate keys are stable.** Never `monotonically_increasing_id()` for SKs that must survive reruns. Unknown member `-1` exists as a real row in every dimension.
8. **macOS/zsh environment.** Use the project `venv/` (`venv/bin/python`), and run modules from the repo root (`PYTHONPATH=. venv/bin/python -m ...`). Docker's CLI is not on PATH: use `/Applications/Docker.app/Contents/Resources/bin/docker`. SQL Server runs emulated (no arm64 image).

## Conventions

- **Python:** `ruff` + `black`; type hints on signatures; no bare `except`; pure transform functions separated from I/O so logic is unit-testable. Modules run as `python -m databricks.<layer>.<module>` from repo root.
- **SQL:** T-SQL for SQL Server (`sql/source`, `sql/quality`, `sql/warehouse`); Spark SQL inside PySpark. Every table has a documented grain.
- **Tests:** `tests/unit` (pure transforms, small DataFrames, assert literal expected values), `tests/integration` (pipeline reruns against a temp lakehouse path, never the real one), `tests/reconciliation` (exact equalities, not `<=`).
- **Every new table ships with:** a data-dictionary entry, a grain/uniqueness check, FK checks against its dimensions, and a reconciliation assertion.

## Reference values

| Thing | Value |
|---|---|
| Local lakehouse root | `delta_lakehouse/{bronze,silver,gold,metadata}` (gitignored) |
| Source data | `sample-data/{fhir_r4,sql_ehr,claims_csv}` (gitignored; regenerate with the generator) |
| Stack (venv) | Python 3.12 · PySpark 4.1.1 · delta-spark 4.3.1 · Java 17 |
| Current data volume | **~366K records**: EHR 20,000 patients / 39,970 encounters / 39,970 diagnoses / 100,027 lab results / 39,970 medications / 50 providers · FHIR 5,000 patients / 75,732 observations / 12,622 encounters / 12,622 conditions (22 bundle files) · 20,000 claims / 5 facilities |
| Control tables (DDL) | `pipeline_config`, `data_quality_rule`, `quarantine_records`, `pipeline_run_audit` — `sql/quality/01_data_quality_framework.sql` |
| Silver tables (12) | EHR: patients 20,000 · encounters/diagnoses/medications 39,970 · lab_results 100,027 · providers 50 · FHIR: patients 5,000 · observations 75,732 · encounters/conditions 12,622 · claims 20,000 · facilities 5 |
| Canonical checks | Second bronze run lands **0 rows** on all 12 watermark/CDC tables (only the 2 Full tables re-land). Bronze current state reconciles **exactly** to `SELECT COUNT(*)` in SQL Server per table. MedicationRequest/Practitioner are **0** — the generator doesn't emit them. |
| Timings (~366K records, local) | bronze first run **1:52** · second run **2:11** (slower: file watermark sources still read everything — see decisions) · test suite ~6 min |
| SQL Server source | `ehr_source` on localhost:1433, CDC on 6 tables, sa password in `.env` (gitignored). JDBC via `com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11`, control statements via `pymssql`. |
| Control / metadata tables | `metadata/{pipeline_config (14 rows), watermark_state, pipeline_run_audit, quarantine_records}` |
| Bronze layout | `bronze/<destination_table>`, partitioned `_ingest_date`/`_pipeline_run_id`; audit statuses `SUCCESS`/`FAILED`/`SKIPPED` |

## Common commands (repo root)

```bash
venv/bin/python sample-data/generate_clinical_data.py          # regenerate synthetic sources (~366K records)
/Applications/Docker.app/Contents/Resources/bin/docker compose up -d sqlserver   # SQL Server source
PYTHONPATH=. venv/bin/python -m scripts.setup_source_db         # schema + CDC + BULK INSERT (--reset to rebuild)
PYTHONPATH=. venv/bin/python -m scripts.simulate_source_changes # inserts/updates/deletes for CDC to capture
venv/bin/python -m databricks.bronze.ingest_raw_data            # bronze (all active pipeline_config rows)
venv/bin/python -m databricks.bronze.ingest_raw_data --run-id <id> [--source S --table T]  # restart a run: landed tables skipped, failed ones retried
venv/bin/python -m databricks.silver.process_fhir_silver        # silver (FHIR, incremental)
venv/bin/python -m databricks.silver.process_relational_silver  # silver (EHR + claims, incremental)
venv/bin/python -m databricks.gold.build_dimensions             # gold dims
venv/bin/python -m databricks.gold.build_facts                  # gold facts
venv/bin/python -m databricks.utilities.failure_simulation      # failure + recovery demo
venv/bin/python -m pytest tests/ -q                             # tests
venv/bin/ruff check . && venv/bin/black --check .               # lint
```

## Fix plan (gap analysis of 2026-09-21)

- [x] **1. Foundations** — remove commit-automation bot; real Delta (no Parquet fallback); secrets out of compose
- [x] **2. Real sources** — SQL Server + CDC in Docker, loaded from the generator (or Synthea); scale to 100K+
- [x] **3. Bronze** — append-only, partitioned, driven by `pipeline_config`, per-source watermarks
- [x] **4. Silver** — incremental `MERGE` on business key + hash, CDC deletes, all FHIR resources + EHR tables, explicit schemas
- [ ] **5. Data quality** — rules from `data_quality_rule`, missing rule types, thresholds that fail the run, idempotent quarantine
- [ ] **6. Gold** — stable SKs, unknown members, point-in-time SCD2 fact joins, missing dims/facts, no fabricated metrics
- [ ] **7. Failure demo** — a real failure, `FAILED` audit + alert, replay only the failed partition
- [ ] **8. Tests + CI** — behavior-asserting tests on a temp lakehouse; GitHub Actions running lint + tests

## Current status

> Keep this section SHORT (≤ 15 lines). Narrative goes to `docs/history.md`.

- **Phase:** steps 1 ✅, 2 ✅, 3 ✅, 4 ✅ (2026-09-22). **Next: step 5 (data-quality rules from the table)**, then 6 (gold), 7 (failure demo), 8 (CI).
- **Bronze restart semantics (don't regress):** a succeeded run ID is SKIPPED and never re-extracted, because its partition is the only copy of past source versions. A failed run ID is retried from the current watermark. Commit order: write → SUCCESS audit → watermark.
- **CDC semantics (don't regress):** LSN watermarks (20 hex chars, string-comparable); snapshot on first load; deletes land in bronze; a retention gap fails the run.
- **Silver semantics (don't regress):** stage watermark over bronze `_ingested_at` (landing time, so retries are picked up); collapse per source key; MERGE with `s._version > t._version` so replays never regress a record; **soft deletes** (`_is_deleted`/`_deleted_at`) from CDC op 1 and from `NOT MATCHED BY SOURCE` on Full snapshots; gold reads through `gold/silver_reader.py`.
- **Known issues:** gold still full-rebuilds with unstable SKs (`monotonically_increasing_id`) and joins only `is_current` rows; DQ rules hard-coded in `quality_engine.RULES_CATALOG`, no thresholds, quarantine appends duplicate on rerun; fabricated `turnaround_time_minutes`/`is_readmission_30d`; failure demo doesn't fail; `dim_patient` stacks EHR + FHIR patients as 25,000 distinct people (no `source_system` column, no identity resolution); generator emits no MedicationRequest/Practitioner; `advance_watermark` costs ~3.2 s/table (batching not taken); old SA password is burned (in git history, pushed).
