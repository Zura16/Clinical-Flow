# Decisions log

Every non-obvious choice: what we decided, why, the alternative rejected, and what changes at real scale. Newest last.

## 2026-09-21 — Gap analysis: the pipeline was idempotent only by full overwrite

**Finding:** every layer wrote with `mode("overwrite")`, so rerunning produced the same counts. That passes a rerun test but is the weakest form of idempotency: every run rewrites all history, it can't carry deletes or updates incrementally, and bronze couldn't be immutable.
**Decision:** rebuild around append-only bronze + Delta `MERGE` in silver/gold, with watermarks in `pipeline_config`. Tracked as the fix plan in `CLAUDE.md`.
**Interview version:** "My first version was 'idempotent' only because it rewrote everything each run. I replaced that with append-only raw data and key-based MERGE, so a rerun converges to the same state without reprocessing history."

## 2026-09-21 — Delta Lake for real; removed the silent Parquet fallback

**Finding:** `get_spark_session` never registered the Delta extension, so every `format("delta")` write raised and `save_df` quietly wrote Parquet instead. The whole lakehouse was plain Parquet: no `_delta_log`, no ACID commits, no `MERGE`, no time travel, while the README said Delta.
**Decision:** enable Delta via `configure_spark_with_delta_pip` (jar version pinned to the pip package: delta-spark 4.3.1 on PySpark 4.1.1) and delete every `try: delta / except: parquet` fallback (config, quality engine, audit logger). A failed write now fails the run.
**Rejected:** keeping the fallback "for offline environments". A fallback that changes the storage format is a silent correctness change; if Delta isn't available, the run should stop.
**Cost:** full test suite went from 34 s to 144 s locally (each write is now a transaction-log commit). Acceptable, and the right thing to measure against once writes become incremental.
**Still open:** `save_df` keeps `overwriteSchema=true`, which accepts schema changes without complaint. It goes away when silver/gold move to `MERGE` (fix plan step 4).
**At scale:** on Databricks, Delta is the default and none of this setup exists; the lesson that carries over is "never catch a storage error and switch formats".
**Interview version:** "I found my 'Delta' tables were Parquet because a try/except quietly fell back when the Delta extension wasn't loaded. I removed the fallback so failures surface, and verified every table has a `_delta_log`."

## 2026-09-21 — Bronze: append-only, partitioned by run, driven by pipeline_config

**Decision:** each `pipeline_config` row is one bronze Delta table under `bronze/<destination_table>`, partitioned by `_ingest_date` / `_pipeline_run_id`, with metadata columns `_pipeline_run_id`, `_source_name`, `_source_file`, `_ingested_at`. Watermark loads land rows strictly past the stored watermark. Full loads (claims, facilities) land a complete snapshot per run. CSV columns stay STRING. FHIR resources are stored as their original JSON text, read with a bundle schema that declares `resource` as STRING, so nothing is inferred.
**Why run-level partitions:** a run's output is addressable and replaceable as a unit (`replaceWhere _pipeline_run_id = X`), and "what did run X land?" is a partition filter.
**Silver's read side:** bronze now holds every version, so silver reads *current state* (`databricks/silver/bronze_reader.py`). Watermark tables take the latest version per primary key. Full tables take the newest snapshot only. Taking the latest per key across all snapshots would keep claims that were deleted at the source alive forever.
**Watermark state lives in its own table** (`watermark_state`), not in `pipeline_config`, so an operator editing config and a run advancing progress never overwrite each other.
**Rejected:** `pipeline_config` rows in YAML. The spec wants a control *table* that ADF can Lookup. The seed duplicates the SQL INSERT until SQL Server hosts it (step 2).
**Known limits:**
- `>` on a timestamp watermark misses a row committed later with the same timestamp as the stored watermark. At scale, use CDC log sequence numbers, or re-read a small overlap window and dedupe on key + hash.
- Rows with a missing or unparseable watermark fail the table loudly instead of being skipped forever. Routing them to quarantine is step 5.
- `wholetext` reads a whole bundle file into one row, which is fine for per-patient Synthea bundles but not for multi-GB bundles.
**Interview version:** "Bronze is append-only and partitioned by run ID, driven by a control table. Silver never trusts that a table holds one row per key; it derives current state, per key for incremental sources and newest snapshot for full extracts, so deletes in full extracts are honored."

## 2026-09-21 — Finding: "replay a run" from a mutable source destroyed bronze history

**Finding:** the first design replayed a run ID by re-reading its recorded watermark window from the source and `replaceWhere`-ing the run's partition. The integration test changed a patient's address between runs and then replayed run 1. The source no longer had the old version, so the replay found 1 row instead of 2 and **overwrote run 1's partition, deleting the only copy of the old address.** The "idempotent replay" was a data-loss path.
**Decision:** a run ID that already succeeded for a table is skipped (`SKIPPED` audit row, partition untouched). A run ID that failed is retried as a normal run from the current watermark, which failed runs never advance. Commit order: bronze write → `SUCCESS` audit → watermark advance. The skip path re-applies the watermark, so a crash between the last two steps self-heals. `replaceWhere` stays, so a crash between the bronze commit and the audit row is retried without duplication. Downstream replay reads bronze, never the source.
**Principle:** once raw data has landed, bronze is the system of record for what the source said at that time. Re-extraction is a different operation (backfill) with different semantics.
**Evidence:** `tests/integration/test_bronze_incremental.py` step 3 asserts the old address survives a rerun.
**Interview version:** "My first replay design re-extracted the run's watermark window. A test showed that when the source had changed, the replay overwrote bronze with a smaller result and lost the old versions. I changed the rule: a successful run is never re-extracted, only failed runs are retried, and all reprocessing downstream reads from bronze."

## 2026-09-21 — Spark session timezone pinned to UTC

**Finding:** casting FHIR `meta.lastUpdated` `…19:35:29Z` gave `12:35:29`, the laptop's local time, so watermarks would shift with whoever ran the job. **Decision:** `spark.sql.session.timeZone=UTC`. Zone-less EHR timestamps are treated as UTC. That's an assumption, and it's documented here. **Interview version:** "Timestamps are compared in UTC at the session level, so a watermark means the same thing on every machine."
