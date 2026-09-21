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
