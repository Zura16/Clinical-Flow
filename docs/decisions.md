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

## 2026-09-22 — SQL Server CDC as the EHR source, with LSN watermarks

**Decision:** the six EHR tables move from timestamp watermarks over CSV to SQL Server CDC over JDBC. The watermark becomes the LSN (log sequence number), SQL Server's position in its transaction log, stored as 20 hex characters so it stays fixed-width and sorts correctly as a string. First load snapshots the table as of the current max LSN and marks the rows operation 0; later runs read `cdc.fn_cdc_get_all_changes_*` between LSNs and land operations 2 (insert), 4 (update after-image) and 1 (delete).
**Why this beats a timestamp watermark:** it closes the gap where a row committed later carrying an earlier `updated_at` is never seen, and it is the only way to observe hard deletes. A deleted row simply stops existing, so no query over the table can find it.
**Deletes:** bronze keeps the delete row as the evidence the deletion happened; silver's current-state read drops keys whose last change was a delete. Bronze is the audit trail, silver is the current picture.
**Access split:** pymssql for control statements and LSN functions, Spark JDBC for bulk reads, which is what a Databricks job would do.
**Retention is a real failure mode:** CDC keeps changes for 3 days by default. If the stored LSN is older than the capture's minimum, changes were cleaned up before ingestion and the run fails loudly, asking for a re-snapshot. A quiet "resume from the new minimum" would silently skip data. This guard can fire falsely if cleanup advances the minimum during a long idle period; the fix is the same (re-snapshot), and the alternative (silence) is worse.
**Snapshot race, accepted:** rows written between reading max LSN and reading the table appear in both the snapshot and the next increment. The later copy has the higher LSN and wins in current-state resolution, so the result converges. Avoiding it entirely needs a snapshot-isolation read.
**Two watermark formats:** a timestamp and an LSN are not comparable, so `watermark_state` records which kind it holds and ignores a stored position when a table's ingestion type changes. Found while switching these tables over; without it, an old timestamp would compare as greater than every LSN and the table would never advance.
**Platform note:** SQL Server has no arm64 image, so on Apple Silicon it runs emulated. Works, but slower than native, and it's the reason the compose file pins `platform: linux/amd64`.
**Interview version:** "The EHR source is SQL Server with CDC. I track position by LSN rather than a modified timestamp, which is what lets me capture hard deletes and avoids missing rows committed out of timestamp order. If CDC retention has passed my stored LSN, the run fails and asks for a re-snapshot instead of silently skipping changes."

## 2026-09-22 — Finding: incremental doesn't mean cheap for file sources

**Measured at ~366k records:** first bronze run 1:52; second run, which lands 0 rows for all 12 watermark and CDC tables, took **2:11 — longer than the first.**

**Corrected attribution (2026-09-22, measured per source group):** the first explanation here blamed re-reading the FHIR files. Measurement says that is only part of it:

| Group | Tables | Second-run time | Per table |
|---|---|---|---|
| fhir_r4 | 6 | 62 s | ~10 s |
| sql_ehr (CDC) | 6 | 53 s | ~9 s |
| claims_csv (Full) | 2 | 26 s | ~13 s |
| Spark session startup | — | 6 s | — |

Reading and parsing *all* 22 FHIR bundles takes **3.2 s total**, so the six FHIR tables spend ~19 s of their 62 s on re-reads and the rest on fixed per-table cost. The CDC tables read nothing at all and still cost ~9 s each.

**Why:** the dominant cost at this scale is per-table overhead, not data: each table runs a `count()`, a Delta write (even for zero rows), an audit-table commit and a watermark MERGE. Fourteen tables x ~4 small Delta commits is roughly two minutes regardless of volume. Delta commits are cheap per byte and expensive per call.
**Secondary:** a watermark over files filters *after* reading, so it can only be made cheap by pruning before the read.
**What would fix each part:** for the re-reads, filter on `_metadata.file_modification_time` before parsing (measured: 3.2 s -> 0.1 s, and the file listing does prune). For the fixed cost, skip the write entirely when an increment is empty, and batch the audit rows into one commit per run — though writing the audit row per table immediately is what makes a crashed run diagnosable, so that one is a real trade, not a free win.
**Lesson:** attribute cost by measurement before writing the explanation down. The first version of this entry sounded right and was wrong.
**Interview version:** "Incremental ingestion only saves work if the source can answer 'what changed' without a full scan. My CDC tables cost nothing when idle; my file-based FHIR source still reads every file to find nothing, which I measured rather than assumed."

## 2026-09-22 — The predicted cheap win wasn't the win; the profiler found the real one

**Predicted:** skipping the Delta write for an empty increment would cut most of the ~9s-per-table idle cost.
**Measured:** it changed nothing (fhir_r4 group 62s -> 72s, inside noise). Phase timings for one idle CDC table told the real story:

| Phase | Time |
|---|---|
| `get_source_config` | **9.75 s** |
| `advance_watermark` (Delta MERGE) | 3.23 s |
| `run_partition_exists` (Spark query) | 1.74 s |
| `get_watermark` | 1.30 s |
| audit `log_run` (Delta append) | 0.71 s |
| CDC read over JDBC | 0.56 s |
| `count()` | 0.13 s |

**Cause:** `get_source_config` re-ran the seed MERGE against `pipeline_config` on *every* lookup — a full Spark job to read one row. **Fix:** reconcile the seed once per process, and answer "did this run already write a partition?" by listing directories instead of querying Delta.
**Result:** fhir_r4 62s -> 52s, sql_ehr 53s -> 42s, claims_csv 26s -> 22s, with no change to behavior.
**Kept anyway:** the empty-write skip stays, because not committing nothing is still right; it just isn't where the time went.
**Still on the table:** `advance_watermark` is 3.2s per table because each is its own MERGE. Batching them into one MERGE per run would save ~40s, at the cost of a crash window where bronze is committed but the watermark hasn't moved (rows would be re-landed and deduplicated downstream). Not taken yet.
**Interview version:** "I guessed the expensive part and was wrong. Profiling showed a config lookup was re-running a MERGE on every call, which cost more than all the I/O combined. Caching it cut idle runtime by about 20% with no behavior change."

## 2026-09-22 — Silver: incremental MERGE, version guard, soft deletes

**Decision:** silver consumes bronze incrementally. Each silver table keeps a **stage watermark** over bronze's `_ingested_at` in `watermark_state` (stage `silver`), reads only partitions landed since, collapses the batch to one row per *source* key, and merges into the silver table.
**Why bronze's landing time, not the source's timestamp:** a retried run lands old source rows with a new `_ingested_at`, so a landing-time cursor picks them up. A source-time cursor would step over them forever.
**The version guard is the important line.** `whenMatchedUpdateAll(condition="s._version > t._version")` means a replayed or out-of-order batch can never overwrite newer data with older. `_version` is one fixed-width comparable string per ingestion type: LSN+seqval for CDC, the source timestamp for Watermark, landing time for Full.
**Deletes are soft** (`_is_deleted`, `_deleted_at`), decided explicitly over hard deletes: facts referencing the record keep resolving, and the deletion stays auditable. "The row is gone and we cannot tell you what it said" is not acceptable for clinical data. Two paths produce it: a CDC delete (operation 1), and, for Full snapshots, `WHEN NOT MATCHED BY SOURCE` — a key absent from a complete snapshot was removed at the source. Gold filters `_is_deleted` through one reader so no gold query has to remember.
**Specs, not blocks:** each silver table is a `SilverSpec` (source, key, column expressions, hash columns, rules) and one engine runs them all. Coverage went from 5 tables to 12 by adding specs, not pipelines. Casts live in silver because bronze keeps every source column as text.
**Measured:** a first load of `silver_ehr_patients` reads 20,000 rows; after 5 patient changes the next run reads **7**. A run with no upstream changes reads 0 for all 12 tables. Active silver rows reconcile exactly to `SELECT COUNT(*)` on every EHR table.
**Bug found on the way:** `dim_patient` added placeholder columns with an untyped `F.lit(None)`. VOID is not storable, so the column silently vanished from the written table and the *second* run failed to resolve it. It only ever appeared on a rerun against an existing table.
**Not done here:** gold still rebuilds in full; that is step 6 along with stable keys and point-in-time joins.
**Interview version:** "Silver merges on the business key with a version guard, so replays and out-of-order batches can't regress a record. Deletes are soft, because a hard delete strands the facts that point at the record and destroys the audit trail. After five source changes my silver run reads seven rows instead of twenty thousand."

## 2026-09-22 — Data quality: rules in a table, thresholds that fail the run

**Decision:** `data_quality_rule` holds the checks (seeded from `quality_rules.py`, mirroring the SQL DDL). Adding a check is a row; retiring one is `active_flag = false`. Rule types: NOT_NULL, RANGE, REGEX (row-scoped expressions), UNIQUE (grain), REFERENTIAL (`target_table.target_column`), FRESHNESS (max age in hours).
**Two independent dials, which is the part worth explaining:**
- `severity` decides the **row's** fate: CRITICAL/ERROR quarantine it and keep it out of silver; WARNING records the violation and lets the row through.
- `failure_threshold` decides the **run's** fate: if the failure rate for a rule exceeds it, the run raises and writes a FAILED audit row before anything merges. Identity and grain rules sit at 0; clinical plausibility gets a small allowance; coding and reference checks warn instead of blocking.
**Quarantine is idempotent:** `quarantine_key = sha256(run_id, dataset, record_id, rule)` merged on that key, so rerunning a batch re-derives the same keys and inserts nothing. The old engine appended, so every rerun duplicated its quarantine rows.
**Passing rules are recorded too**, in `data_quality_result`. If only failures were written, "no rows today" would be indistinguishable from "the check never ran". That table is what the data-quality dashboard reads.
**Rules skip soft-deleted rows.** Found by running against real data: the two REFERENTIAL violations were the deleted patient's own encounter deletions, whose parent was already (correctly) gone. A record on its way out does not have to satisfy constraints.
**Interview version:** "Rules live in a table with two dials: severity decides whether the row is quarantined, threshold decides whether the run fails. Passing checks are recorded as well as failures, because 'no violations' should be a measurement, not silence."

## 2026-09-22 — Finding: a daylight-saving bug the quality rules caught

**Symptom:** the rule `result_timestamp >= order_timestamp` failed for exactly 1 row in 100,027, while `SELECT COUNT(*) ... WHERE result_timestamp < order_timestamp` in SQL Server returned **0**.
**Cause:** SQL Server's `DATETIME2` carries no zone. The JDBC driver materialises it into a `java.sql.Timestamp` using the **JVM's** default zone — `spark.sql.session.timeZone=UTC` governs Spark, not the driver. 2024-03-10 is US spring-forward: the order at 02:50 was read as PST (UTC-8) and the result at 03:48 as PDT (UTC-7), so a 58-minute gap became **minus two minutes**. Every EHR timestamp was shifted by the local offset; only the row spanning the DST boundary became visibly impossible.
**Fix:** CDC and snapshot reads `CONVERT(VARCHAR(33), col, 126)` date/time columns to ISO-8601 **text** in SQL. Bronze then stores the source's own characters — consistent with how CSV columns already land — and silver casts under a UTC session. `-Duser.timezone=UTC` is set as well, but the text conversion is the actual fix: it removes the driver's zone from the path entirely.
**Verified:** silver now reports `2024-03-10 02:50:22` / `03:48:22`, identical to the source, and the violation count is 0.
**Why this one matters:** the pipeline had been "working" for two steps with every EHR timestamp silently shifted by 7-8 hours. Row counts reconciled perfectly the whole time, because counts cannot see it. A quality rule comparing two columns could.
**Interview version:** "A data quality rule flagged one lab result as resulted before it was ordered, but the source had none. JDBC was reading zone-less timestamps in the JVM's local zone, and across the daylight-saving boundary two columns shifted by different amounts. I moved the conversion into SQL so bronze stores the source's exact characters. Row-count reconciliation had been green throughout — it can't catch a uniform shift."
