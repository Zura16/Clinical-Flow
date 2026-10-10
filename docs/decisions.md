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

## 2026-09-26 — Gold: hash surrogate keys, SCD2 that holds, measures that are measured

**Surrogate keys are `xxhash64` of the natural key.** Delta identity columns are unsupported in this build (probed), and a counter — `monotonically_increasing_id`, a sequence table — renumbers on every rebuild and needs coordination to stay unique. A hash is stable, parallel-safe and needs no state. The cost is collisions: 64 bits gives roughly 1 in 10^8 at a million rows. That is the price of not coordinating, and a grain test would catch one.
**Unknown member (-1) in every dimension.** A fact whose dimension is missing or late joins to a real row. The alternatives are a NULL key (every downstream query needs a COALESCE) or dropping the fact (silent data loss).
**SCD2 on dim_patient only.** Nothing asks what a facility used to be called, so the rest are Type 1. A change closes the current version *at the change time* and opens the next one there, so windows are contiguous: `start <= event < end` can never fall in a gap or match twice.
**Facts join point-in-time**, not `is_current`. Joining on `is_current` silently rewrites history: every patient who moves house changes what last year's encounters say about them.
**Measures are computed.** `turnaround_minutes` comes from order-to-result (232 distinct values; it used to be the literal 15), `is_readmission_30d` from each patient's own previous discharge, length of stay from the timestamps. FHIR Observation has no order time, so its turnaround is NULL: the source cannot answer the question, and a zero would be a lie.
**dim_patient carries source_system.** EHR and FHIR share no patient identifier, so its 25,000 rows are 20,000 EHR members plus 5,000 FHIR members, not 25,000 people. Identity resolution is the real fix and is out of scope; labelling the source is the honest minimum.
**Verified:** 0 orphans, 0 NULL keys and 0 unknown members across every fact-to-dimension join at ~316k fact rows; a rebuild produces identical keys; the 2023-24 encounters of a patient who moved still resolve to their pre-move version.
**Interview version:** "Keys are hashes of the natural key, so rebuilding a dimension doesn't invalidate the facts pointing at it. dim_patient is SCD2 with contiguous windows and facts join point-in-time, so a patient moving house doesn't rewrite last year's encounters. Every measure is computed from the data, and where the source can't answer — a FHIR observation has no order time — the column is NULL rather than a filler."

## 2026-09-26 — Three bugs step 6 surfaced, all found by measurement

1. **All 75,758 FHIR observations resolved to the unknown patient.** A dimension version opened at `source_updated_at`, which for an unchanged record is when we last *saw* it, not when it began. FHIR patients were last updated in 2026; their observations are from 2025, so no version covered the event. **Fix:** a key's first version opens at `1900-01-01`; later versions open when the change happened. Unknown members went to 0.
2. **SCD2 silently inserted nothing.** `changed` was derived from dim_patient, and the expire step rewrites dim_patient. When Spark recomputed `changed` for the insert, those keys no longer had a current version, so they were treated as brand new, restarted at 1900, re-derived the key of the row just expired, matched, and inserted nothing — leaving patients with an expired version and no current one. **Fix:** `localCheckpoint(eager=True)` severs the lineage before the first write, and the insert count is now compared against the change count and raises if they differ. A lazy DataFrame that reads the table you are about to modify is a trap, and `.cache()` is not a guarantee.
3. **A test clobbered a shared table.** The SCD2 test wrote its two fake patients over `silver_ehr_patients`; the later whole-pipeline test then found 100% of encounters referencing unknown patients and — correctly — failed on the referential threshold. The quality gate did its job on the test suite's own mess. **Fix:** `build_dim_patient` takes its source as an argument, so the test injects data instead of writing to shared state.
**Also worth knowing:** PySpark converts timestamps to the *driver's* local zone on `collect()`. A stored `1900-01-01` UTC arrives in Python as `1899-12-31 16:00`, which failed an assertion that looked obviously right. Assert on `date_format(...)` computed in Spark instead.

## 2026-09-26 — Failure and recovery made real, not narrated

**The old demo failed at nothing.** It built 500 bad rows in memory, never put them through the pipeline, called a no-op "applying the correction", reran everything and printed success down both branches of its check. **Now:** the rows are corrupted in SQL Server, CDC carries them, a real threshold fails a real stage, and recovery uses the same commands an operator would.
**Why the failure is survivable, and this is the part worth explaining:** bronze lands the corrupt rows (raw data always lands — that is what bronze is for), the offending rows go to quarantine with their payloads, silver is untouched, and **the stage watermark does not move**. That last point is the recovery mechanism: because the watermark still points before the bad batch, rerunning the stage reprocesses exactly that batch. There is no "replay the failed partition" command to get wrong; correctness comes from the watermark not advancing.
**Alerting** (`pipeline_alert`) answers three questions without a query: what broke, what it means, and what to do next. Delivery is print + table + optional webhook, and every send is wrapped so a dead sink can never replace the exception it was reporting. The alert key is (run, pipeline, summary), so a retried run does not re-page for the same failure.
**Demonstrated, with the verification in the script:** after 500 corrupted rows and a recovery, live silver rows equal the source count exactly, the repaired rows are present once, no corrupt value reached silver, bronze holds 4,000 more row-versions than it started with (every version, including the corrupt ones), and quarantine kept the evidence.
**The demo repairs the source even when it aborts** (`try/finally`), and repairs anything an earlier aborted run left behind before it starts. Found the hard way: the first attempt died before its repair step, so the *next* run failed at baseline instead of at the injection.
**Interview version:** "The demo corrupts 500 rows in SQL Server and lets the pipeline fail on a real quality threshold. Bronze keeps the bad rows, quarantine keeps the rejected ones with their payloads, and silver is untouched because the stage watermark never advanced — so rerunning after the fix reprocesses exactly that batch and lands each record once. I verify that by reconciling live silver against the source afterwards."

## 2026-09-28 — CI, lint and Delta maintenance (step 8)

**Two workflows, split by what they need.** `ci.yml` runs on every push: ruff, black and the full suite against a small generated dataset (200 EHR / 50 FHIR patients / 200 claims) in a temp lakehouse. The two SQL Server tests skip themselves and `-rs` prints why, so a skip can't pass as a pass. `nightly-integration.yml` starts a SQL Server service container with the Agent enabled (CDC capture is an Agent job) and is the only place the CDC and whole-pipeline tests actually execute. Running SQL Server on every push would cost minutes per push to re-prove a path that rarely changes.
**The loader falls back to client-side inserts** when the server cannot see `/data`: a service container cannot mount the workspace, so `BULK INSERT` has nothing to read. It is detected, not configured: `sys.dm_os_enumerate_filesystem` raises when the directory is missing. Slower (batched `executemany` of 1,000 rows) but it needs no shared filesystem.
**Simplified, named:** the nightly workflow falls back to a literal SA password when no `MSSQL_SA_PASSWORD` secret is set, so a fork can run it. That password only protects a throwaway container on the runner's loopback, but it is still a literal in git. Set the repository secret and the fallback is never used.
**Not yet proven:** neither workflow has run on GitHub, and the client-side insert path has never executed (the local container mounts `/data`, so it always takes `BULK INSERT`). The CI job was simulated locally in a clean copy with no `.env` and no SQL Server: **29 passed, 2 skipped, 2:25**.
**Lint is a small rule set on purpose** (`pyproject.toml`): pyflakes, bugbear, import order, simplifications, unused arguments. The first pass removed dead imports and three unused parameters (`run_partition_exists(spark)`, `build_dim_date(run_id)`, and `read_csv_source`'s `cfg`, which the shared reader signature requires and so became `_cfg`). No behaviour changed; the other diffs are black reformatting, checked by comparing each file's AST against HEAD reformatted.

**Finding: compaction cannot fix bronze, by design.** `maintain_lakehouse` (OPTIMIZE, VACUUM only on `--vacuum`) took the active file count from **481 to 75** with **all 45 tables' row counts identical** before and after (1:28). But every bronze table kept its file count (`bronze_ehr_lab_results` 17 → 17). OPTIMIZE bin-packs *within* a partition, and bronze is partitioned by `_pipeline_run_id`, so each run's files are a floor. That is the price of replay-by-run-partition. At scale the answer is a coarser partition (date only) with run ID as a column, and the replay then becomes `replaceWhere _pipeline_run_id = X`.
**The real small-file offender was metadata:** `pipeline_run_audit` went from 301 active files to 1 (603 on disk), `data_quality_result` 53 → 1, `watermark_state` 22 → 1 (208 on disk). Every stage appends one row to one of these, so they grow fastest and are read on every run.
**Interview version:** "CI splits by dependency: lint and the full suite run on every push against a generated dataset, and the SQL Server CDC path runs nightly in a service container. When I added compaction, the file count dropped from 481 to 75 with identical row counts, but bronze didn't compact at all. OPTIMIZE can't merge across partitions, and I partition bronze by run ID for replay, so that was a tradeoff I'd made without noticing its cost. The biggest offender turned out to be the audit table, which grows one small file per stage."

## 2026-09-29 — What the first nightly runs found

**The demo assumed a built lakehouse and never checked.** CI's tests use a temporary lakehouse, so
the real one was empty when the demo ran. `table_count` reads a missing table as 0, so the demo
corrupted the source, recovered correctly, and only crashed reading `gold/fact_observation` at the
final check. **Fix:** `require_built_lakehouse()` runs before the source is touched, and the nightly
workflow builds the lakehouse (bronze, both silver jobs, dims, facts) in the operator's order first.
That step is also the only end-to-end run against a real SQL Server outside the tests.
**The insert fallback worked on its first run** (it could never run locally): all six tables loaded
with batched client-side inserts, and **31 tests passed with 0 skipped** (12:32). This was the first time the CDC and
whole-pipeline tests ran anywhere but one laptop.
**A weak SA password stops SQL Server before any step runs.** The container logs `Password
validation failed` and shuts down; the job page only shows every step skipped. Now scenario 6 in
the troubleshooting guide.
**The jar cache never saved.** Spark 4 keeps its Ivy cache in `~/.ivy2.5.2`, not `~/.ivy2`, so
both workflows cached a directory that never existed and downloaded the Delta and JDBC jars on every run.
**Verified:** nightly run 36604789450 green end to end: all nine demo checks pass, and
`fact_observation` 4,039 = live labs 2,551 + live FHIR 1,488.
**Interview version:** "The first nightly run failed in the demo, not the pipeline: the demo read a
missing gold table as zero rows and only crashed at its last check. I made it verify its
precondition before touching the source, and had CI build the lakehouse the way an operator would.
That also gave me my first end-to-end run against a real SQL Server in CI."

## 2026-10-03 — FileIncremental: a file log replaces the timestamp filter for FHIR

**Finding (a silent-loss bug, not just a cost):** FHIR bronze filtered rows by `meta_lastUpdated > watermark`. A new bundle whose resources carry *older* timestamps than the watermark (a delayed export, a backfill, a re-sent batch) landed **0 of its rows with a SUCCESS audit row**: nothing failed and nothing arrived. Shown by running the new lifecycle test with the type set to `Watermark`: `assert 0 == 2`. It is the late-arrival gap listed under bronze's known limits, and it would have hit the next real data drop, because the generator dates observations before the stored watermark (`2026-01-01 01:35`).
**Decision:** a new `ingestion_type`, `FileIncremental`, for the six FHIR tables. A run lists the files, reads only those not yet ingested by that table, and lands **every row** of them; no timestamp filter. `metadata/file_ingest_log` records (path, size, mtime, run) per table. This is the pattern behind Databricks Auto Loader's checkpoint.
**The audit row is the commit marker.** A logged file counts only if its run has a SUCCESS audit row for the table. The order is bronze write → log entries → SUCCESS audit. A crash anywhere before the audit row leaves the files pending, so a retry re-reads them and `replaceWhere` replaces the partial partition. The rejected order, log first, would let a retry find nothing new and overwrite the partition with an empty one.
**Files with no rows for a table are logged too**, or Practitioner and MedicationRequest (which the generator never emits) would re-read all 22 files forever.
**Why `Watermark` was not changed instead:** it has a valid contract the existing test pins: a CSV rewritten in place lands only its changed rows. A file log would re-land the whole file. Two behaviours, two names, and `pipeline_config` says which each table uses.
**Silver had to change too.** `version_expr` versioned anything that was not `Watermark` by `_ingested_at`. A late file holding an older version lands *later*, so it would have overwritten the newer record in silver. `SourceConfig.versioned_by_watermark_column` now answers that once for both layers. Proven by mutation: with silver reverted, the new merge test fails with the stale address (`'1 Old St' == '9 New Ave'`). `bronze_reader.current_state` now raises on an unknown type instead of falling through.
**Measured (22 bundles, ~106k resources):** a second FHIR run went **51 s → 31 s**, reading **0 of 22 files**. The ~20 s saved matches the ~19 s of re-reads measured on 2026-09-22; the remaining ~31 s is per-table fixed cost (audit commit, Spark startup), which this does not touch.
**Migration cost, accepted:** the first FileIncremental run starts with an empty log, reads all 22 files, and re-lands every FHIR resource once (bronze 2× for those tables). Silver read all 105,976 rows and **inserted 0, updated 0**: the version guard absorbed them, and silver counts are identical. The rejected alternative, pre-filling the log with every file currently on disk, would mark as ingested any file that arrived after the last real run, so its rows would never land.
**Limits, named:** a rewrite that keeps both size and mtime is not detected (catching it means hashing every file, every run). Listing is still O(files): fine at 22, the bottleneck at millions, where the answer is event-driven discovery (Auto Loader's notification mode, Event Grid on ADLS). A file read while it is still being written can be read partially; at scale, writers publish by atomic rename or a `_SUCCESS` marker. A rewritten file re-lands its unchanged rows (storage, not correctness). The log is one more append-per-run metadata table for `maintain_lakehouse` to compact.
**Interview version:** "My FHIR bronze filtered on the record's lastUpdated, so a late export with older timestamps landed nothing and still reported success. I replaced it with a processed-files log, the Auto Loader pattern: new files land in full, and silver orders versions by the record's own timestamp, so an old version can't overwrite a newer one. The audit row is the commit marker, so a crash can never mark a file read when its rows didn't land. Idle runs also got 40% faster, but the correctness fix was the point."

## 2026-10-06 — Full loads land only when the extract changed

**Decision:** a Full table lists its file and compares (path, size, mtime) with the files of the table's **latest** successful run in `file_ingest_log`. If they match, the run lands nothing, writes a SUCCESS audit row with 0 rows, and prints which run it matches. Otherwise it reads and lands the whole snapshot, as before.
**Latest, not any.** FileIncremental skips a file that matches any committed entry. Full cannot: a snapshot source has one current state. A file restored from a backup with `cp -p` keeps an old size and mtime and matches an old entry. "Any" would skip it, so bronze's newest snapshot would stay ahead of the source and silver would be wrong with no error. The test restores the original file with its original mtime and asserts that it lands.
**A missing extract still fails the run.** Listing raises on a missing path, so a vanished file is neither "unchanged" nor an empty snapshot that soft-deletes every claim downstream.
**Status stays SUCCESS.** SKIPPED already means "this run ID already landed this table"; reusing it would give it two meanings, and a new UNCHANGED status would be one more value every consumer must know.
**Safe because:** silver reads Full tables through `latest_snapshot` (newest landed run) and soft-deletes keys missing from it. A skipped run adds no partition, so the newest snapshot is still the current content. Nothing counts snapshots per run.
**Measured:** an idle claims run went 24.6 s to 19.0 s and lands 0 rows instead of 20,005. Silver claims stay at 20,000 live.
**Finding: silver ignores `record_hash`.** Silver computes `record_hash` for every row, but the MERGE condition is only `s._version > t._version`. For Full tables the version is `_ingested_at`, so every new snapshot "wins" and rewrites every row: `read 20000, inserted 0, updated 20000` for a snapshot whose content had not changed. Before this change that happened on every run. Now it happens only when the file changes, but a one-claim change still rewrites all 20,000. CLAUDE.md's rule 1 says silver MERGEs "on business key + record_hash", so the code does not match the rule. Open as its own decision because it changes silver semantics for every ingestion type.
**An empty extract fails the run.** A header-only extract used to land 0 rows and write no partition, so `latest_snapshot` kept returning the previous snapshot and an empty source never deleted anything. Treating it as a real empty snapshot would soft-delete every claim instead. Both outcomes are silent, and a 0-row claims extract is far more likely a broken export than a real empty table, so the run now fails before anything is written or logged. A second run fails too, so the file cannot later pass as "unchanged". If a source can genuinely be empty, an `allow_empty_snapshot` flag in `pipeline_config` is the extension point.
**Interview version:** "Full extracts now land only when the file differs from the last one I landed. I compare against the latest successful run, not any run, because a file restored from a backup carries an old timestamp and must still count as a change. Measuring it showed silver was rewriting all 20,000 claims on every snapshot because the MERGE never checked the record hash."
