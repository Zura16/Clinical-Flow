# ClinicalFlow Troubleshooting & Recovery Guide

Every stage writes a row per table to `pipeline_run_audit` (`SUCCESS`, `FAILED` or `SKIPPED`) and
raises an alert into `pipeline_alert` on failure. Start there: the alert's `next_step` names the
recovery for that failure.

To see the whole failure-and-recovery path end to end, run the demonstration:

```bash
python -m databricks.utilities.failure_simulation --bad-rows 500
```

It corrupts 500 lab results in SQL Server, lets CDC carry them, fails a real quality gate, shows
the evidence, repairs the source, replays the stage and verifies that nothing was duplicated or
lost. It repairs the source even if it aborts part way.

---

## Scenario 1: a quality threshold failed the run

**Symptoms:** a `FAILED` audit row with `error_code = DataQualityThresholdError`, an alert in
`pipeline_alert`, and new rows in `quarantine_records`.

**What has and has not happened:** the rows are in bronze (raw data always lands), the offending
rows are in quarantine, **silver is unchanged**, and the stage watermark has **not** moved. That
last point is the whole recovery mechanism: the same bronze batch is still waiting to be processed.

1. Find which rule and how badly:
   ```sql
   SELECT rule_name, rows_checked, rows_failed, failure_rate_pct, failure_threshold_pct
   FROM data_quality_result WHERE pipeline_run_id = '<run_id>' AND passed = 0;
   ```
2. Look at the actual records, payload included:
   ```sql
   SELECT record_identifier, failed_rule, error_message, raw_payload
   FROM quarantine_records WHERE pipeline_run_id = '<run_id>';
   ```
3. Decide which is wrong, the data or the rule:
   - **The data.** Fix it at source. The correction arrives as a normal CDC update.
   - **The rule.** Adjust `data_quality_rule` (`rule_expression`, `failure_threshold`, or
     `active_flag = 0` to retire it). Record why in `docs/decisions.md`: loosening a rule because
     it is inconvenient is how a pipeline stops being trustworthy.
4. Rerun the stage. It reprocesses the same bronze batch, because the watermark never moved:
   ```bash
   python -m databricks.silver.process_relational_silver
   ```
5. Confirm recovery: the stage logs `SUCCESS`, and live silver rows reconcile to the source count.
   Rows are not duplicated, because the merge is keyed on the business key.

## Scenario 2: a bronze table failed

**Symptoms:** `FAILED` audit row for `bronze:<table>`, and the run raised `RuntimeError` naming the
tables that failed. Other tables in the run are unaffected: one table failing does not stop the rest.

1. Read `error_code` / `error_message` on the audit row. Common causes:
   - `OperationalError` — SQL Server unreachable. Start it: `docker compose up -d sqlserver`.
   - `ValueError: ... unparseable <column>` — a source row has a missing or unreadable watermark.
   - `ValueError: CDC retention gap` — changes were cleaned up before they were ingested. See below.
2. Fix the cause, then rerun **with the same run ID**:
   ```bash
   python -m databricks.bronze.ingest_raw_data --run-id <run_id>
   ```
   Tables the run already landed are skipped; the failed one resumes from the current watermark,
   which a failed attempt never advances. Nothing lands twice.

## Scenario 3: CDC retention gap

**Symptoms:** `ValueError: CDC retention gap on dbo.<table>: need changes from X, but the capture
only retains from Y`.

Changes were removed by CDC cleanup (3 days by default) before ingestion read them. The run fails
rather than resuming from the new minimum, because resuming would skip those changes silently.

**Recovery:** re-snapshot the table by clearing its watermark, then ingest. The next run reads the
table as it stands now and establishes a fresh baseline.
```sql
DELETE FROM watermark_state WHERE source_name = 'sql_ehr' AND source_table = '<table>';
```
Note what the gap means: the *changes* in the window are gone for good. The snapshot gives current
state, not the history in between.

## Scenario 4: schema drift

**Symptoms:** `AnalysisException: [UNRESOLVED_COLUMN...]` in silver, or a new source column that
never reaches silver.

Bronze cannot break on drift: it stores CSV columns as text and FHIR resources as raw JSON, so a new
or renamed field lands untouched. Silver is where drift surfaces, because its column expressions and
FHIR schemas are explicit.

1. Compare the source against the spec in `databricks/silver/specs.py`.
2. A **new** column is ignored until its spec asks for it. Add it to `columns` (and to the resource
   schema for FHIR), plus the data dictionary.
3. A **renamed or retyped** column needs the spec's expression updated. Do not reach for
   `mergeSchema`: an inferred column arriving silently is how a type changes without anyone
   noticing.

## Scenario 5: timestamps look wrong

Symptoms: times shifted by a whole number of hours, or a comparison between two timestamp columns
failing for a handful of rows.

SQL Server's date/time types carry no zone, and JDBC would materialise them in the JVM's zone, so
CDC reads convert them to ISO text in SQL (`databricks/utilities/sqlserver.py`). Keep it that way.
See the daylight-saving finding in `docs/decisions.md` for what this looked like when it was broken.

Note when debugging interactively: PySpark converts timestamps to the **driver's local zone** on
`collect()`, so a stored `1900-01-01` UTC prints as `1899-12-31 16:00`. Compare with
`date_format(col, 'yyyy-MM-dd HH:mm:ss')` computed in Spark instead.

## Scenario 6: the nightly workflow fails with every step skipped

Symptoms: the `cdc` job is red, `Initialize containers` failed, and every step after it shows as
skipped. None of the pipeline code ran.

The SQL Server service container never became healthy, and the reason is only in the
`Initialize containers` log. Search it for `ERROR:`. The one seen so far:

    ERROR: Unable to set system administrator password: Password validation failed.

SQL Server shuts itself down when `MSSQL_SA_PASSWORD` does not meet its policy: at least 8
characters, from three of uppercase, lowercase, digits and symbols. Update the repository secret
(Settings -> Secrets and variables -> Actions) and dispatch the workflow again. The secret's name
must be exactly `MSSQL_SA_PASSWORD`; under any other name the workflow silently uses its fallback.
