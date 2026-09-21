# Decisions log

Every non-obvious choice: what we decided, why, the alternative rejected, and what changes at real scale. Newest last.

## 2026-09-21 — Gap analysis: the pipeline was idempotent only by full overwrite

**Finding:** every layer wrote with `mode("overwrite")`, so rerunning produced the same counts. That passes a rerun test but is the weakest form of idempotency: every run rewrites all history, it can't carry deletes or updates incrementally, and bronze couldn't be immutable.
**Decision:** rebuild around append-only bronze + Delta `MERGE` in silver/gold, with watermarks in `pipeline_config`. Tracked as the fix plan in `CLAUDE.md`.
**Interview version:** "My first version was 'idempotent' only because it rewrote everything each run. I replaced that with append-only raw data and key-based MERGE, so a rerun converges to the same state without reprocessing history."
