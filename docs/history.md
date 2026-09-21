# History

Dated journal of what landed. Newest last.

## 2026-09-21

- Gap analysis of the existing scaffold against the ClinicalFlow spec; fix plan recorded in `CLAUDE.md`.
- Added `CLAUDE.md`, `docs/decisions.md`, `docs/history.md`.
- Removed the automated staggered-commit bot.
- Enabled Delta Lake for real (was silently writing Parquet); removed format fallbacks. All 21 tables verified to have `_delta_log`; 5/5 tests pass (34 s → 144 s).
