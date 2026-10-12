"""seed_drift: which differences between the seed and pipeline_config count as drift."""

import dataclasses

from databricks.utilities import control


def test_seed_drift_is_a_pure_comparison():
    seed = control.PIPELINE_CONFIG_SEED
    as_rows = [dataclasses.asdict(c) for c in seed]
    assert control.seed_drift(as_rows, seed) is False

    # Operational column only: not drift.
    flipped = [{**r, "active_flag": not r["active_flag"]} for r in as_rows]
    assert control.seed_drift(flipped, seed) is False

    # A definition column differs, or a seed row is missing: drift.
    retyped = [{**r, "ingestion_type": "Full"} if r["source_table"] == "Patient" else r for r in as_rows]
    assert control.seed_drift(retyped, seed) is True
    assert control.seed_drift(as_rows[1:], seed) is True

    # Extra rows the seed does not know (an operator's or a test's) are not drift.
    extra = [*as_rows, {**as_rows[0], "source_table": "operator_added"}]
    assert control.seed_drift(extra, seed) is False
