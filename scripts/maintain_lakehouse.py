#!/usr/bin/env python3
"""
Delta maintenance: compact small files, then expire old versions.

    python -m scripts.maintain_lakehouse [--vacuum] [--retain-hours 168] [--layer bronze]

Why this is needed at all: bronze writes one partition per run, and each run of a table with few
changes writes a handful of tiny files. Reading a table then costs one request per file, and the
transaction log grows a row per file. Compaction rewrites them into larger files without changing
a single value; it is pure I/O bookkeeping.

Two separate operations, deliberately separate:
- OPTIMIZE rewrites small files into bigger ones. Safe to run any time, and time travel still works
  because the old files stay until they are vacuumed.
- VACUUM deletes files no longer referenced by any retained version. That is what actually frees
  space, and it is what makes time travel beyond the retention window impossible. It only runs when
  asked (`--vacuum`), because "reclaim space" and "discard history" are the same button.

The default 168-hour retention is Delta's own, and it is the one to keep: a shorter window can
delete files a concurrent reader is still using.
"""

import argparse
import os

from delta.tables import DeltaTable

from databricks.utilities.config import BRONZE_PATH, GOLD_PATH, META_PATH, SILVER_PATH, get_spark_session

LAYERS = {"bronze": BRONZE_PATH, "silver": SILVER_PATH, "gold": GOLD_PATH, "metadata": META_PATH}


def active_files(spark, path: str) -> tuple[int, int]:
    """(count, bytes) of the files the CURRENT table version references.

    Not the files on disk: OPTIMIZE writes new files and leaves the superseded ones in place until
    VACUUM, so counting the filesystem makes compaction look like it doubled the table.
    """
    files = [uri.replace("file:", "") for uri in spark.read.format("delta").load(path).inputFiles()]
    return len(files), sum(os.path.getsize(f) for f in files if os.path.exists(f))


def files_on_disk(path: str) -> int:
    """Every data file present, including versions only time travel can still reach."""
    return sum(
        1 for root, _, files in os.walk(path) if "_delta_log" not in root for name in files if name.endswith(".parquet")
    )


def delta_tables(layer_paths: dict[str, str]) -> list[tuple[str, str, str]]:
    found = []
    for layer, root in layer_paths.items():
        if not os.path.isdir(root):
            continue
        for name in sorted(os.listdir(root)):
            path = os.path.join(root, name)
            if os.path.isdir(os.path.join(path, "_delta_log")):
                found.append((layer, name, path))
    return found


def maintain(layer: str | None = None, do_vacuum: bool = False, retain_hours: int = 168) -> None:
    spark = get_spark_session("ClinicalFlow_Maintenance")
    spark.sparkContext.setLogLevel("ERROR")

    selected = LAYERS if layer is None else {layer: LAYERS[layer]}
    tables = delta_tables(selected)
    if not tables:
        print("no Delta tables found")
        return

    header = f"{'table':38} {'active before':>14} {'active after':>13} {'MB active':>10} {'on disk':>9}"
    print(header)
    totals = [0, 0, 0, 0]
    for layer_name, name, path in tables:
        before_files, _ = active_files(spark, path)
        table = DeltaTable.forPath(spark, path)
        table.optimize().executeCompaction()
        if do_vacuum:
            table.vacuum(retain_hours)
        after_files, after_bytes = active_files(spark, path)
        disk_files = files_on_disk(path)

        totals[0] += before_files
        totals[1] += after_files
        totals[2] += after_bytes
        totals[3] += disk_files
        print(
            f"{layer_name + '/' + name:38} {before_files:>14} {after_files:>13} "
            f"{after_bytes / 1e6:>10.1f} {disk_files:>9}"
        )

    print(f"\n{'TOTAL':38} {totals[0]:>14} {totals[1]:>13} {totals[2] / 1e6:>10.1f} {totals[3]:>9}")
    if totals[3] > totals[1]:
        print(
            f"\n{totals[3] - totals[1]} superseded file(s) remain on disk: that is what time travel reads,\n"
            "and what --vacuum reclaims."
        )
    if not do_vacuum:
        print(
            "\nFiles compacted. The originals are still on disk so time travel keeps working; pass\n"
            "--vacuum to delete versions older than the retention window and reclaim the space."
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--layer", choices=sorted(LAYERS), help="limit to one layer")
    parser.add_argument("--vacuum", action="store_true", help="also delete files outside the retention window")
    parser.add_argument("--retain-hours", type=int, default=168, help="retention for --vacuum (default Delta's 168)")
    args = parser.parse_args()
    maintain(args.layer, args.vacuum, args.retain_hours)
