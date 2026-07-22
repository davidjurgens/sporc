"""
Remove turns that are stored more than once.

81,807 turns appear two to four times, duplicated verbatim: same text, same
timings, same speaker. They come from version 1.0 -- none of them carry
speakers_recomputed -- and the repack carried them forward faithfully. They are
in all three turn trees, so a join on (episode_id, turn_count) multiplies them,
and they inflate turn counts and every metric built from them.

Deduplication is on (episode_id, turn_count), keeping the first row. That is
exact rather than lossy here: a separate pass confirmed no key has copies that
differ in any column.

The row-group boundary is the constraint, as everywhere in this layout: one
podcast is one row group and metadata/shard_map.parquet addresses it by
position. Groups are rewritten in place, in order, so positions hold; only the
row counts change, and the map is rewritten to match.
"""

import glob
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from buildconfig import load as _load_build_config

_CFG = _load_build_config()

ROOT = _CFG.release
TREES = [
    ("turns_text", "turns/text"),
    ("turns_metrics", "turns/metrics"),
    ("acoustics", "acoustics"),
]


def dedupe_table(tbl):
    """Keep the first row of each (episode_id, turn_count)."""
    eids = tbl.column("episode_id").to_pylist()
    tcs = tbl.column("turn_count").to_pylist()
    seen = set()
    keep = []
    for i, key in enumerate(zip(eids, tcs)):
        if key in seen:
            continue
        seen.add(key)
        keep.append(i)
    if len(keep) == tbl.num_rows:
        return tbl, 0
    return tbl.take(pa.array(keep, pa.int64())), tbl.num_rows - len(keep)


def rewrite(path):
    pf = pq.ParquetFile(path)
    n_groups = pf.metadata.num_row_groups
    tmp = path + ".tmp"
    counts, removed, writer = [], 0, None
    try:
        for g in range(n_groups):
            tbl = pf.read_row_group(g)
            tbl, dropped = dedupe_table(tbl)
            removed += dropped
            if writer is None:
                writer = pq.ParquetWriter(tmp, tbl.schema, compression="zstd")
            writer.write_table(tbl, row_group_size=max(tbl.num_rows, 1))
            counts.append(tbl.num_rows)
    finally:
        if writer is not None:
            writer.close()

    check = pq.ParquetFile(tmp)
    if check.metadata.num_row_groups != n_groups:
        os.remove(tmp)
        raise SystemExit(f"{path}: row groups {n_groups} -> "
                         f"{check.metadata.num_row_groups}; map would break")
    os.replace(tmp, path)
    return os.path.basename(path), counts, removed


def main():
    t0 = time.time()
    # part file -> [rows per row group], per tree, so the map can be rewritten.
    new_counts = {}
    for tree, subdir in TREES:
        parts = sorted(glob.glob(f"{ROOT}/{subdir}/part-*.parquet"))
        removed = 0
        print(f"{tree}: {len(parts)} parts", flush=True)
        with ProcessPoolExecutor(max_workers=12) as ex:
            for name, counts, n in ex.map(rewrite, parts):
                new_counts[(tree, name)] = counts
                removed += n
        print(f"  removed {removed:,} duplicate rows", flush=True)

    # Rewrite the shard map's num_rows from what is now on disk.
    smap_path = f"{ROOT}/metadata/shard_map.parquet"
    smap = pq.read_table(smap_path)
    trees = smap.column("tree").to_pylist()
    parts = smap.column("part").to_pylist()
    groups = smap.column("row_group").to_pylist()
    rows = smap.column("num_rows").to_pylist()
    changed = 0
    for i, (tr, pt, g) in enumerate(zip(trees, parts, groups)):
        counts = new_counts.get((tr, pt))
        if counts is None:
            continue
        if rows[i] != counts[g]:
            rows[i] = counts[g]
            changed += 1
    out = smap.set_column(smap.schema.get_field_index("num_rows"), "num_rows",
                          pa.array(rows, smap.schema.field("num_rows").type))
    pq.write_table(out, smap_path)
    print(f"shard map: {changed:,} entries updated", flush=True)

    for tree, subdir in TREES:
        total = sum(pq.ParquetFile(p).metadata.num_rows
                    for p in sorted(glob.glob(f"{ROOT}/{subdir}/part-*.parquet")))
        print(f"  {tree:14s} now {total:,} rows", flush=True)
    print(f"done in {(time.time()-t0)/60:.1f}m", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
