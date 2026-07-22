"""
Rename turns/text.word_count to token_count.

The dataset shipped two columns called word_count meaning different things:
this one counts the timestamped tokens the transcript aligned to a turn, with
punctuation as its own token, while turns/metrics.word_count counts
whitespace-separated words. The median ratio between them is 1.21, so a reader
who joined the trees and picked either got a number that did not mean what the
name said.

Rewrites each part row group by row group, in order, so the row-group index in
metadata/shard_map.parquet keeps pointing at the same podcast. That is the
whole constraint here: one podcast is one row group, and the map addresses it
by position.
"""

import glob
import os
import shutil
import sys
import time
from concurrent.futures import ProcessPoolExecutor

import pyarrow as pa
import pyarrow.parquet as pq

from buildconfig import load as _load_build_config

_CFG = _load_build_config()

ROOT = _CFG.release
OLD, NEW = "word_count", "token_count"


def rewrite(path):
    pf = pq.ParquetFile(path)
    names = pf.schema_arrow.names
    if NEW in names:
        return path, pf.metadata.num_row_groups, 0, "already renamed"
    if OLD not in names:
        return path, 0, 0, f"no {OLD} column"

    renamed = [NEW if n == OLD else n for n in names]
    tmp = path + ".tmp"
    n_groups = pf.metadata.num_row_groups
    rows = 0
    writer = None
    try:
        for g in range(n_groups):
            tbl = pf.read_row_group(g).rename_columns(renamed)
            if writer is None:
                writer = pq.ParquetWriter(tmp, tbl.schema, compression="zstd")
            # One write_table per group preserves the boundaries the shard map
            # indexes; batching them would silently re-chunk the file.
            writer.write_table(tbl, row_group_size=tbl.num_rows)
            rows += tbl.num_rows
    finally:
        if writer is not None:
            writer.close()

    check = pq.ParquetFile(tmp)
    if check.metadata.num_row_groups != n_groups:
        os.remove(tmp)
        raise SystemExit(
            f"{path}: row groups changed {n_groups} -> "
            f"{check.metadata.num_row_groups}; shard map would break")
    if check.metadata.num_rows != rows:
        os.remove(tmp)
        raise SystemExit(f"{path}: row count changed")
    os.replace(tmp, path)
    return path, n_groups, rows, "ok"


def main():
    parts = sorted(glob.glob(f"{ROOT}/turns/text/part-*.parquet"))
    if not parts:
        raise SystemExit("no turns/text parts found")
    print(f"rewriting {len(parts)} parts", flush=True)
    t0 = time.time()
    done = groups = rows = 0
    with ProcessPoolExecutor(max_workers=12) as ex:
        for path, g, r, status in ex.map(rewrite, parts):
            done += 1
            groups += g
            rows += r
            if status != "ok" or done % 20 == 0:
                print(f"  [{done}/{len(parts)}] {os.path.basename(path)}: {status}",
                      flush=True)
    print(f"done: {groups:,} row groups, {rows:,} rows, "
          f"{(time.time()-t0)/60:.1f}m", flush=True)


if __name__ == "__main__":
    sys.exit(main())
