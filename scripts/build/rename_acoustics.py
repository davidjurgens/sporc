"""
Put the acoustic columns back on v1.0's naming convention.

The rebuild takes its feature names from the openSMILE output it reads
(``mfcc1_sma3Mean``), but v1.0 published them in snake_case
(``mfcc1_sma3_mean``). Shipping the camelCase names would break existing code
for no reason other than which upstream file the values came through, so the
six carried-over columns keep their v1.0 names and the six new standard
deviations follow the same convention.

Row groups are rewritten one for one, so the shard map stays valid: a podcast
remains at the same row group index of the same part.
"""

import glob
import os
import sys
import time
from multiprocessing import Pool

import pyarrow as pa
import pyarrow.parquet as pq

from buildconfig import load as _load_build_config

_CFG = _load_build_config()

BUILD = _CFG.build
OUT = _CFG.release

BASE = {
    "mfcc1_sma3": "mfcc1_sma3",
    "mfcc2_sma3": "mfcc2_sma3",
    "mfcc3_sma3": "mfcc3_sma3",
    "mfcc4_sma3": "mfcc4_sma3",
    "F0semitoneFrom27.5Hz_sma3nz": "f0_semitone_from_27_5hz_sma3nz",
    "F1frequency_sma3nz": "f1_frequency_sma3nz",
}
RENAME = {f"{src}{stat}": f"{dst}_{stat.lower()}"
          for src, dst in BASE.items() for stat in ("Mean", "StDev")}


def rename_part(path):
    tmp = path + ".renamed"
    pf = pq.ParquetFile(path)
    names = [RENAME.get(n, n) for n in pf.schema_arrow.names]
    schema = pa.schema([pa.field(new, f.type) for new, f
                        in zip(names, pf.schema_arrow)])

    if names == pf.schema_arrow.names:
        return os.path.basename(path), 0, True

    w = pq.ParquetWriter(tmp, schema, compression="zstd")
    try:
        for g in range(pf.metadata.num_row_groups):
            t = pf.read_row_group(g)
            w.write_table(pa.table(t.columns, schema=schema))
    finally:
        w.close()

    # Only swap once the replacement is complete on disk.
    check = pq.ParquetFile(tmp)
    if (check.metadata.num_rows != pf.metadata.num_rows
            or check.metadata.num_row_groups != pf.metadata.num_row_groups):
        os.remove(tmp)
        raise SystemExit(f"{path}: rewrite does not match the original")
    os.replace(tmp, path)
    return os.path.basename(path), pf.metadata.num_rows, False


def main():
    parts = sorted(glob.glob(f"{OUT}/acoustics/part-*.parquet"))
    if not parts:
        raise SystemExit("no acoustics parts")
    workers = int(os.environ.get("SPORC_WORKERS", "12"))
    print(f"{len(parts)} parts, {workers} workers", flush=True)
    print("renaming:", ", ".join(f"{k} -> {v}" for k, v in
                                 list(RENAME.items())[:2]), "...", flush=True)

    t0 = time.time()
    rows = done = skipped = 0
    with Pool(min(workers, len(parts))) as pool:
        for name, n, was_skipped in pool.imap_unordered(rename_part, parts):
            done += 1
            rows += n
            skipped += was_skipped
            if done % 20 == 0 or done == len(parts):
                print(f"  {done}/{len(parts)}  {rows:,} rows", flush=True)
    print(f"done in {(time.time()-t0)/60:.1f}m, {rows:,} rows, "
          f"{skipped} already renamed", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
