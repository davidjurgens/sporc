"""
Resolve which episodes can be rebuilt, and where their inputs live.

An episode is buildable when the corpus has both word timings
(``prosodyMerged``) and speaker segments (``diarization/mayJune``) for it. The
file name in both trees is the mp3 URL with every character outside
``[A-Za-z0-9.]`` stripped, so that sanitised form is the join key back to the
episode catalog.
"""

import os
import re
import sys

import pyarrow as pa
import pyarrow.parquet as pq

from buildconfig import load as _load_build_config

_CFG = _load_build_config()

BUILD = _CFG.build
V1 = _CFG.source

sanitize = re.compile(r"[^A-Za-z0-9.]").sub


def key_of(url):
    return sanitize("", url)


def load_paths(path, suffix):
    """Map sanitised episode key -> full path, for a listing of one tree."""
    out = {}
    with open(path) as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line:
                continue
            base = os.path.basename(line)
            if suffix and base.endswith(suffix):
                base = base[: -len(suffix)]
            # First writer wins; duplicates across batches are the same episode.
            out.setdefault(base, line)
    return out


def main():
    print("loading catalog...", flush=True)
    cat = pq.ParquetFile(f"{V1}/metadata/episode_catalog.parquet").read(
        columns=["episode_id", "podcast_id", "mp3_url", "total_sp_labels"]
    )
    eid = cat.column("episode_id").to_pylist()
    pid = cat.column("podcast_id").to_pylist()
    url = cat.column("mp3_url").to_pylist()
    spl = cat.column("total_sp_labels").to_pylist()
    print(f"  {len(eid):,} episodes", flush=True)

    print("loading prosody paths...", flush=True)
    pros = load_paths(f"{BUILD}/prosody_paths.txt", "MERGED")
    print(f"  {len(pros):,} prosody files", flush=True)

    print("loading rttm paths...", flush=True)
    rttm = load_paths(f"{BUILD}/rttm_paths.txt", ".rttm")
    print(f"  {len(rttm):,} rttm files", flush=True)

    rows = {"episode_id": [], "podcast_id": [], "prosody_path": [],
            "rttm_path": [], "had_turns": []}
    n_new = n_rebuild = 0
    for i, u in enumerate(url):
        k = key_of(u)
        p = pros.get(k)
        if p is None:
            continue
        r = rttm.get(k)
        if r is None:
            continue
        had = bool((spl[i] or 0) > 0)
        rows["episode_id"].append(eid[i])
        rows["podcast_id"].append(pid[i])
        rows["prosody_path"].append(p)
        rows["rttm_path"].append(r)
        rows["had_turns"].append(had)
        if had:
            n_rebuild += 1
        else:
            n_new += 1

    tbl = pa.table(rows)
    out = f"{BUILD}/worklist.parquet"
    pq.write_table(tbl, out, compression="zstd")
    print(f"\nwrote {out}")
    print(f"  buildable episodes : {tbl.num_rows:,}")
    print(f"    newly diarized   : {n_new:,}")
    print(f"    rebuilt (had turns): {n_rebuild:,}")
    print(f"  distinct podcasts  : {len(set(rows['podcast_id'])):,}")


if __name__ == "__main__":
    sys.exit(main())
