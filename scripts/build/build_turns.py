"""
Rebuild speaker turns for every buildable episode.

Reads the work list produced by ``build_worklist.py`` and, for each episode,
joins word timings against speaker segments to produce turns. Output is written
one file per shard so the run is restartable: a shard whose file already exists
is skipped, and killing the job costs at most the shards in flight.

The work is I/O bound — roughly a megabyte and a half of prosody per episode,
read cold from NFS — so throughput comes from running many workers, not from
faster parsing.
"""

import hashlib
import os
import sys
import time
from collections import defaultdict
from multiprocessing import Pool

import pyarrow as pa
import pyarrow.parquet as pq

import merge_lib as M

from buildconfig import load as _load_build_config

_CFG = _load_build_config()

BUILD = _CFG.build
OUT = os.path.join(BUILD, "turns_raw")
NSHARDS = 4096
WORKERS = int(os.environ.get("SPORC_WORKERS", "48"))

FEATURE_COLS = [f"{k}{stat}" for k in M.BASE_FEATURES for stat in ("Mean", "StDev")]

SCHEMA = pa.schema(
    [
        ("episode_id", pa.string()),
        ("podcast_id", pa.string()),
        ("speaker", pa.list_(pa.string())),
        ("turn_text", pa.string()),
        ("start_time", pa.float64()),
        ("end_time", pa.float64()),
        ("duration", pa.float64()),
        ("turn_count", pa.int32()),
        ("word_count", pa.int32()),
    ]
    + [(c, pa.float64()) for c in FEATURE_COLS]
)


def shard_of(podcast_id):
    """Stable shard for a podcast, independent of catalog ordering."""
    h = hashlib.blake2b(podcast_id.encode(), digest_size=8).digest()
    return int.from_bytes(h, "big") % NSHARDS


def build_shard(args):
    """Build every episode in one shard. Returns (shard, episodes, turns, errors)."""
    shard, episodes = args
    path = os.path.join(OUT, f"shard={shard:04d}.parquet")
    if os.path.exists(path):
        return (shard, 0, 0, 0, True)

    cols = {name: [] for name in SCHEMA.names}
    errors = 0
    for eid, pid, pro, rtm in episodes:
        try:
            turns = M.build_turns(pro, rtm)
        except Exception:
            errors += 1
            continue
        for t in turns:
            cols["episode_id"].append(eid)
            cols["podcast_id"].append(pid)
            cols["speaker"].append(t["speaker"])
            cols["turn_text"].append(t["turn_text"])
            cols["start_time"].append(t["start_time"])
            cols["end_time"].append(t["end_time"])
            cols["duration"].append(t["duration"])
            cols["turn_count"].append(t["turn_count"])
            cols["word_count"].append(t["word_count"])
            for c in FEATURE_COLS:
                cols[c].append(t.get(c))

    tbl = pa.table(cols, schema=SCHEMA)
    tmp = path + ".tmp"
    pq.write_table(tbl, tmp, compression="zstd")
    os.replace(tmp, path)
    return (shard, len(episodes), tbl.num_rows, errors, False)


def main():
    os.makedirs(OUT, exist_ok=True)
    wl = pq.read_table(os.path.join(BUILD, "worklist.parquet"))
    eids = wl.column("episode_id").to_pylist()
    pids = wl.column("podcast_id").to_pylist()
    pros = wl.column("prosody_path").to_pylist()
    rtms = wl.column("rttm_path").to_pylist()

    buckets = defaultdict(list)
    for i in range(len(eids)):
        buckets[shard_of(pids[i])].append((eids[i], pids[i], pros[i], rtms[i]))
    # Largest shards first so the tail of the run is not one huge straggler.
    work = sorted(buckets.items(), key=lambda kv: -len(kv[1]))
    print(f"{len(eids):,} episodes across {len(work):,} shards, {WORKERS} workers",
          flush=True)

    t0 = time.time()
    done = eps = turns = errs = skipped = 0
    with Pool(WORKERS) as pool:
        for shard, n_eps, n_turns, n_err, was_skipped in pool.imap_unordered(
                build_shard, work, chunksize=1):
            done += 1
            if was_skipped:
                skipped += 1
            eps += n_eps
            turns += n_turns
            errs += n_err
            if done % 50 == 0 or done == len(work):
                el = time.time() - t0
                rate = eps / el if el else 0
                left = (len(eids) - eps) / rate / 3600 if rate else 0
                print(f"  shards {done}/{len(work)}  episodes {eps:,}  turns {turns:,}"
                      f"  errors {errs}  skipped {skipped}"
                      f"  {rate:.0f} ep/s  eta {left:.1f}h", flush=True)

    print(f"\ndone in {(time.time()-t0)/3600:.2f}h: {eps:,} episodes, {turns:,} turns, "
          f"{errs} errors, {skipped} shards already present")


if __name__ == "__main__":
    sys.exit(main())
