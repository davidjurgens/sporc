"""
Rebuild the metadata catalogs against the repacked turns.

The episode catalog carries several columns describing an episode's speaker
turns. In v1.0 those were copied from the source JSONL rather than derived from
the turn files, so after a rebuild they no longer describe what is actually in
the dataset: they read zero for every newly diarized episode, and they are stale
for every episode whose turns were recomputed. Client code tests
``total_sp_labels > 0`` to decide whether an episode has turns at all, so
leaving them alone would hide the new data entirely.

Four of the columns were checked against v1.0 and reproduce it closely, so they
are recomputed for every episode:

    total_sp_labels          distinct speaker labels            100%
    overlap_prop_duration    multi-speaker duration / total     100%
    avg_turn_duration        mean turn duration                 98.7%
    overlap_prop_turn_count  multi-speaker turns / all turns     98.7%

``main_ep_speakers`` is left alone where v1.0 supplied it. It is exactly the
list whose length is ``num_main_speakers``, but it is not a speaking-time
threshold -- one episode marks a speaker with a 4.9% share as main, which no
cutoff near 5% reproduces -- and the rule that produced it lived in the upstream
pipeline. Rather than invent a threshold that demonstrably does not match,
newly diarized episodes list every speaker detected, which the README describes.
"""

import glob
import os
import sys
from collections import defaultdict
from multiprocessing import Pool

import pyarrow as pa
import pyarrow.parquet as pq

from buildconfig import load as _load_build_config

_CFG = _load_build_config()

BUILD = _CFG.build
V1 = _CFG.source
OUT = _CFG.release

NO_NAME = "NO_INFERRED_SPEAKER"
NO_ROLE = "NO_INFERRED_ROLE"


def _part_stats(path):
    """
    Per-episode turn statistics for one part.

    These are final, not partial. A podcast occupies exactly one row group in
    exactly one part, so every turn of an episode lands in the same file and no
    episode straddles a part boundary. That is what lets the parts be summarised
    independently and simply concatenated.
    """
    t = pq.ParquetFile(path).read(columns=["episode_id", "speaker", "duration"])
    labels = defaultdict(set)
    n_turns = defaultdict(int)
    n_multi = defaultdict(int)
    sum_dur = defaultdict(float)
    multi_dur = defaultdict(float)
    for eid, spk, dur in zip(t.column("episode_id").to_pylist(),
                             t.column("speaker").to_pylist(),
                             t.column("duration").to_pylist()):
        d = dur or 0.0
        n_turns[eid] += 1
        sum_dur[eid] += d
        s = spk or []
        labels[eid].update(s)
        if len(s) > 1:
            n_multi[eid] += 1
            multi_dur[eid] += d

    out = {}
    for eid, n in n_turns.items():
        tot = sum_dur[eid]
        out[eid] = {
            "total_sp_labels": len(labels[eid]),
            "speakers": sorted(labels[eid]),
            "avg_turn_duration": tot / n if n else 0.0,
            "overlap_prop_turn_count": n_multi[eid] / n if n else 0.0,
            "overlap_prop_duration": (multi_dur[eid] / tot) if tot else 0.0,
        }
    return os.path.basename(path), out


def turn_stats():
    """Per-episode turn statistics, accumulated over the repacked turn parts."""
    parts = sorted(glob.glob(f"{OUT}/turns/text/part-*.parquet"))
    if not parts:
        raise SystemExit("no turns/text parts; run stage.py first")
    workers = int(os.environ.get("SPORC_WORKERS", "16"))
    print(f"reading {len(parts)} turn parts, {workers} workers...", flush=True)

    stats = {}
    done = 0
    with Pool(min(workers, len(parts))) as pool:
        for name, part in pool.imap_unordered(_part_stats, parts):
            overlap = stats.keys() & part.keys()
            if overlap:
                # Would mean an episode spans two parts, which the layout rules
                # out. Better to hear about it than to silently take one side.
                raise SystemExit(
                    f"{name}: {len(overlap)} episodes already seen in another "
                    "part; per-part stats are not independent")
            stats.update(part)
            done += 1
            print(f"  {done}/{len(parts)}  episodes so far {len(stats):,}",
                  flush=True)
    return stats


def rebuild_episode_catalog(stats):
    """Write the episode catalog with turn-derived columns brought up to date."""
    cat = pq.ParquetFile(f"{V1}/metadata/episode_catalog.parquet").read()
    eids = cat.column("episode_id").to_pylist()
    cols = {n: cat.column(n).to_pylist() for n in cat.column_names}

    changed = added = 0
    for i, eid in enumerate(eids):
        s = stats.get(eid)
        if s is None:
            continue
        had = (cols["total_sp_labels"][i] or 0) > 0
        if not had:
            added += 1
        else:
            changed += 1
        cols["total_sp_labels"][i] = s["total_sp_labels"]
        cols["avg_turn_duration"][i] = s["avg_turn_duration"]
        cols["overlap_prop_duration"][i] = s["overlap_prop_duration"]
        if "overlap_prop_turn_count" in cols:
            cols["overlap_prop_turn_count"][i] = s["overlap_prop_turn_count"]
        # Only fill the "main speaker" fields where v1.0 had nothing to say.
        if "num_main_speakers" in cols and not had:
            cols["num_main_speakers"][i] = len(s["speakers"])

    out = pa.table({n: pa.array(v) for n, v in cols.items()})
    pq.write_table(out, f"{OUT}/metadata/episode_catalog.parquet",
                   compression="zstd")
    print(f"episode_catalog: {added:,} episodes gained turn stats, "
          f"{changed:,} updated", flush=True)


def copy_unchanged():
    """
    Indexes the diarization rebuild does not affect.

    ``speaker_name_index`` belongs here, which is not obvious. It is not built
    from turn-level speaker labels: it covers 555,150 episodes while only
    372,606 have turns at all, and in a sample of its rows barely a sixth had
    the indexed name present in that episode's turns. It comes from the
    episode-level ``host_predicted_names`` and ``guest_predicted_names``, which
    this release does not change. Rebuilding it from turns would cut it from
    921,287 rows to roughly 175,000 and quietly break speaker search for most
    episodes.
    """
    import shutil
    for name in ("podcast_catalog.parquet", "category_index.parquet",
                 "hostname_index.parquet", "speaker_name_index.parquet"):
        src = f"{V1}/metadata/{name}"
        dst = f"{OUT}/metadata/{name}"
        if os.path.exists(src) and not os.path.exists(dst):
            shutil.copy2(src, dst)
            print(f"copied {name}", flush=True)


def main():
    os.makedirs(f"{OUT}/metadata", exist_ok=True)
    copy_unchanged()
    stats = turn_stats()
    print(f"episodes with turns: {len(stats):,}", flush=True)
    rebuild_episode_catalog(stats)
    return 0


if __name__ == "__main__":
    sys.exit(main())
