"""
Rebuild the turn and episode metrics against the repacked turns.

``scripts/build_indexes.py`` derives these by walking ``turns/podcast_id=*/``,
one directory per podcast. That layout is gone, so the walk is replaced by a
pass over the written ``turns/text`` parts.

Output mirrors ``turns/text`` exactly: one metrics part per text part, same
name, same row groups in the same order. A podcast's metrics therefore sit at
the same row group index as its turns, and the shard map for the metrics tree
is the turn map with the tree renamed. Parts are independent, so this runs one
worker per part.

One number changes meaning. v1.0's ``unique_speaker_count`` was computed from
the turn index rather than the speaker labels -- the source comments it as a
placeholder -- so it equals ``total_turn_count`` in 98.85% of episodes and runs
as high as 7,683. The repacked turns carry real speaker labels, so it is now
the count of distinct labels in the episode.
"""

import glob
import os
import re
import statistics
import sys
import time
from collections import defaultdict
from multiprocessing import Pool

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from buildconfig import load as _load_build_config

_CFG = _load_build_config()

BUILD = _CFG.build
OUT = _CFG.release

# Copied verbatim from scripts/build_indexes.py so the counts stay comparable
# with v1.0's. Change it there and here together, or the two stop matching.
DISCOURSE_MARKERS = re.compile(
    r"\b(um|uh|uh huh|mm hmm|like|you know|i mean|so|well|right|okay|oh)\b",
    re.IGNORECASE,
)

TURN_METRIC_SCHEMA = pa.schema([
    ("episode_id", pa.string()),
    ("turn_count", pa.int32()),
    ("word_count", pa.int32()),
    ("words_per_second", pa.float32()),
    ("gap_from_prev", pa.float32()),
    ("overlap_with_prev", pa.float32()),
    ("discourse_marker_count", pa.int16()),
    ("char_count", pa.int32()),
])

READ_COLS = ["episode_id", "podcast_id", "speaker", "turn_text",
             "start_time", "end_time", "duration", "turn_count",
             "inferred_speaker_role"]


def _episode_rows(tbl):
    """Column lists plus row indices grouped by episode."""
    cols = {c: tbl.column(c).to_pylist() for c in READ_COLS}
    rows = defaultdict(list)
    for i, eid in enumerate(cols["episode_id"]):
        rows[eid].append(i)
    return cols, rows


def _one_group(cols, rows, out, ep_metrics):
    """Metrics for one podcast's row group. Appends to `out` and `ep_metrics`."""
    texts, starts, ends = cols["turn_text"], cols["start_time"], cols["end_time"]
    durs, tcs, roles = cols["duration"], cols["turn_count"], cols["inferred_speaker_role"]
    spks, pids = cols["speaker"], cols["podcast_id"]

    for eid, idxs in rows.items():
        idxs.sort(key=lambda i: starts[i] or 0.0)

        words = host_words = guest_words = host_turns = guest_turns = dm_total = 0
        ep_durs, ep_gaps, ep_overlaps = [], [], []
        labels = set()
        prev_end = None

        for i in idxs:
            text = str(texts[i]) if texts[i] else ""
            st = float(starts[i]) if starts[i] is not None else 0.0
            et = float(ends[i]) if ends[i] is not None else 0.0
            dur = float(durs[i]) if durs[i] is not None else 0.0
            tc = int(tcs[i]) if tcs[i] is not None else 0
            role = (str(roles[i]) if roles[i] else "").lower()

            wc = len(text.split())
            dm = len(DISCOURSE_MARKERS.findall(text))
            wps = wc / dur if dur > 0 else 0.0

            gap = overlap = None
            if prev_end is not None and st > 0:
                diff = st - prev_end
                gap, overlap = (diff, 0.0) if diff >= 0 else (0.0, abs(diff))

            words += wc
            dm_total += dm
            if dur > 0:
                ep_durs.append(dur)
            if gap:
                ep_gaps.append(gap)
            if overlap:
                ep_overlaps.append(overlap)

            # Real speaker labels, not the turn index v1.0 used.
            labels.update(spks[i] or [])

            if "host" in role:
                host_words += wc
                host_turns += 1
            elif "guest" in role:
                guest_words += wc
                guest_turns += 1

            out["episode_id"].append(eid)
            out["turn_count"].append(tc)
            out["word_count"].append(wc)
            out["words_per_second"].append(round(wps, 4))
            out["gap_from_prev"].append(round(gap, 4) if gap is not None else None)
            out["overlap_with_prev"].append(
                round(overlap, 4) if overlap is not None else None)
            out["discourse_marker_count"].append(min(dm, 32767))
            out["char_count"].append(len(text))

            if et > 0:
                prev_end = et

        n = len(idxs)
        total_dur = sum(ep_durs)
        host_dur = sum(float(durs[i]) for i in idxs
                       if durs[i] and str(roles[i] or "").lower().startswith("host"))
        guest_dur = sum(float(durs[i]) for i in idxs
                        if durs[i] and str(roles[i] or "").lower().startswith("guest"))

        ep_metrics[eid] = {
            "episode_id": eid,
            "podcast_id": pids[idxs[0]],
            "total_word_count": words,
            "total_turn_count": n,
            "unique_speaker_count": len(labels),
            "avg_turn_duration": round(statistics.mean(ep_durs), 4) if ep_durs else 0.0,
            "median_turn_duration": round(statistics.median(ep_durs), 4) if ep_durs else 0.0,
            "avg_words_per_second": round(words / total_dur, 4) if total_dur > 0 else 0.0,
            "host_word_count": host_words,
            "guest_word_count": guest_words,
            "host_turn_proportion": round(host_turns / n, 4) if n else 0.0,
            "host_word_proportion": round(host_words / words, 4) if words else 0.0,
            "avg_gap_duration": round(statistics.mean(ep_gaps), 4) if ep_gaps else 0.0,
            "total_overlap_duration": round(sum(ep_overlaps), 4),
            "discourse_marker_count": dm_total,
            "discourse_marker_rate": round(dm_total / words * 1000, 4) if words else 0.0,
            "speaking_rate_host": round(host_words / host_dur, 4) if host_dur > 0 else 0.0,
            "speaking_rate_guest": round(guest_words / guest_dur, 4) if guest_dur > 0 else 0.0,
        }


def process_part(path):
    """One turns/text part -> one metrics part with matching row groups."""
    name = os.path.basename(path)
    dest = os.path.join(OUT, "turns/metrics", name)
    if os.path.exists(dest):
        return name, None, True

    pf = pq.ParquetFile(path)
    ep_metrics = {}
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    writer = pq.ParquetWriter(dest + ".tmp", TURN_METRIC_SCHEMA, compression="zstd")
    try:
        for g in range(pf.metadata.num_row_groups):
            tbl = pf.read_row_group(g, columns=READ_COLS)
            cols, rows = _episode_rows(tbl)
            out = {n: [] for n in TURN_METRIC_SCHEMA.names}
            _one_group(cols, rows, out, ep_metrics)
            # Rows come back grouped by episode; the turns tree orders by
            # episode too, but not necessarily identically, so metrics are
            # joined on (episode_id, turn_count) rather than by position.
            writer.write_table(pa.table(out, schema=TURN_METRIC_SCHEMA))
    finally:
        writer.close()
    os.replace(dest + ".tmp", dest)
    return name, ep_metrics, False


def main():
    parts = sorted(glob.glob(f"{OUT}/turns/text/part-*.parquet"))
    if not parts:
        raise SystemExit("no turns/text parts; run stage.py first")
    workers = int(os.environ.get("SPORC_WORKERS", "16"))
    print(f"{len(parts)} parts, {workers} workers", flush=True)

    t0 = time.time()
    all_ep = {}
    done = skipped = 0
    with Pool(min(workers, len(parts))) as pool:
        for name, ep, was_skipped in pool.imap_unordered(process_part, parts):
            done += 1
            if was_skipped:
                skipped += 1
            else:
                dup = all_ep.keys() & ep.keys()
                if dup:
                    raise SystemExit(f"{name}: {len(dup)} episodes seen in "
                                     "another part; parts are not independent")
                all_ep.update(ep)
            print(f"  {done}/{len(parts)}  episodes {len(all_ep):,}", flush=True)

    if skipped:
        # Episode metrics are only returned by parts this run actually built, so
        # a partial rerun would silently drop the rest.
        raise SystemExit(f"{skipped} parts were already present; delete "
                         "turns/metrics and rerun so episode_metrics is complete")

    rows = list(all_ep.values())
    print(f"writing episode_metrics for {len(rows):,} episodes", flush=True)
    cols = {k: [r[k] for r in rows] for k in rows[0]}
    types = {"total_word_count": pa.int32(), "total_turn_count": pa.int32(),
             "unique_speaker_count": pa.int32(), "host_word_count": pa.int32(),
             "guest_word_count": pa.int32(), "discourse_marker_count": pa.int32()}
    tbl = pa.table({k: pa.array(v, type=types.get(k)) for k, v in cols.items()})
    pq.write_table(tbl, f"{OUT}/metadata/episode_metrics.parquet", compression="zstd")

    # The metrics tree is written part-for-part and group-for-group against the
    # turns tree, so its shard map is the turn map with the tree renamed.
    smap = pq.read_table(f"{OUT}/metadata/shard_map.parquet")
    txt = smap.filter(pc.equal(smap.column("tree"), "turns_text"))
    if txt.num_rows:
        # Drop any turns_metrics rows already there before adding them back.
        # Appending unconditionally meant a second run doubled the tree's
        # entries, and a shard map with two rows per podcast resolves to
        # whichever it happens to see first.
        keep = smap.filter(pc.not_equal(smap.column("tree"), "turns_metrics"))
        mtree = txt.set_column(txt.schema.get_field_index("tree"), "tree",
                               pa.array(["turns_metrics"] * txt.num_rows, pa.string()))
        pq.write_table(pa.concat_tables([keep, mtree]),
                       f"{OUT}/metadata/shard_map.parquet", compression="zstd")
        print(f"shard_map: {mtree.num_rows:,} turns_metrics rows "
              f"({smap.num_rows - keep.num_rows:,} replaced)", flush=True)

    print(f"done in {(time.time()-t0)/60:.1f}m", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
