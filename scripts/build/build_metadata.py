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
import json
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


def _explode_names(rows):
    """Yield (normalized, original) for a list of raw host names, deduped."""
    seen = set()
    for name in rows or []:
        if name is None:
            continue
        s = str(name).strip()
        low = s.lower()
        if not s or low in seen:
            continue
        seen.add(low)
        yield low, s


def build_host_indexes():
    """
    Host-name lookups, at two grains, built from the catalogs alone.

    ``host_index.parquet`` is podcast-grained -- (host name -> podcast) exploded
    from ``podcast_catalog.host_names`` -- so a client can answer "which shows
    does this person host" from the ~195 MB metadata without touching a part
    file. ``host_episode_index.parquet`` carries the same names down to the
    episode via ``episode_catalog.host_predicted_names``.

    Neither is affected by the diarization rebuild: host names come from the
    episode/podcast catalogs, not the turn labels (same reasoning as
    copy_unchanged's note on speaker_name_index). Reading the rebuilt
    episode_catalog here is safe because the rebuild leaves host_predicted_names
    untouched. This is the host half of speaker_name_index without the predicted
    -guest rows, whose names are mentions rather than appearances.
    """
    pc = pq.ParquetFile(
        f"{OUT}/metadata/podcast_catalog.parquet").read(
        columns=["podcast_id", "host_names"])
    p_norm, p_orig, p_pid = [], [], []
    for pid, hosts in zip(pc.column("podcast_id").to_pylist(),
                          pc.column("host_names").to_pylist()):
        for low, orig in _explode_names(hosts):
            p_norm.append(low)
            p_orig.append(orig)
            p_pid.append(pid)
    pq.write_table(
        pa.table({
            "name_normalized": pa.array(p_norm, type=pa.string()),
            "name_original": pa.array(p_orig, type=pa.string()),
            "podcast_id": pa.array(p_pid, type=pa.string()),
        }),
        f"{OUT}/metadata/host_index.parquet", compression="zstd")
    print(f"host_index: {len(p_norm):,} rows "
          f"({len(set(p_norm)):,} distinct hosts)", flush=True)

    ec = pq.ParquetFile(
        f"{OUT}/metadata/episode_catalog.parquet").read(
        columns=["episode_id", "podcast_id", "host_predicted_names"])
    e_norm, e_orig, e_pid, e_eid = [], [], [], []
    for eid, pid, hosts in zip(ec.column("episode_id").to_pylist(),
                               ec.column("podcast_id").to_pylist(),
                               ec.column("host_predicted_names").to_pylist()):
        for low, orig in _explode_names(hosts):
            e_norm.append(low)
            e_orig.append(orig)
            e_pid.append(pid)
            e_eid.append(eid)
    pq.write_table(
        pa.table({
            "name_normalized": pa.array(e_norm, type=pa.string()),
            "name_original": pa.array(e_orig, type=pa.string()),
            "podcast_id": pa.array(e_pid, type=pa.string()),
            "episode_id": pa.array(e_eid, type=pa.string()),
        }),
        f"{OUT}/metadata/host_episode_index.parquet", compression="zstd")
    print(f"host_episode_index: {len(e_norm):,} rows", flush=True)


def _part_guests(path):
    """
    Diarized guests in one episode part: (normalized, original, podcast, episode).

    ``guest_speaker_labels`` is a JSON object mapping a guest's name to the
    speaker label diarization assigned them, so its keys are the guests who
    actually spoke -- unlike ``guest_predicted_names``, which lists everyone
    named in the text and so includes people who were only discussed.
    """
    t = pq.ParquetFile(path).read(
        columns=["podcast_id", "episode_id", "guest_speaker_labels"])
    rows = []
    for pid, eid, gl in zip(t.column("podcast_id").to_pylist(),
                            t.column("episode_id").to_pylist(),
                            t.column("guest_speaker_labels").to_pylist()):
        if not gl or gl in ("{}", "SPEAKER_DATA_UNAVAILABLE"):
            continue
        raw = gl if isinstance(gl, str) else json.dumps(gl)
        try:
            d = json.loads(raw)
        except Exception:
            continue
        if not isinstance(d, dict):
            continue
        for k in d:
            s = str(k).strip()
            if s:
                rows.append((s.lower(), s, pid, eid))
    return rows


def build_guest_indexes():
    """
    Diarized-guest lookups, at two grains, from ``guest_speaker_labels``.

    This is the appearance-verified counterpart to the guest rows of
    ``speaker_name_index``: those come from ``guest_predicted_names`` and carry
    the mention artefact (George Floyd indexed as a guest on 237 podcasts he
    never appeared on), whereas these come from the diarization labels, so a
    name here is someone who actually spoke on that episode.

    Built by scanning the episode parts once at build time -- the same
    guest_speaker_labels scan the tutorial subset builder used to run over the
    Hub on every invocation. ``guest_index.parquet`` is podcast-grained (guest
    -> podcast) and ``guest_episode_index.parquet`` carries it down to the
    episode, mirroring the two host indexes.
    """
    parts = sorted(glob.glob(f"{OUT}/episodes/part-*.parquet"))
    if not parts:
        raise SystemExit("no episodes parts; run stage.py first")
    workers = int(os.environ.get("SPORC_WORKERS", "16"))
    print(f"scanning {len(parts)} episode parts for diarized guests, "
          f"{workers} workers...", flush=True)

    e_norm, e_orig, e_pid, e_eid = [], [], [], []
    done = 0
    with Pool(min(workers, len(parts))) as pool:
        for rows in pool.imap_unordered(_part_guests, parts):
            for low, orig, pid, eid in rows:
                e_norm.append(low)
                e_orig.append(orig)
                e_pid.append(pid)
                e_eid.append(eid)
            done += 1
            print(f"  {done}/{len(parts)}  guest rows so far {len(e_norm):,}",
                  flush=True)

    pq.write_table(
        pa.table({
            "name_normalized": pa.array(e_norm, type=pa.string()),
            "name_original": pa.array(e_orig, type=pa.string()),
            "podcast_id": pa.array(e_pid, type=pa.string()),
            "episode_id": pa.array(e_eid, type=pa.string()),
        }),
        f"{OUT}/metadata/guest_episode_index.parquet", compression="zstd")
    print(f"guest_episode_index: {len(e_norm):,} rows", flush=True)

    # Podcast grain: distinct (guest, podcast). Keep the first spelling seen for
    # each pair, which is enough to render a name.
    seen = {}
    for low, orig, pid in zip(e_norm, e_orig, e_pid):
        seen.setdefault((low, pid), orig)
    p_norm, p_orig, p_pid = [], [], []
    for (low, pid), orig in seen.items():
        p_norm.append(low)
        p_orig.append(orig)
        p_pid.append(pid)
    pq.write_table(
        pa.table({
            "name_normalized": pa.array(p_norm, type=pa.string()),
            "name_original": pa.array(p_orig, type=pa.string()),
            "podcast_id": pa.array(p_pid, type=pa.string()),
        }),
        f"{OUT}/metadata/guest_index.parquet", compression="zstd")
    print(f"guest_index: {len(p_norm):,} rows "
          f"({len(set(p_norm)):,} distinct diarized guests)", flush=True)


def main():
    os.makedirs(f"{OUT}/metadata", exist_ok=True)
    if "--host-only" in sys.argv:
        # Rebuild just the host indexes against the finished catalogs, without
        # re-running the turn-stats pass.
        build_host_indexes()
        return 0
    if "--guest-only" in sys.argv:
        # Rebuild just the diarized-guest indexes from the episode parts.
        build_guest_indexes()
        return 0
    copy_unchanged()
    stats = turn_stats()
    print(f"episodes with turns: {len(stats):,}", flush=True)
    rebuild_episode_catalog(stats)
    build_host_indexes()
    build_guest_indexes()
    return 0


if __name__ == "__main__":
    sys.exit(main())
