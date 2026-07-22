"""
Pack the corpus into a few hundred large Parquet files instead of ~685k small ones.

v1.0 gave every podcast its own directory, so a full download meant 685k files —
far past what the Hub serves without rate-limiting, and slow even when it
worked. Here podcasts are laid out in one stable order and written into ~128 MB
parts, with a row group per podcast so a client can fetch one podcast with a
ranged read rather than pulling a whole file.

``metadata/shard_map.parquet`` records, for every podcast, which part holds it
and which row group to ask for, in each tree.

The machine has far more memory than the corpus, so each tree is assembled in
memory and sorted once. That is much simpler than gathering podcasts scattered
across 4,096 hash-sharded intermediates, and the peak footprint is tens of GB.
"""

import glob
import os
import sys
from collections import defaultdict

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from buildconfig import load as _load_build_config

_CFG = _load_build_config()

BUILD = _CFG.build
V1 = _CFG.source
OUT = _CFG.release

TARGET_PART_BYTES = 128 << 20
TARGET_ROWGROUP_BYTES = 1 << 20

# Acoustics live apart from turn text so they are never fetched unless asked
# for. They are the bulkiest per-turn thing shipped and are rarely used.
ACOUSTIC_COLS = [
    f"{k}{s}"
    for k in ["mfcc1_sma3", "mfcc2_sma3", "mfcc3_sma3", "mfcc4_sma3",
              "F0semitoneFrom27.5Hz_sma3nz", "F1frequency_sma3nz"]
    for s in ("Mean", "StDev")
]
TURN_COLS = ["episode_id", "podcast_id", "speaker", "turn_text",
             "start_time", "end_time", "duration", "turn_count", "word_count",
             "inferred_speaker_name", "inferred_speaker_role"]


def podcast_order():
    """
    Every podcast_id, ordered by (primary_category, podcast_id).

    Sorting by category rather than hashing means a category-scoped query lands
    on a contiguous run of row groups instead of touching every part.
    """
    cat = pq.ParquetFile(f"{V1}/metadata/podcast_catalog.parquet").read(
        columns=["podcast_id", "primary_category"]
    )
    pairs = sorted(
        zip(cat.column("primary_category").to_pylist(),
            cat.column("podcast_id").to_pylist()),
        key=lambda r: ((r[0] or "").lower(), r[1]),
    )
    return [p for _, p in pairs]


def load_turns():
    """All rebuilt turns, as one table."""
    files = sorted(glob.glob(f"{BUILD}/turns_raw/shard=*.parquet"))
    if not files:
        raise SystemExit("no turns_raw shards; run build_turns.py first")
    print(f"reading {len(files):,} turn shards...", flush=True)
    return pa.concat_tables([pq.read_table(f) for f in files])


NO_NAME = "NO_INFERRED_SPEAKER"
NO_ROLE = "NO_INFERRED_ROLE"
_SENTINELS = {NO_NAME, NO_ROLE, ""}
_SENTINEL_ARR = pa.array(sorted(_SENTINELS), pa.string())


def widen_strings(tbl):
    """
    Promote string columns to 64-bit offsets.

    ``Table.take`` combines the source column's chunks before selecting, and a
    32-bit ``string`` array cannot address more than 2 GB of characters. Turn
    text across 185M turns is far past that, and the ``speaker`` lists are close
    behind, so selecting a single podcast out of the table overflows unless the
    offsets are widened first. Parquet has no separate large-string type, so
    this changes nothing about the files that get written.
    """
    cols = []
    fields = []
    for field in tbl.schema:
        col = tbl.column(field.name)
        t = field.type
        if pa.types.is_string(t):
            t = pa.large_string()
            col = col.cast(t)
        elif pa.types.is_list(t) and pa.types.is_string(t.value_type):
            t = pa.list_(pa.large_string())
            col = col.cast(t)
        cols.append(col)
        fields.append(pa.field(field.name, t))
    return pa.Table.from_arrays(cols, schema=pa.schema(fields))


def align_to(tbl, schema):
    """
    Reshape *tbl* to *schema*: cast what differs, null what is absent, drop the rest.

    The v1.0 turns and the rebuilt turns do not agree on shape. v1.0 stores
    ``turn_count`` as int64 and carries an ``mp3_url`` the rebuild does not,
    while the rebuild adds word counts and the standard-deviation acoustics.
    Concatenating without reconciling all of that fails on the first mismatch.
    """
    cols = []
    for field in schema:
        if field.name in tbl.column_names:
            col = tbl.column(field.name)
            if col.type != field.type:
                col = col.cast(field.type)
        else:
            col = pa.nulls(tbl.num_rows, field.type)
        cols.append(col)
    return pa.Table.from_arrays(cols, schema=schema)


def carryover_targets(rebuilt_eids):
    """Episodes with v1.0 turns that the rebuild could not reach, by podcast."""
    cat = pq.ParquetFile(f"{V1}/metadata/episode_catalog.parquet").read(
        columns=["episode_id", "podcast_id", "total_sp_labels"])
    out = defaultdict(set)
    for eid, pid, spl in zip(cat.column("episode_id").to_pylist(),
                             cat.column("podcast_id").to_pylist(),
                             cat.column("total_sp_labels").to_pylist()):
        if (spl or 0) > 0 and eid not in rebuilt_eids:
            out[pid].add(eid)
    return out


def scan_v1_turns(rebuilt_eids):
    """
    One pass over the v1.0 turns, collecting two things.

    First, the turns for episodes the corrected merge cannot reach, because
    their prosody or RTTM inputs no longer exist. Dropping those would lose turn
    data v1.0 users rely on, so they are carried across as-is and flagged.

    Second, a map from (episode, speaker label) to the inferred name and role.
    Rebuilding turns from prosody and diarization alone reproduces the
    ``SPEAKER_00`` labels but not the names, which came from a classifier that
    is not being re-run. Without this map the rebuild would silently drop names
    from every episode that had them. The mapping is nearly always unambiguous;
    where a label carries more than one name the first is kept.
    """
    # The scan costs ~25 minutes across 152k partitions, so its two results are
    # cached. Rerunning the repack after a downstream failure then starts from
    # here instead of paying for the scan again.
    cc = os.path.join(BUILD, "cache_carryover.parquet")
    cn = os.path.join(BUILD, "cache_names.parquet")
    if os.path.exists(cc) and os.path.exists(cn):
        print("reusing cached v1.0 scan", flush=True)
        carry_tbl = pq.read_table(cc)
        nt = pq.read_table(cn)
        names = {(e, s): (n, r) for e, s, n, r in zip(
            nt.column("episode_id").to_pylist(), nt.column("speaker").to_pylist(),
            nt.column("name").to_pylist(), nt.column("role").to_pylist())}
        print(f"  carry-over turns {carry_tbl.num_rows:,}, "
              f"name mappings {len(names):,}", flush=True)
        return carry_tbl, names

    # Which episodes need carrying over, grouped by podcast. Testing membership
    # against the whole rebuilt set on every partition is what makes this slow:
    # Arrow rehashes the value set per call, and at 646k values that costs 65x
    # more than reading the file. Nearly every partition needs no test at all,
    # and the few that do only test a handful of ids.
    carry_by_pod = carryover_targets(rebuilt_eids)
    print(f"{sum(len(v) for v in carry_by_pod.values()):,} episodes to carry over, "
          f"in {len(carry_by_pod):,} podcasts", flush=True)

    carried = []
    names = {}
    parts = sorted(glob.glob(f"{V1}/turns/podcast_id=*/text.parquet"))
    print(f"scanning {len(parts):,} v1.0 turn partitions...", flush=True)

    for i, p in enumerate(parts):
        tbl = pq.ParquetFile(p).read()
        if tbl.num_rows == 0:
            continue
        pid = p.split("podcast_id=")[1].split("/")[0]

        # Filter in Arrow before touching Python. There are ~100M v1.0 turns and
        # converting them all to dicts would cost hours and tens of GB; only the
        # few percent that carry a name, plus the episodes needing carry-over,
        # ever become Python objects.
        nm_col = tbl.column("inferred_speaker_name")
        has_name = pc.and_(pc.is_valid(nm_col),
                           pc.invert(pc.is_in(nm_col, value_set=_SENTINEL_ARR)))
        named = tbl.filter(has_name)
        if named.num_rows:
            for eid, spk, nm, ro in zip(
                    named.column("episode_id").to_pylist(),
                    named.column("speaker").to_pylist(),
                    named.column("inferred_speaker_name").to_pylist(),
                    named.column("inferred_speaker_role").to_pylist()):
                for s in (spk or []):
                    names.setdefault((eid, s), (nm.strip(), (ro or "").strip()))

        want = carry_by_pod.get(pid)
        if want:
            sub = tbl.filter(pc.is_in(tbl.column("episode_id"),
                                      value_set=pa.array(sorted(want), pa.string())))
            if sub.num_rows:
                carried.append(sub)

        if (i + 1) % 20000 == 0:
            n = sum(t.num_rows for t in carried)
            print(f"  {i+1:,}/{len(parts):,}  carried {n:,}  names {len(names):,}",
                  flush=True)

    carry_tbl = (pa.concat_tables(carried, promote_options="default")
                 if carried else None)
    n = carry_tbl.num_rows if carry_tbl is not None else 0
    print(f"  carry-over turns {n:,}, name mappings {len(names):,}", flush=True)

    if carry_tbl is not None:
        pq.write_table(carry_tbl, cc, compression="zstd")
    keys = sorted(names)
    pq.write_table(pa.table({
        "episode_id": [k[0] for k in keys],
        "speaker": [k[1] for k in keys],
        "name": [names[k][0] for k in keys],
        "role": [names[k][1] for k in keys],
    }), cn, compression="zstd")
    print(f"  cached scan to {os.path.basename(cc)} / {os.path.basename(cn)}",
          flush=True)
    return carry_tbl, names


def restore_names(turns, names):
    """
    Attach v1.0 inferred names to rebuilt turns via their speaker labels.

    A turn takes the name of the first of its speakers that has one, which
    matches v1.0's one-name-per-turn shape for the overlapping case.
    """
    eids = turns.column("episode_id").to_pylist()
    spk = turns.column("speaker").to_pylist()
    out_n, out_r = [], []
    for eid, ss in zip(eids, spk):
        nm, ro = NO_NAME, NO_ROLE
        for s in (ss or []):
            hit = names.get((eid, s))
            if hit:
                nm, ro = hit
                break
        out_n.append(nm)
        out_r.append(ro)
    return (turns
            .append_column("inferred_speaker_name", pa.array(out_n, pa.string()))
            .append_column("inferred_speaker_role", pa.array(out_r, pa.string())))


def group_rows(tbl, order):
    """Row index ranges per podcast, in the given podcast order."""
    pids = tbl.column("podcast_id").to_pylist()
    idx = defaultdict(list)
    for i, p in enumerate(pids):
        idx[p].append(i)
    return [(p, idx[p]) for p in order if p in idx]


def write_tree(tbl, groups, subdir, name):
    """
    Write one tree as ~128 MB parts, one row group per podcast.

    Returns shard-map rows: podcast -> (part, row group, rows).
    """
    dest = os.path.join(OUT, subdir)
    os.makedirs(dest, exist_ok=True)
    # Wide offsets are needed to select out of the whole table, but each podcast
    # slice is small, so it is narrowed again before writing. The files then
    # carry ordinary string columns, as v1.0's did.
    narrow_schema = tbl.schema
    tbl = widen_strings(tbl)
    nbytes = tbl.nbytes
    nrows = max(tbl.num_rows, 1)
    per_row = max(nbytes / nrows, 1)

    smap = []
    part_no = 0
    writer = None
    part_bytes = 0
    rg = 0
    try:
        for pid, rows in groups:
            est = len(rows) * per_row
            if writer is None or part_bytes + est > TARGET_PART_BYTES:
                if writer is not None:
                    writer.close()
                    part_no += 1
                path = os.path.join(dest, f"part-{part_no:04d}.parquet")
                writer = pq.ParquetWriter(path, narrow_schema, compression="zstd")
                part_bytes = 0
                rg = 0
            sub = align_to(tbl.take(rows), narrow_schema)
            # One write_table call is one row group, which is what makes a
            # per-podcast ranged read possible on the client.
            writer.write_table(sub)
            smap.append({
                "podcast_id": pid,
                "tree": name,
                "part": f"part-{part_no:04d}.parquet",
                "row_group": rg,
                "num_rows": len(rows),
            })
            rg += 1
            part_bytes += est
    finally:
        if writer is not None:
            writer.close()
    print(f"  {subdir}: {part_no+1} parts, {len(smap):,} podcasts", flush=True)
    return smap


def main():
    order = podcast_order()
    print(f"{len(order):,} podcasts in category order", flush=True)

    turns = load_turns()
    print(f"rebuilt turns: {turns.num_rows:,} rows", flush=True)
    rebuilt = set(turns.column("episode_id").to_pylist())

    carry, names = scan_v1_turns(rebuilt)
    print(f"carried-over turns: {carry.num_rows if carry is not None else 0:,} rows",
          flush=True)

    # The rebuild reproduces speaker labels but not the names attached to them,
    # so they are carried across from v1.0 before anything is written.
    turns = restore_names(turns, names)
    named = sum(1 for n in turns.column("inferred_speaker_name").to_pylist()
                if n != NO_NAME)
    print(f"rebuilt turns with a restored name: {named:,} "
          f"({100*named/max(turns.num_rows,1):.1f}%)", flush=True)

    # Flag provenance so the difference is queryable rather than invisible.
    turns = turns.append_column(
        "speakers_recomputed",
        pa.array([True] * turns.num_rows, pa.bool_()),
    )
    if carry is not None and carry.num_rows:
        ctbl = carry.append_column(
            "speakers_recomputed", pa.array([False] * carry.num_rows, pa.bool_()))
        ctbl = align_to(ctbl, turns.schema)
        turns = pa.concat_tables([turns, ctbl])
    print(f"total turns: {turns.num_rows:,}", flush=True)

    tgroups = group_rows(turns, order)
    smap = []
    text = turns.select([c for c in TURN_COLS + ["speakers_recomputed"]
                         if c in turns.column_names])
    smap += write_tree(text, tgroups, "turns/text", "turns_text")
    acou = turns.select(["episode_id", "podcast_id", "turn_count"] +
                        [c for c in ACOUSTIC_COLS if c in turns.column_names])
    smap += write_tree(acou, tgroups, "acoustics", "acoustics")

    del turns, text, acou

    print("reading v1.0 episodes...", flush=True)
    eps = pa.concat_tables(
        [pq.ParquetFile(p).read()
         for p in sorted(glob.glob(f"{V1}/episodes/podcast_id=*/data.parquet"))],
        promote_options="default",
    )
    print(f"episodes: {eps.num_rows:,} rows", flush=True)
    smap += write_tree(eps, group_rows(eps, order), "episodes", "episodes")

    os.makedirs(f"{OUT}/metadata", exist_ok=True)
    pq.write_table(pa.Table.from_pylist(smap),
                   f"{OUT}/metadata/shard_map.parquet", compression="zstd")
    print(f"wrote metadata/shard_map.parquet ({len(smap):,} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
