"""
The repack, split into stages that can be run, rerun, and parallelised.

``repack.py`` assembles all 185M turns in one process and writes the whole tree
in a single pass. That works, but it holds ~100 GB for an hour, checkpoints
nothing, and a failure anywhere costs the entire run -- which is how the first
two attempts ended.

Nothing about the output requires that. Podcasts are written in one fixed order,
each into its own row group, so any contiguous run of that order can be built
independently of the rest. This splits the order into bands and gives each band
its own stage:

    band      route every rebuilt and carried-over turn to its band   (one pass)
    write N   build band N's parts and its slice of the shard map     (per band)
    episodes  the same treatment for the episode tree
    map       concatenate the per-band shard-map slices

Each stage skips work whose output already exists, so a rerun after a failure
costs only the bands that did not finish. ``write`` bands are independent and
run in parallel.

The only concession to banding is that a part cannot span a band boundary, so
the tree carries up to NBANDS-1 undersized parts. At 64 bands against ~450
parts that is noise, and parts are named per band -- ``part-007-002.parquet`` --
so a band can be rebuilt without renumbering anything else.
"""

import glob
import os
import subprocess
import sys
import time
from collections import defaultdict
from multiprocessing import Pool

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import repack as R

from buildconfig import load as _load_build_config

_CFG = _load_build_config()

BUILD = _CFG.build
V1 = _CFG.source
OUT = _CFG.release
BANDS = os.path.join(BUILD, "bands")

NBANDS = 64
TARGET_PART_BYTES = 128 << 20
FLUSH_ROWS = 500_000


# --------------------------------------------------------------------------
# band assignment


def band_plan():
    """
    Split the podcast order into NBANDS contiguous, roughly equal-weight ranges.

    Contiguous is the point: it keeps a category on a short run of parts, which
    is why the order is category-sorted in the first place. Weighting by episode
    count rather than podcast count matters because podcast sizes span four
    orders of magnitude -- an unweighted split leaves one band doing a tenth of
    the corpus while its neighbours idle.
    """
    order = R.podcast_order()
    cat = pq.ParquetFile(f"{V1}/metadata/episode_catalog.parquet").read(
        columns=["podcast_id"])
    weight = defaultdict(int)
    for p in cat.column("podcast_id").to_pylist():
        weight[p] += 1

    total = sum(weight.get(p, 0) for p in order)
    per = total / NBANDS
    pid_band, band_pods = {}, defaultdict(list)
    b, acc = 0, 0
    for p in order:
        if b < NBANDS - 1 and acc >= per * (b + 1):
            b += 1
        pid_band[p] = b
        band_pods[b].append(p)
        acc += weight.get(p, 0)
    return pid_band, band_pods


# --------------------------------------------------------------------------
# stage: band


class BandWriters:
    """Per-band Parquet writers, buffering rows so row groups stay reasonable."""

    def __init__(self, prefix, schema):
        self.prefix = prefix
        self.schema = schema
        self.buf = defaultdict(list)
        self.rows = defaultdict(int)
        self.writers = {}

    def add(self, band, tbl):
        self.buf[band].append(tbl)
        self.rows[band] += tbl.num_rows
        if self.rows[band] >= FLUSH_ROWS:
            self.flush(band)

    def flush(self, band):
        if not self.buf[band]:
            return
        w = self.writers.get(band)
        if w is None:
            path = os.path.join(BANDS, f"{self.prefix}-{band:03d}.parquet")
            w = pq.ParquetWriter(path + ".tmp", self.schema, compression="zstd")
            self.writers[band] = w
        w.write_table(pa.concat_tables(self.buf[band], promote_options="default"))
        self.buf[band] = []
        self.rows[band] = 0

    def close(self):
        for band in list(self.buf):
            self.flush(band)
        for band, w in self.writers.items():
            w.close()
            path = os.path.join(BANDS, f"{self.prefix}-{band:03d}.parquet")
            os.replace(path + ".tmp", path)


def split_table(tbl, pid_band):
    """Row indices of `tbl` grouped by band."""
    out = defaultdict(list)
    for i, p in enumerate(tbl.column("podcast_id").to_pylist()):
        b = pid_band.get(p)
        if b is not None:
            out[b].append(i)
    return out


def stage_band(pid_band):
    os.makedirs(BANDS, exist_ok=True)
    done = os.path.join(BANDS, "_band.done")
    if os.path.exists(done):
        print("band stage already done", flush=True)
        return

    shards = sorted(glob.glob(f"{BUILD}/turns_raw/shard=*.parquet"))
    if not shards:
        raise SystemExit("no turns_raw shards; run build_turns.py first")
    schema = pq.ParquetFile(shards[0]).schema_arrow
    print(f"routing {len(shards):,} shards into {NBANDS} bands...", flush=True)

    t0 = time.time()
    w = BandWriters("turns", schema)
    for i, path in enumerate(shards):
        tbl = pq.ParquetFile(path).read()
        if tbl.num_rows:
            for band, rows in split_table(tbl, pid_band).items():
                w.add(band, tbl.take(rows))
        if (i + 1) % 500 == 0:
            print(f"  {i+1:,}/{len(shards):,}  {time.time()-t0:.0f}s", flush=True)
    w.close()
    print(f"  rebuilt turns banded in {(time.time()-t0)/60:.1f}m", flush=True)

    cc = os.path.join(BUILD, "cache_carryover.parquet")
    if os.path.exists(cc):
        pf = pq.ParquetFile(cc)
        w = BandWriters("carry", pf.schema_arrow)
        for batch in pf.iter_batches(batch_size=500_000):
            tbl = pa.Table.from_batches([batch])
            for band, rows in split_table(tbl, pid_band).items():
                w.add(band, tbl.take(rows))
        w.close()
        print("  carry-over banded", flush=True)

    open(done, "w").close()


# --------------------------------------------------------------------------
# stage: write


def load_names():
    nt = pq.read_table(os.path.join(BUILD, "cache_names.parquet"))
    return {(e, s): (n, r) for e, s, n, r in zip(
        nt.column("episode_id").to_pylist(), nt.column("speaker").to_pylist(),
        nt.column("name").to_pylist(), nt.column("role").to_pylist())}


def write_band_tree(tbl, pods, subdir, tree, band, narrow=None):
    """
    Write one band's slice of one tree: ~128 MB parts, a row group per podcast.

    Returns shard-map rows. Identical in effect to ``repack.write_tree`` except
    that parts are numbered within the band.
    """
    dest = os.path.join(OUT, subdir)
    os.makedirs(dest, exist_ok=True)

    idx = defaultdict(list)
    for i, p in enumerate(tbl.column("podcast_id").to_pylist()):
        idx[p].append(i)

    # Callers that had to widen before slicing pass the pre-widening schema, so
    # the files still carry ordinary string columns as v1.0's did.
    narrow = narrow or tbl.schema
    # Combine chunks before selecting. A table concatenated from N sources keeps
    # N chunks per column, and ``take`` pays a cost per chunk on every call, so
    # selecting each podcast in turn goes quadratic in the number of sources.
    # Measured on one band: 300 takes cost 62.6s across 10 chunks and 0.3s once
    # combined. This is what made the unstaged repack write 1 MB in 100 minutes
    # against a table of 4,097 chunks.
    wide = R.widen_strings(tbl).combine_chunks()

    smap, seq, writer, rg = [], 0, None, 0
    path = None
    try:
        for pid in pods:
            rows = idx.get(pid)
            if not rows:
                continue
            if writer is None:
                name = f"part-{band:03d}-{seq:03d}.parquet"
                path = os.path.join(dest, name)
                writer = pq.ParquetWriter(path, narrow, compression="zstd")
                rg = 0
            sub = R.align_to(wide.take(rows), narrow)
            # One write_table call is one row group, which is what lets a client
            # fetch a single podcast with a ranged read.
            writer.write_table(sub)
            smap.append({"podcast_id": pid, "tree": tree,
                         "part": f"part-{band:03d}-{seq:03d}.parquet",
                         "row_group": rg, "num_rows": len(rows)})
            rg += 1
            # Roll on the size of the file actually on disk. Estimating from the
            # in-memory footprint sizes the parts before compression, which came
            # out about 3.6x small and would have tripled the file count -- the
            # one number this whole layout exists to keep down. Parquet flushes
            # each row group as it is written, so the file size is current here.
            if os.path.getsize(path) >= TARGET_PART_BYTES:
                writer.close()
                writer = None
                seq += 1
    finally:
        if writer is not None:
            writer.close()
    return smap


def stage_write(band):
    """Build one band's turn text and acoustics trees."""
    out = os.path.join(BANDS, f"smap-turns-{band:03d}.parquet")
    if os.path.exists(out):
        return (band, 0, 0, True)

    _, band_pods = band_plan()
    pods = band_pods[band]

    src = os.path.join(BANDS, f"turns-{band:03d}.parquet")
    turns = pq.read_table(src) if os.path.exists(src) else None
    if turns is not None and turns.num_rows:
        names = load_names()
        turns = R.restore_names(turns, names)
        turns = turns.append_column(
            "speakers_recomputed", pa.array([True] * turns.num_rows, pa.bool_()))
    else:
        turns = None

    carry_path = os.path.join(BANDS, f"carry-{band:03d}.parquet")
    if os.path.exists(carry_path):
        carry = pq.read_table(carry_path)
        if carry.num_rows:
            carry = carry.append_column(
                "speakers_recomputed",
                pa.array([False] * carry.num_rows, pa.bool_()))
            if turns is None:
                # No rebuilt turns in this band, so there is no schema to align
                # to. Build the target schema from the shard layout instead.
                shards = sorted(glob.glob(f"{BUILD}/turns_raw/shard=*.parquet"))
                base = pq.ParquetFile(shards[0]).schema_arrow
                fields = list(base) + [
                    pa.field("inferred_speaker_name", pa.string()),
                    pa.field("inferred_speaker_role", pa.string()),
                    pa.field("speakers_recomputed", pa.bool_())]
                turns = R.align_to(carry, pa.schema(fields))
            else:
                turns = pa.concat_tables(
                    [turns, R.align_to(carry, turns.schema)])

    if turns is None or turns.num_rows == 0:
        pq.write_table(pa.table({"podcast_id": pa.array([], pa.string()),
                                 "tree": pa.array([], pa.string()),
                                 "part": pa.array([], pa.string()),
                                 "row_group": pa.array([], pa.int64()),
                                 "num_rows": pa.array([], pa.int64())}), out)
        return (band, 0, 0, False)

    text = turns.select([c for c in R.TURN_COLS + ["speakers_recomputed"]
                         if c in turns.column_names])
    smap = write_band_tree(text, pods, "turns/text", "turns_text", band)
    acou = turns.select(["episode_id", "podcast_id", "turn_count"] +
                        [c for c in R.ACOUSTIC_COLS if c in turns.column_names])
    smap += write_band_tree(acou, pods, "acoustics", "acoustics", band)

    pq.write_table(pa.Table.from_pylist(smap), out + ".tmp")
    os.replace(out + ".tmp", out)
    return (band, turns.num_rows, len(smap), False)


# --------------------------------------------------------------------------
# stage: episodes


def stage_episodes_band(pid_band):
    """
    Route v1.0's per-podcast episode files into bands.

    v1.0 gives every podcast its own episode file, so this opens 228,099 of them
    over NFS and is the slowest step in the build. It is done once, and its
    output is 64 band files that every later step reads instead.

    Rows are accumulated per band and never assembled into one table. The
    obvious version -- concatenate everything, then slice out each band -- has
    to widen and combine 60-odd GB of transcripts to make the slicing affordable,
    which is both a large contiguous allocation and the exact operation that
    overran Arrow's 32-bit string offsets on the previous attempt. Grouping at
    read time keeps every concatenation down to one band.
    """
    done = os.path.join(BANDS, "_eps_band.done")
    if os.path.exists(done):
        print("episode bands already built", flush=True)
        return

    os.makedirs(BANDS, exist_ok=True)
    parts = sorted(glob.glob(f"{V1}/episodes/podcast_id=*/data.parquet"))
    print(f"reading {len(parts):,} v1.0 episode partitions...", flush=True)

    t0 = time.time()
    acc = defaultdict(list)
    kept = 0
    for i, p in enumerate(parts):
        tbl = pq.ParquetFile(p).read()
        if not tbl.num_rows:
            continue
        for band, rows in split_table(tbl, pid_band).items():
            acc[band].append(tbl if len(rows) == tbl.num_rows else tbl.take(rows))
            kept += len(rows)
        if (i + 1) % 20000 == 0:
            print(f"  {i+1:,}/{len(parts):,}  {kept:,} episodes  "
                  f"{time.time()-t0:.0f}s", flush=True)
    print(f"  read {kept:,} episodes in {(time.time()-t0)/60:.1f}m", flush=True)

    for band in sorted(acc):
        # pop as we go so each band's inputs are freed once written
        tbl = pa.concat_tables(acc.pop(band), promote_options="default")
        path = os.path.join(BANDS, f"eps-{band:03d}.parquet")
        pq.write_table(tbl, path + ".tmp", compression="zstd",
                       row_group_size=10_000)
        os.replace(path + ".tmp", path)
    print(f"  wrote {len(glob.glob(os.path.join(BANDS, 'eps-*.parquet')))} "
          f"episode bands", flush=True)
    open(done, "w").close()


def stage_episodes_write(band):
    """Write one band's slice of the episode tree. Independent of every other."""
    out = os.path.join(BANDS, f"smap-eps-{band:03d}.parquet")
    if os.path.exists(out):
        return (band, 0, 0, True)

    src = os.path.join(BANDS, f"eps-{band:03d}.parquet")
    tbl = pq.read_table(src) if os.path.exists(src) else None
    if tbl is None or tbl.num_rows == 0:
        pq.write_table(pa.table({"podcast_id": pa.array([], pa.string()),
                                 "tree": pa.array([], pa.string()),
                                 "part": pa.array([], pa.string()),
                                 "row_group": pa.array([], pa.int64()),
                                 "num_rows": pa.array([], pa.int64())}), out)
        return (band, 0, 0, False)

    _, band_pods = band_plan()
    smap = write_band_tree(tbl, band_pods[band], "episodes", "episodes", band)
    pq.write_table(pa.Table.from_pylist(smap), out + ".tmp")
    os.replace(out + ".tmp", out)
    return (band, tbl.num_rows, len(smap), False)


# --------------------------------------------------------------------------
# stage: map


def stage_map():
    slices = sorted(glob.glob(os.path.join(BANDS, "smap-*.parquet")))
    if not slices:
        raise SystemExit("no shard-map slices; run the write stages first")
    tbl = pa.concat_tables([pq.read_table(p) for p in slices],
                           promote_options="default")
    os.makedirs(f"{OUT}/metadata", exist_ok=True)
    write_shard_map(tbl, f"{OUT}/metadata/shard_map.parquet")
    print(f"wrote metadata/shard_map.parquet ({tbl.num_rows:,} rows "
          f"from {len(slices)} slices)", flush=True)


def write_shard_map(tbl, path):
    """
    Write the shard map grouped by tree, one row group per tree.

    A client wants one tree at a time and does not care about the others.
    Filtering a mixed table costs seconds -- most of it comparing the `tree`
    string across every row -- whereas a row group per tree lets the reader find
    its rows from the Parquet statistics and read only those. The map is the
    first thing touched on any lookup, so that difference lands in startup time.
    """
    trees = sorted(set(tbl.column("tree").to_pylist()))
    writer = pq.ParquetWriter(path + ".tmp", tbl.schema, compression="zstd")
    try:
        for tree in trees:
            sub = tbl.filter(pc.equal(tbl.column("tree"), tree))
            sub = sub.sort_by("podcast_id")
            writer.write_table(sub)
    finally:
        writer.close()
    os.replace(path + ".tmp", path)


# --------------------------------------------------------------------------


def guard_monolith():
    """Refuse to run while repack.py owns the output tree."""
    r = subprocess.run(["pgrep", "-f", "python repack.py"],
                       capture_output=True, text=True)
    if r.stdout.strip():
        raise SystemExit(f"repack.py is still running (pid {r.stdout.split()[0]}); "
                         "stop it before writing to the same tree")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"
    workers = int(os.environ.get("SPORC_WORKERS", "16"))

    if cmd == "band":
        pid_band, _ = band_plan()
        stage_band(pid_band)
        return 0

    guard_monolith()

    if cmd == "write":
        bands = ([int(a) for a in sys.argv[2:]] if len(sys.argv) > 2
                 else list(range(NBANDS)))
        t0 = time.time()
        with Pool(min(workers, len(bands))) as pool:
            for band, rows, n, skipped in pool.imap_unordered(stage_write, bands):
                print(f"  band {band:03d}: "
                      + ("already done" if skipped
                         else f"{rows:,} turns, {n:,} row groups"), flush=True)
        print(f"write stage in {(time.time()-t0)/60:.1f}m", flush=True)
        return 0

    if cmd == "episodes":
        pid_band, _ = band_plan()
        stage_episodes_band(pid_band)
        t0 = time.time()
        with Pool(workers) as pool:
            for band, rows, n, skipped in pool.imap_unordered(
                    stage_episodes_write, range(NBANDS)):
                print(f"  band {band:03d}: "
                      + ("already done" if skipped
                         else f"{rows:,} episodes, {n:,} row groups"), flush=True)
        print(f"episode write in {(time.time()-t0)/60:.1f}m", flush=True)
        return 0

    if cmd == "map":
        stage_map()
        return 0

    if cmd == "all":
        pid_band, band_pods = band_plan()
        stage_band(pid_band)
        guard_monolith()
        t0 = time.time()
        with Pool(workers) as pool:
            for band, rows, n, skipped in pool.imap_unordered(
                    stage_write, range(NBANDS)):
                print(f"  band {band:03d}: "
                      + ("already done" if skipped
                         else f"{rows:,} turns, {n:,} row groups"), flush=True)
        print(f"write stage in {(time.time()-t0)/60:.1f}m", flush=True)
        stage_episodes_band(pid_band)
        with Pool(workers) as pool:
            for band, rows, n, skipped in pool.imap_unordered(
                    stage_episodes_write, range(NBANDS)):
                print(f"  band {band:03d}: "
                      + ("already done" if skipped
                         else f"{rows:,} episodes, {n:,} row groups"), flush=True)
        stage_map()
        return 0

    raise SystemExit(f"unknown stage {cmd!r}")


if __name__ == "__main__":
    sys.exit(main())
