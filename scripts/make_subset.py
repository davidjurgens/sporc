#!/usr/bin/env python3
"""
Build a small, self-contained SPoRC subset.

Copying partitions alone is not enough: the catalogs would still describe all
228,099 podcasts, so a learner would see search results and statistics for data
the subset does not contain. This filters the catalogs to match, producing a
layout that behaves like a miniature SPoRC -- every count, search and statistic
is true of what is actually there.

Subsets are diarized-only by default, since two thirds of SPoRC episodes have no
speaker turns and a learner hitting those would see empty results that look like
bugs.

The source layout need not be a full corpus copy. A directory populated by lazy
fetching (SPORCDataset().prefetch(...), which writes into the HuggingFace
snapshot dir) is a valid --data-dir: podcasts whose partitions are absent are
reported and dropped rather than silently left in the catalogs.

Usage:
    python scripts/make_subset.py --data-dir /path/to/sporc_parquet \\
        --out subsets/subset_01 --episodes 1000 --seed 1

    # Ten disjoint teaching subsets:
    for i in $(seq 1 10); do
      python scripts/make_subset.py --data-dir DATA --out subsets/subset_$i \\
          --episodes 1000 --seed $i --exclude-used subsets/used.txt
    done

    # From an explicit, hand-picked set of podcasts:
    python scripts/make_subset.py --data-dir DATA --out subsets/tutorial \\
        --podcast-ids tutorial_ids.txt
"""

import argparse
import json
import logging
import os
import random
import shutil
import sys
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

DEFAULT_DATA_DIR = os.environ.get("SPORC_PARQUET_DIR", "sporc_parquet")

# Subset parts are written to the same size target as the published dataset, so
# a subset looks like a small dataset rather than a different format.
TARGET_PART_BYTES = 128 << 20


def _write_subset_shard_map(rows, path):
    """
    Write the subset's shard map, one row group per tree.

    Matches how the published map is laid out, so the client finds a tree's
    rows from the Parquet statistics rather than scanning the others.
    """
    if not rows:
        return
    table = pa.table({
        "podcast_id": [r["podcast_id"] for r in rows],
        "tree": [r["tree"] for r in rows],
        "part": [r["part"] for r in rows],
        "row_group": [r["row_group"] for r in rows],
        "num_rows": [r["num_rows"] for r in rows],
    })
    writer = pq.ParquetWriter(path, table.schema, compression="zstd")
    try:
        for tree in sorted(set(table.column("tree").to_pylist())):
            writer.write_table(
                table.filter(pc.equal(table.column("tree"), tree)))
    finally:
        writer.close()


def _filter_table(path, column, keep, out_path):
    """Write the rows of *path* whose *column* is in *keep*."""
    if not os.path.exists(path):
        return 0
    table = pq.ParquetFile(path).read()
    if column not in table.column_names:
        return 0
    mask = pc.is_in(table.column(column), value_set=pa.array(sorted(keep)))
    filtered = table.filter(mask)
    pq.write_table(filtered, out_path, compression="zstd")
    return filtered.num_rows


def select_podcasts(data_dir, n_episodes, seed, diarized_only, exclude):
    """Pick whole podcasts until the episode target is met."""
    cat = pq.ParquetFile(f"{data_dir}/metadata/episode_catalog.parquet").read(
        columns=["episode_id", "podcast_id", "num_main_speakers"])
    pids = cat.column("podcast_id").to_pylist()
    nms = cat.column("num_main_speakers").to_pylist()

    per_pod = {}
    for pid, n in zip(pids, nms):
        if diarized_only and not n:
            continue
        per_pod[pid] = per_pod.get(pid, 0) + 1

    candidates = sorted(p for p in per_pod if p not in exclude)
    rng = random.Random(seed)
    rng.shuffle(candidates)

    chosen, total = [], 0
    for pid in candidates:
        if total >= n_episodes:
            break
        # Whole podcasts only: a partial podcast would make episode_count in the
        # catalog disagree with the data.
        chosen.append(pid)
        total += per_pod[pid]
    return chosen, total


def resolve_present(data_dir, podcast_ids):
    """
    Narrow *podcast_ids* to those whose episode partition is actually in
    *data_dir*.

    The catalogs describe the whole corpus, but a layout built by lazy fetching
    holds only the podcasts that were touched. Filtering the catalogs to ids
    whose partitions are absent would produce exactly the catalog/data
    disagreement this script exists to prevent, so the ids are checked against
    the partitions on disk first.
    """
    from sporc.shard_map import ShardMap

    smap_path = os.path.join(data_dir, "metadata", "shard_map.parquet")
    if not os.path.exists(smap_path):
        raise SystemExit(
            f"{smap_path} not found. Dataset version 1.1 and later locate "
            "podcasts through the shard map; a 1.0 layout needs sporc<1.1."
        )
    smap = ShardMap(smap_path)

    present, missing = [], []
    for pid in podcast_ids:
        loc = smap.locate("episodes", pid)
        # Present means both that the corpus has the podcast and that the part
        # holding it was actually fetched -- a lazily built layout has the
        # shard map for everything but the parts for only what was touched.
        if loc is not None and os.path.exists(
                os.path.join(data_dir, smap.relpath("episodes", loc.part))):
            present.append(pid)
        else:
            missing.append(pid)
    if missing:
        logger.warning(
            "%d of %d selected podcasts have no episode partition in %s and "
            "were dropped from the subset (first few: %s). Prefetch them first "
            "if they were meant to be included.",
            len(missing), len(podcast_ids), data_dir, missing[:5],
        )
    return present, missing


def build(data_dir, out, podcast_ids, diarized_only):
    keep = set(podcast_ids)
    meta_out = os.path.join(out, "metadata")
    os.makedirs(meta_out, exist_ok=True)

    counts = {}
    counts["podcasts"] = _filter_table(
        f"{data_dir}/metadata/podcast_catalog.parquet", "podcast_id", keep,
        f"{meta_out}/podcast_catalog.parquet")

    # Episode catalog: restrict to the chosen podcasts, and to diarized episodes
    # if asked, so the catalog never advertises turns that are not there.
    ec = pq.ParquetFile(f"{data_dir}/metadata/episode_catalog.parquet").read()
    mask = pc.is_in(ec.column("podcast_id"), value_set=pa.array(sorted(keep)))
    if diarized_only:
        mask = pc.and_(mask, pc.greater(ec.column("num_main_speakers"), 0))
    ec = ec.filter(mask)
    pq.write_table(ec, f"{meta_out}/episode_catalog.parquet", compression="zstd")
    counts["episodes"] = ec.num_rows
    kept_eids = set(ec.column("episode_id").to_pylist())

    for name, col in [("category_index", "podcast_id"),
                      ("hostname_index", "podcast_id"),
                      ("speaker_name_index", "podcast_id")]:
        counts[name] = _filter_table(
            f"{data_dir}/metadata/{name}.parquet", col, keep,
            f"{meta_out}/{name}.parquet")

    counts["episode_metrics"] = _filter_table(
        f"{data_dir}/metadata/episode_metrics.parquet", "episode_id", kept_eids,
        f"{meta_out}/episode_metrics.parquet")

    # Partitions. The episode partitions are filtered to the same episodes as
    # the catalog: copying them whole would leave podcast.episodes yielding
    # undiarized episodes that the catalog says are not here, which is the
    # confusion this script exists to avoid.
    #
    # A missing *turns* tree is normal (two thirds of episodes are undiarized),
    # but a missing *episodes* partition means the catalog would advertise a
    # podcast whose data is absent. resolve_present() has already dropped those,
    # so reaching one here is a bug rather than a data condition.
    # The subset is written in the same shape as the full dataset: parts holding
    # many podcasts, one row group each, addressed through a shard map. Only the
    # selected podcasts' row groups are copied, and the map is rewritten to
    # match, so the subset is a smaller dataset rather than a differently
    # shaped one and the same client code reads both.
    from sporc.shard_map import ShardMap, TREE_DIRS

    smap = ShardMap(os.path.join(data_dir, "metadata", "shard_map.parquet"))
    eid_filter = pa.array(sorted(kept_eids)) if diarized_only else None

    n_files = 0
    shard_rows = []
    for tree, subdir in TREE_DIRS.items():
        # Group the wanted podcasts by the part they live in, so each source
        # file is opened once instead of once per podcast.
        by_part = {}
        for pid in podcast_ids:
            loc = smap.locate(tree, pid)
            if loc is None:
                if tree == "episodes":
                    raise RuntimeError(
                        f"podcast_id={pid} is absent from the {tree} shard map "
                        "after selection; refusing to write a subset whose "
                        "catalog does not match its data."
                    )
                continue
            by_part.setdefault(loc.part, []).append((pid, loc.row_group))

        if not by_part:
            continue
        dst_dir = os.path.join(out, subdir)
        os.makedirs(dst_dir, exist_ok=True)

        seq = 0
        writer = None
        rg = 0
        dst_name = None
        try:
            for part in sorted(by_part):
                src = os.path.join(data_dir, subdir, part)
                if not os.path.exists(src):
                    if tree == "episodes":
                        raise RuntimeError(
                            f"{src} is missing but the shard map names it; "
                            "prefetch the podcasts before building a subset."
                        )
                    continue
                pf = pq.ParquetFile(src)
                for pid, row_group in sorted(by_part[part], key=lambda x: x[1]):
                    table = pf.read_row_group(row_group)
                    if eid_filter is not None and "episode_id" in table.column_names:
                        table = table.filter(pc.is_in(table.column("episode_id"),
                                                      value_set=eid_filter))
                        if table.num_rows == 0:
                            continue
                    if writer is None:
                        dst_name = f"part-{seq:04d}.parquet"
                        writer = pq.ParquetWriter(
                            os.path.join(dst_dir, dst_name), table.schema,
                            compression="zstd")
                        rg = 0
                        n_files += 1
                    writer.write_table(table)
                    shard_rows.append({
                        "podcast_id": pid, "tree": tree, "part": dst_name,
                        "row_group": rg, "num_rows": table.num_rows,
                    })
                    rg += 1
                    if os.path.getsize(os.path.join(dst_dir, dst_name)) >= TARGET_PART_BYTES:
                        writer.close()
                        writer = None
                        seq += 1
        finally:
            if writer is not None:
                writer.close()

    _write_subset_shard_map(shard_rows, f"{meta_out}/shard_map.parquet")
    counts["shard_map"] = len(shard_rows)
    counts["partition_files"] = n_files

    manifest = {
        "version": "1.1",
        "schema_version": 2,
        "subset": True,
        "creation_date": datetime.now().isoformat(),
        "source": "blitt/SPoRC (HuggingFace), subset via scripts/make_subset.py",
        "diarized_only": diarized_only,
        "record_counts": {
            "podcasts": counts["podcasts"],
            "episodes": counts["episodes"],
            "category_index_rows": counts.get("category_index", 0),
            "hostname_index_rows": counts.get("hostname_index", 0),
        },
        "compression": "zstd",
        "note": (
            "Self-contained SPoRC subset. Catalogs are filtered to match the "
            "partitions present, so counts and searches are true of this subset. "
            "No full-text index: search_turns()/concordance() scan the "
            "partitions, which is fast at this size."
        ),
    }
    with open(os.path.join(out, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    return counts


def main():
    p = argparse.ArgumentParser(description="Build a self-contained SPoRC subset")
    p.add_argument("--data-dir", default=DEFAULT_DATA_DIR,
                   help="Full SPoRC parquet layout to draw from")
    p.add_argument("--out", required=True, help="Output directory for the subset")
    p.add_argument("--episodes", type=int, default=1000,
                   help="Approximate number of episodes (whole podcasts are taken)")
    p.add_argument("--seed", type=int, default=0, help="Selection seed")
    p.add_argument("--include-undiarized", action="store_true",
                   help="Also include episodes with no speaker turns (default: "
                        "diarized only, so turn-based lessons always work)")
    p.add_argument("--exclude-used", default=None,
                   help="File of podcast_ids to avoid, appended to after "
                        "selection, so repeated runs build disjoint subsets")
    p.add_argument("--podcast-ids", default=None,
                   help="File of podcast_ids (one per line) to build from, "
                        "instead of sampling. Use when the subset must contain "
                        "particular podcasts -- random sampling almost never "
                        "yields, say, a guest appearing on two different shows. "
                        "Overrides --episodes/--seed/--exclude-used.")
    args = p.parse_args()

    if not os.path.isdir(args.data_dir):
        logger.error("Data directory not found: %s", args.data_dir)
        sys.exit(1)

    diarized_only = not args.include_undiarized

    if args.podcast_ids:
        if not os.path.exists(args.podcast_ids):
            logger.error("Podcast id file not found: %s", args.podcast_ids)
            sys.exit(1)
        with open(args.podcast_ids) as f:
            pods = [ln.strip() for ln in f
                    if ln.strip() and not ln.startswith("#")]
        pods = list(dict.fromkeys(pods))
        logger.info("Building from %d explicit podcast ids", len(pods))
    else:
        exclude = set()
        if args.exclude_used and os.path.exists(args.exclude_used):
            with open(args.exclude_used) as f:
                exclude = {ln.strip() for ln in f if ln.strip()}
            logger.info("Excluding %d podcasts already used", len(exclude))

        pods, total = select_podcasts(args.data_dir, args.episodes, args.seed,
                                      diarized_only, exclude)
        logger.info("Selected %d podcasts / ~%d episodes", len(pods), total)

    pods, _missing = resolve_present(args.data_dir, pods)
    if not pods:
        logger.error("None of the selected podcasts have partitions in %s. "
                     "Prefetch them before building the subset.", args.data_dir)
        sys.exit(1)

    counts = build(args.data_dir, args.out, pods, diarized_only)
    size = sum(os.path.getsize(os.path.join(r, f))
               for r, _, fs in os.walk(args.out) for f in fs)
    logger.info("Wrote %s: %d podcasts, %d episodes, %d files, %.1f MB",
                args.out, counts["podcasts"], counts["episodes"],
                counts["partition_files"], size / 1e6)

    if args.exclude_used:
        with open(args.exclude_used, "a") as f:
            for pid in pods:
                f.write(pid + "\n")
        logger.info("Appended %d podcast ids to %s", len(pods), args.exclude_used)


if __name__ == "__main__":
    main()
