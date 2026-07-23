#!/usr/bin/env python3
"""
Build the subset the tutorial notebooks read.

Downloads the metadata catalogs (~195 MB, once), selects the podcasts the
tutorials need, reads their rows, and runs make_subset.py over the result.

This is expensive against the Hub, and was not before dataset 1.1. Podcasts are
packed into shared part files ordered by category, so fetching a set of
podcasts costs whole parts rather than one small file each. The selection here
is deliberately scattered across the corpus -- that is what makes it a teaching
subset -- so it touches most parts: measured on 1.1, 1,000 podcasts drawn at
random span 116 of the 127 turn parts, 11.6 GB, where 1,000 consecutive
podcasts in one category span 2 parts and 0.19 GB. Scanning 6,000 podcasts for
repeat guests reaches essentially every episodes part.

Run it against a local copy (--data-dir) if you have one. The output subset is
about 210 MB, and the notebooks read that, so this only needs running when the
selection itself changes.

Why the selection is hand-picked rather than random
---------------------------------------------------
`make_subset.py --episodes N` samples podcasts at random, which is right for a
teaching subset in general and wrong for these tutorials specifically:

* **Notebook 04** needs guests who appear on two *different* shows. Across 228k
  podcasts, a random sample of a few hundred contains essentially none.
* **Notebook 03** needs the guest-mention artefact to be visible.
* **Notebook 07** needs turns containing THOUGHT/LOT words whose audio is live.

So the subset is a union of a random diarized sample and podcasts selected to
satisfy those needs. The chosen ids are written to subsets/tutorial_ids.txt so
the result is reproducible.

Usage:
    python scripts/build_tutorial_subset.py
    python scripts/build_tutorial_subset.py --episodes 2500 --out subsets/tutorial
"""

import argparse
import collections
import itertools
import json
import logging
import os
import random
import subprocess
import sys

import pandas as pd
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("build_tutorial_subset")

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Frequent members of the two lexical sets notebook 07 measures. Selection only
# needs words common enough to yield tokens; the authoritative sets live in
# sporc/phonetics.py.
PROBE_WORDS = ["talk", "thought", "long", "off", "across", "call", "small",
               "not", "got", "lot", "job", "problem", "top", "stop", "common"]

# Names that SPoRC reports as guests but which are always artifacts: people who
# were discussed, not present.
#
# `guest_predicted_names` extracts names from text and cannot tell "joined today
# by X" from "let's talk about X", so in May-June 2020 it makes George Floyd the
# single most-connected "guest" in the corpus (237 podcasts). Checking
# `guest_speaker_labels` -- i.e. that a guest was actually diarized -- removes
# ~98% of that, but not all: the name-attribution step sometimes pins a mentioned
# name onto a real speaker, leaving Floyd on 5 podcasts. Selecting "guests" by
# podcast count therefore promotes exactly these cases to the top of the list.
#
# This list is deliberately small and specific. It is not a general fix -- there
# is no way to enumerate every mentioned name -- it just keeps the teaching
# subset from being built around people who never spoke. Notebook 03 teaches the
# artefact rather than hiding it.
NOT_GUESTS = {
    "george floyd", "breonna taylor", "brianna taylor", "ahmaud arbery",
    "derek chauvin", "jesus christ", "donald trump", "joe biden",
    "anthony fauci", "barack obama", "hillary clinton", "kim jong un",
    "vladimir putin", "bernie sanders", "elizabeth warren",
    # Not people at all: greetings and stock phrases the extractor reads as names.
    "kia ora", "mary ann", "dee dee", "john paul", "mark anthony",
}


def _read(path, columns=None):
    # ParquetFile(...).read(), never pq.read_table(path): the latter infers hive
    # partitioning from a podcast_id=<id> parent and collides with the file's own
    # podcast_id column (ArrowTypeError: string vs dictionary).
    return pq.ParquetFile(path).read(columns=columns).to_pandas()


def choose_parts(sporc, n_parts, want_guests):
    """
    Pick the ``n_parts`` turn parts that between them hold the most repeat
    guests, and return the podcasts living in them.

    A subset drawn from a handful of parts is one a user can actually rebuild:
    podcasts are packed into shared part files, so fetching a scattered
    selection costs whole parts either way. Measured on dataset 1.1, 1,000
    podcasts sampled at random span 116 of the 127 turn parts and 11.6 GB,
    where the same number taken from within a few parts costs those parts and
    nothing else.

    Repeat guests are what makes this tight rather than arbitrary. Notebook 04
    needs people who appear on two different shows *and* were diarized on both,
    and only about 2.5% of cross-podcast guests ever were. The richest single
    part holds 7 of them; three parts together reach into the forties, which is
    why the default is three and not one.
    """
    backend = sporc._parquet_backend
    smap = backend.shard_map
    part_of = {pid: loc[0] for pid, loc in smap.items("turns_text")}
    pods_in = collections.defaultdict(set)
    for pid, part in part_of.items():
        pods_in[part].add(pid)

    labelled = diarized_guest_index(backend, smap)

    def n_repeat(pods):
        return sum(1 for ps in labelled.values() if len(ps & pods) >= 2)

    # Search combinations rather than growing greedily. Guests are what makes a
    # combination good and they do not add up: a part rich on its own can share
    # its guests with the parts you would pick next, while two thinner parts
    # that happen to share shows beat it together. Greedy picked 34 here where
    # the best triple holds 42.
    ranked = sorted(pods_in, key=lambda p: -n_repeat(pods_in[p]))
    pool_parts = ranked[:max(8, n_parts * 3)]
    best_combo, best_gain = None, -1
    for combo in itertools.combinations(pool_parts, min(n_parts, len(pool_parts))):
        pods = set().union(*(pods_in[p] for p in combo))
        gain = n_repeat(pods)
        if gain > best_gain:
            best_combo, best_gain = combo, gain
    chosen = list(best_combo)
    pool = set().union(*(pods_in[p] for p in chosen))
    for part in chosen:
        logger.info("  + %s (%d podcasts)", part, len(pods_in[part]))

    real = {n: ps & pool for n, ps in labelled.items()}
    real = {n: ps for n, ps in real.items() if len(ps) >= 2}
    if len(real) < want_guests:
        logger.warning(
            "Only %d repeat guests available in %d parts (wanted %d). "
            "Notebook 04 works with fewer, but with less to say; raise "
            "--parts to widen the pool.", len(real), len(chosen), want_guests)
    logger.info("Parts chosen: %s", ", ".join(chosen))
    logger.info("Universe: %d podcasts, %d repeat guests", len(pool), len(real))
    return chosen, pool, real


def diarized_guest_index(backend, smap):
    """
    Guest name -> podcasts where that guest was actually diarized.

    `guest_predicted_names` lists people *mentioned*, so it cannot be used:
    its top "guests" are figures who never spoke on any podcast. A non-empty
    `guest_speaker_labels` is the only evidence a guest was really there, and
    that lives in the episode rows rather than the catalogs.
    """
    labelled = collections.defaultdict(set)
    parts = sorted({loc[0] for _, loc in smap.items("episodes")})
    logger.info("Indexing diarized guests across %d episode parts", len(parts))
    for i, part in enumerate(parts, 1):
        # Two columns out of a ~110 MB part, across every part in the corpus.
        # read_columns range-reads just those column chunks over HTTP rather
        # than downloading each part in full only to discard ~99% of it, which
        # on the Hub is the difference between ~100 MB and ~15 GB for this scan.
        t = backend._source.read_columns(
            smap.relpath("episodes", part),
            columns=["podcast_id", "guest_speaker_labels"])
        if t is None:
            continue
        for pid, gl in zip(t.column("podcast_id").to_pylist(),
                           t.column("guest_speaker_labels").to_pylist()):
            if not gl or gl in ("{}", "SPEAKER_DATA_UNAVAILABLE"):
                continue
            raw = gl if isinstance(gl, str) else json.dumps(gl)
            try:
                d = json.loads(raw)
            except Exception:
                continue
            if isinstance(d, dict):
                for k in d:
                    name = str(k).strip().lower()
                    if name and name not in NOT_GUESTS:
                        labelled[name].add(pid)
        if i % 20 == 0:
            logger.info("  %d/%d episode parts", i, len(parts))
    return labelled


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default=os.path.join(REPO, "subsets", "tutorial"))
    p.add_argument("--episodes", type=int, default=2000,
                   help="Approximate episodes from the random diarized sample")
    p.add_argument("--guests", type=int, default=40,
                   help="Repeat guests to guarantee (drives notebook 04)")
    p.add_argument("--parts", type=int, default=3,
                   help="Part files to draw the whole subset from. The point "
                        "of the subset is that it is cheap to rebuild, and a "
                        "scattered selection costs whole parts anyway; three "
                        "is where there are enough diarized repeat guests for "
                        "notebook 04.")
    p.add_argument("--seed", type=int, default=20200525)
    p.add_argument("--data-dir", default=None,
                   help="Local copy of the full Parquet layout to build from. "
                        "Without it the podcasts come from the Hub, which for a "
                        "selection this scattered means fetching most part "
                        "files -- see the module docstring.")
    args = p.parse_args()

    from sporc import SPORCDataset

    if args.data_dir:
        logger.info("Reading from local layout %s", args.data_dir)
        sporc = SPORCDataset(parquet_dir=args.data_dir)
    else:
        logger.info("Downloading metadata catalogs if needed (~195 MB)...")
        logger.warning(
            "Building from the Hub. The selection spans most part files, so "
            "expect tens of GB of downloads; pass --data-dir to read a local "
            "copy instead.")
        sporc = SPORCDataset()
    snap = sporc._parquet_backend.data_dir
    logger.info("Snapshot: %s", snap)

    # 1. Confine the whole selection to a few part files, then take the repeat
    #    guests from inside them.
    parts, universe, real_guests = choose_parts(sporc, args.parts, args.guests)
    ranked = sorted(real_guests.items(), key=lambda kv: -len(kv[1]))
    guest_names = [n for n, _ in ranked[:args.guests]]
    guest_pods = set()
    for n in guest_names:
        guest_pods |= real_guests[n]
    logger.info("Repeat guests kept: %d, spanning %d podcasts",
                len(guest_names), len(guest_pods))

    # 2. A random diarized sample for the general-purpose notebooks, drawn from
    #    the same parts so the subset stays cheap to rebuild.
    ec = _read(f"{snap}/metadata/episode_catalog.parquet",
               ["episode_id", "podcast_id", "num_main_speakers"])
    ec = ec[ec.podcast_id.isin(universe)]
    ec["nms"] = pd.to_numeric(ec.num_main_speakers, errors="coerce").fillna(0)
    diar = ec[ec.nms > 0]
    per_pod = diar.groupby("podcast_id").size()
    # Podcasts with a few episodes each: enough turns to model, not so many that
    # one show dominates.
    ok = per_pod[(per_pod >= 3) & (per_pod <= 40)].index.tolist()
    rng = random.Random(args.seed)
    rng.shuffle(ok)
    random_pods, total = [], 0
    for pid in ok:
        if total >= args.episodes:
            break
        if pid in guest_pods:
            continue
        random_pods.append(pid)
        total += int(per_pod[pid])
    logger.info("Random diarized sample: %d podcasts / ~%d episodes",
                len(random_pods), total)

    pods = list(dict.fromkeys(list(guest_pods) + random_pods))
    logger.info("Total podcasts to fetch: %d", len(pods))

    # 3. Fetch their partitions.
    ids_path = os.path.join(os.path.dirname(args.out), "tutorial_ids.txt")
    os.makedirs(os.path.dirname(ids_path), exist_ok=True)
    with open(ids_path, "w") as f:
        f.write("# Podcasts in the tutorial subset. Generated by "
                "scripts/build_tutorial_subset.py\n")
        f.write(f"# seed={args.seed} episodes={args.episodes} "
                f"guests={len(guest_names)}\n")
        f.write(f"# drawn from {len(parts)} part(s): {', '.join(parts)}\n")
        for pid in pods:
            f.write(pid + "\n")
    logger.info("Wrote %s", ids_path)

    if args.data_dir:
        logger.info("Local layout, nothing to prefetch")
    else:
        logger.info("Prefetching partitions (this is the slow part)...")
        res = sporc.prefetch({"podcast_ids": pods})
        logger.info("Prefetched %d podcasts, %d files",
                    res["podcasts"], res["files"])

    # 4. Cut the self-contained subset.
    cmd = [sys.executable, os.path.join(REPO, "scripts", "make_subset.py"),
           "--data-dir", snap, "--out", args.out, "--podcast-ids", ids_path]
    logger.info("Running: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)

    with open(os.path.join(args.out, "tutorial_guests.json"), "w") as f:
        json.dump({"repeat_guests": guest_names, "seed": args.seed}, f, indent=2)
    logger.info("Done: %s", args.out)


if __name__ == "__main__":
    main()
