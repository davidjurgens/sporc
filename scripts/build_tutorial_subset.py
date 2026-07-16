#!/usr/bin/env python3
"""
Build the subset the tutorial notebooks read.

Downloads the metadata catalogs (~195 MB, once), selects the podcasts the
tutorials need, fetches only those partitions, and runs make_subset.py over the
result. The full corpus is never downloaded.

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


def find_repeat_guest_podcasts(sporc, snap, want_guests, max_scan):
    """
    Podcasts sharing a guest who is actually diarized in them.

    `guest_predicted_names` cannot be used for this: it lists people *mentioned*,
    so its top "guests" are figures like George Floyd who never spoke on any
    podcast. Only a non-empty `guest_speaker_labels` means a guest was really
    there, and that lives in the episode partitions rather than the catalogs --
    hence the scan.
    """
    sn = _read(f"{snap}/metadata/speaker_name_index.parquet")
    ec = _read(f"{snap}/metadata/episode_catalog.parquet",
               ["episode_id", "podcast_id", "num_main_speakers"])
    ec["nms"] = pd.to_numeric(ec.num_main_speakers, errors="coerce").fillna(0)
    multi = set(ec[ec.nms >= 2].episode_id)

    g = sn[(sn.role == "guest") & (sn.episode_id.isin(multi))]
    agg = g.groupby("name_normalized").agg(n_pods=("podcast_id", "nunique"))
    cand = set(agg[agg.n_pods >= 2].index)
    pods = sorted(g[g.name_normalized.isin(cand)].podcast_id.unique())[:max_scan]
    logger.info("Scanning %d podcasts for guests with real speaker labels", len(pods))

    src = sporc._parquet_backend._source

    def scan(pid):
        try:
            p = src.path(f"episodes/podcast_id={pid}/data.parquet")
            if not p:
                return pid, {}
            df = _read(p, ["episode_id", "guest_speaker_labels"])
        except Exception:
            return pid, {}
        found = set()
        for gl in df.guest_speaker_labels:
            if gl is None:
                continue
            s = gl if isinstance(gl, str) else json.dumps(gl)
            if not s or s in ("{}", "SPEAKER_DATA_UNAVAILABLE"):
                continue
            try:
                d = json.loads(s)
            except Exception:
                continue
            if isinstance(d, dict):
                found |= {str(k).strip().lower() for k in d}
        return pid, found

    real = collections.defaultdict(set)
    with ThreadPoolExecutor(max_workers=16) as ex:
        for i, (pid, found) in enumerate(ex.map(scan, pods), 1):
            for name in found:
                real[name].add(pid)
            if i % 1000 == 0:
                logger.info("  %d/%d scanned, %d named guests so far", i, len(pods), len(real))

    shared = {n: p for n, p in real.items() if len(p) >= 2}
    logger.info("Guests with real labels on >=2 podcasts: %d", len(shared))

    dropped = sorted(n for n in shared if n in NOT_GUESTS)
    if dropped:
        # These rank near the top precisely because the artefact scales with how
        # much a name is discussed, so leaving them in would build the subset
        # around people who never spoke.
        logger.info("Dropping %d known non-guests from the selection: %s",
                    len(dropped), ", ".join(dropped))
    shared = {n: p for n, p in shared.items() if n not in NOT_GUESTS}

    keep, chosen = set(), []
    for name, pids in sorted(shared.items(), key=lambda kv: -len(kv[1])):
        if len(chosen) >= want_guests:
            break
        chosen.append(name)
        keep |= set(pids)
    logger.info("Selected %d repeat guests spanning %d podcasts", len(chosen), len(keep))
    return keep, chosen


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default=os.path.join(REPO, "subsets", "tutorial"))
    p.add_argument("--episodes", type=int, default=2000,
                   help="Approximate episodes from the random diarized sample")
    p.add_argument("--guests", type=int, default=40,
                   help="Repeat guests to guarantee (drives notebook 04)")
    p.add_argument("--max-scan", type=int, default=6000,
                   help="Cap on podcasts scanned for real repeat guests")
    p.add_argument("--seed", type=int, default=20200525)
    args = p.parse_args()

    from sporc import SPORCDataset

    logger.info("Downloading metadata catalogs if needed (~195 MB)...")
    sporc = SPORCDataset()
    snap = sporc._parquet_backend.data_dir
    logger.info("Snapshot: %s", snap)

    # 1. Podcasts containing validated repeat guests.
    guest_pods, guest_names = find_repeat_guest_podcasts(
        sporc, snap, args.guests, args.max_scan)

    # 2. A random diarized sample for the general-purpose notebooks.
    ec = _read(f"{snap}/metadata/episode_catalog.parquet",
               ["episode_id", "podcast_id", "num_main_speakers"])
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
        f.write(f"# seed={args.seed} episodes={args.episodes} guests={args.guests}\n")
        for pid in pods:
            f.write(pid + "\n")
    logger.info("Wrote %s", ids_path)

    logger.info("Prefetching partitions (this is the slow part)...")
    res = sporc.prefetch({"podcast_ids": pods})
    logger.info("Prefetched %d podcasts, %d files", res["podcasts"], res["files"])

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
