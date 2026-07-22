"""
Write manifest.json from what is actually on disk.

v1.0's manifest was hand-maintained and drifted from the tree it described.
Every count here is measured at write time, so the manifest cannot claim
something the release does not contain.
"""

import glob
import json
import os
import sys
from datetime import datetime, timezone

import pyarrow.parquet as pq

from buildconfig import load as _load_build_config

_CFG = _load_build_config()

BUILD = _CFG.build
OUT = _CFG.release

TREES = [
    ("episodes", "episodes", "episodes/part-*.parquet",
     "Full episode data including transcript"),
    ("turns_text", "turns/text", "turns/text/part-*.parquet",
     "Turn text, timing, and speaker info"),
    ("turns_metrics", "turns/metrics", "turns/metrics/part-*.parquet",
     "Turn-level computed metrics"),
    ("acoustics", "acoustics", "acoustics/part-*.parquet",
     "MFCCs, F0, formants per turn"),
]


def rows_and_size(pattern):
    files = sorted(glob.glob(os.path.join(OUT, pattern)))
    rows = groups = size = 0
    for f in files:
        m = pq.ParquetFile(f).metadata
        rows += m.num_rows
        groups += m.num_row_groups
        size += os.path.getsize(f)
    return len(files), rows, groups, size


def main():
    trees = {}
    total_files = total_bytes = 0
    for name, _, pattern, desc in TREES:
        n, rows, groups, size = rows_and_size(pattern)
        trees[name] = {
            "path": pattern,
            "description": desc,
            "files": n,
            "rows": rows,
            "row_groups": groups,
            "bytes": size,
        }
        total_files += n
        total_bytes += size
        print(f"{name:14s} {n:4d} files  {rows:>13,} rows  "
              f"{size/2**30:7.2f} GB", flush=True)

    meta = {}
    for f in sorted(glob.glob(os.path.join(OUT, "metadata", "*"))):
        base = os.path.basename(f)
        # The client writes its own caches (_index_cache.pkl, _podcast_df.arrow,
        # _episode_df.arrow) into whatever metadata directory it reads, which
        # during testing is this one. They are derived, machine-local, and
        # several hundred MB; listing them here would publish them as though
        # they were part of the release.
        if base.startswith("_"):
            print(f"  skipping client cache metadata/{base}", flush=True)
            continue
        # Documentation is counted once, under "docs", so that metadata/ stays
        # a list of data files and every directory's README is treated alike.
        if base == "README.md":
            continue
        size = os.path.getsize(f)
        entry = {"bytes": size}
        if base.endswith(".parquet"):
            entry["rows"] = pq.ParquetFile(f).metadata.num_rows
        meta[base] = entry
        total_files += 1
        total_bytes += size
        print(f"  metadata/{base:32s} {size/2**20:9.1f} MB", flush=True)

    # Every README in the release, including the root one and the per-directory
    # ones. Listed so the manifest's file count matches what gets uploaded --
    # an upload allow-list built from the trees and metadata/ alone would drop
    # the documentation without anything noticing.
    docs = {}
    for f in sorted(glob.glob(os.path.join(OUT, "**", "README.md"),
                              recursive=True)):
        rel = os.path.relpath(f, OUT)
        docs[rel] = {"bytes": os.path.getsize(f)}
        total_files += 1
        total_bytes += os.path.getsize(f)
        print(f"  doc {rel:38s} {os.path.getsize(f)/2**10:9.1f} KB", flush=True)

    # From episode_metrics, which has one row per episode that actually has
    # turns. Counting episode_catalog.total_sp_labels > 0 instead undercounts
    # by 12: those episodes carry a single turn and no speaker label, so they
    # have turn data while the label count says they do not. Verified against
    # COUNT(DISTINCT episode_id) over turns/text, which is 731,113.
    with_turns = pq.ParquetFile(
        f"{OUT}/metadata/episode_metrics.parquet").metadata.num_rows

    manifest = {
        "version": "1.1",
        "schema_version": 2,
        "creation_date": datetime.now(timezone.utc).isoformat(),
        "source": "blitt/SPoRC (HuggingFace)",
        "record_counts": {
            "podcasts": pq.ParquetFile(
                f"{OUT}/metadata/podcast_catalog.parquet").metadata.num_rows,
            "episodes": pq.ParquetFile(
                f"{OUT}/metadata/episode_catalog.parquet").metadata.num_rows,
            "episodes_with_turns": with_turns,
            "turns": trees["turns_text"]["rows"],
            "category_index_rows": pq.ParquetFile(
                f"{OUT}/metadata/category_index.parquet").metadata.num_rows,
            "hostname_index_rows": pq.ParquetFile(
                f"{OUT}/metadata/hostname_index.parquet").metadata.num_rows,
            "speaker_name_index_rows": pq.ParquetFile(
                f"{OUT}/metadata/speaker_name_index.parquet").metadata.num_rows,
        },
        "totals": {"files": total_files, "bytes": total_bytes},
        "compression": "zstd",
        "layout": {
            "style": "sorted parts, one row group per podcast",
            "ordering": "primary_category, then podcast_id",
            "shard_map": "metadata/shard_map.parquet",
            "note": ("Each part file holds many podcasts; each podcast occupies "
                     "exactly one row group. Look up a podcast in the shard map "
                     "to read it with a single ranged read."),
            "trees": trees,
            "metadata": meta,
            "docs": docs,
        },
        "id_scheme": {
            "podcast_id": "md5(rssUrl)[:12]",
            "episode_id": "md5(mp3url)[:16]",
        },
        "changes_from_1_0": [
            "Repacked from ~685,000 per-podcast files into a few hundred parts "
            "to stay within Hugging Face request limits.",
            "Speaker turns recomputed with a corrected word-to-segment matcher; "
            "see the speakers_recomputed column.",
            "358,509 previously unmerged diarized episodes added; their speakers "
            "are anonymous labels with no inferred names or roles.",
            "episode_metrics.unique_speaker_count now counts speaker labels "
            "rather than turns.",
            "Acoustic features moved to their own acoustics/ tree and gained "
            "standard deviations.",
            "Removed 85,541 turn rows that 1.0 had stored twice. "
            "(episode_id, turn_count) is now unique in every turn tree, so it "
            "is safe to join on.",
            "turns/text.word_count renamed to token_count, which is what it "
            "counts: timestamped tokens from the transcript aligner, which "
            "treats punctuation as a token and so runs about 21% above the "
            "number of words. For words, use turns/metrics.word_count.",
            "turns_search.duckdb no longer duplicates turn text; join back to "
            "turns/text on (episode_id, turn_count).",
            "Turn text for SQL search moved to its own optional file, "
            "turns_text.duckdb, keyed on (episode_id, turn_count). Both "
            "databases are optional: the Parquet trees alone are 46 GB of the "
            "94 GB total.",
        ],
    }

    path = os.path.join(OUT, "manifest.json")
    with open(path, "w") as fh:
        json.dump(manifest, fh, indent=2)
        fh.write("\n")
    print(f"\nwrote {path}")
    print(f"total {total_files:,} files, {total_bytes/2**30:.1f} GB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
