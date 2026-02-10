#!/usr/bin/env python3
"""
Build precomputed indexes for SPORC Parquet dataset.

Three phases:
  1. Speaker Name Index: flatten host/guest names from episode catalog
  2. Episode + Turn Metrics: compute per-episode and per-turn aggregates
  3. DuckDB Turn Database: full-text search index over all turn text

Usage:
    python scripts/build_indexes.py --data-dir /shared/6/projects/sporc/v1
    python scripts/build_indexes.py --data-dir /shared/6/projects/sporc/v1 --phase 1
    python scripts/build_indexes.py --data-dir /shared/6/projects/sporc/v1 --phase 2
    python scripts/build_indexes.py --data-dir /shared/6/projects/sporc/v1 --phase 3
"""

import argparse
import logging
import os
import re
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DEFAULT_DATA_DIR = "/shared/6/projects/sporc/v1"

DISCOURSE_MARKERS = re.compile(
    r"\b(um|uh|uh huh|mm hmm|like|you know|i mean|so|well|right|okay|oh)\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Phase 1: Speaker Name Index
# ---------------------------------------------------------------------------
def build_speaker_name_index(data_dir: str) -> None:
    """Build metadata/speaker_name_index.parquet from episode catalog."""
    meta_dir = os.path.join(data_dir, "metadata")
    ec_path = os.path.join(meta_dir, "episode_catalog.parquet")
    out_path = os.path.join(meta_dir, "speaker_name_index.parquet")

    logger.info("Phase 1: Building speaker name index from %s", ec_path)
    start = time.time()

    table = pq.read_table(
        ec_path,
        columns=[
            "episode_id",
            "podcast_id",
            "host_predicted_names",
            "guest_predicted_names",
        ],
    )

    names_norm = []
    names_orig = []
    roles = []
    episode_ids = []
    podcast_ids = []

    eids = table.column("episode_id").to_pylist()
    pids = table.column("podcast_id").to_pylist()
    hosts = table.column("host_predicted_names").to_pylist()
    guests = table.column("guest_predicted_names").to_pylist()

    for i in tqdm(range(len(eids)), desc="Flattening speaker names"):
        eid = eids[i]
        pid = pids[i]

        host_list = hosts[i] if isinstance(hosts[i], list) else []
        for name in host_list:
            if name is None:
                continue
            name_str = str(name).strip()
            if not name_str:
                continue
            names_norm.append(name_str.lower())
            names_orig.append(name_str)
            roles.append("host")
            episode_ids.append(eid)
            podcast_ids.append(pid)

        guest_list = guests[i] if isinstance(guests[i], list) else []
        for name in guest_list:
            if name is None:
                continue
            name_str = str(name).strip()
            if not name_str:
                continue
            names_norm.append(name_str.lower())
            names_orig.append(name_str)
            roles.append("guest")
            episode_ids.append(eid)
            podcast_ids.append(pid)

    out_table = pa.table(
        {
            "name_normalized": pa.array(names_norm, type=pa.string()),
            "name_original": pa.array(names_orig, type=pa.string()),
            "role": pa.array(roles, type=pa.string()),
            "episode_id": pa.array(episode_ids, type=pa.string()),
            "podcast_id": pa.array(podcast_ids, type=pa.string()),
        }
    )

    pq.write_table(out_table, out_path, compression="snappy")
    elapsed = time.time() - start
    logger.info(
        "Phase 1 complete: %s rows written to %s in %.1fs",
        f"{len(names_norm):,}",
        out_path,
        elapsed,
    )


# ---------------------------------------------------------------------------
# Phase 2: Episode + Turn Metrics
# ---------------------------------------------------------------------------
def _count_discourse_markers(text: str) -> int:
    """Count discourse markers in text."""
    return len(DISCOURSE_MARKERS.findall(text))


def _count_words(text: str) -> int:
    """Count words in text."""
    return len(text.split())


def build_episode_and_turn_metrics(data_dir: str) -> None:
    """Build episode_metrics.parquet and per-partition turn metrics."""
    meta_dir = os.path.join(data_dir, "metadata")
    turns_dir = os.path.join(data_dir, "turns")
    out_episode_path = os.path.join(meta_dir, "episode_metrics.parquet")

    logger.info("Phase 2: Building episode + turn metrics")
    start = time.time()

    # Collect all turn partition directories
    partition_dirs = sorted(
        [
            d
            for d in os.listdir(turns_dir)
            if d.startswith("podcast_id=") and os.path.isdir(os.path.join(turns_dir, d))
        ]
    )
    logger.info("Found %d turn partition directories", len(partition_dirs))

    # Episode-level accumulators
    ep_metrics = {}  # episode_id -> metrics dict

    for pdir in tqdm(partition_dirs, desc="Processing turn partitions"):
        podcast_id = pdir.replace("podcast_id=", "")
        text_path = os.path.join(turns_dir, pdir, "text.parquet")

        if not os.path.exists(text_path):
            continue

        try:
            table = pq.ParquetFile(text_path).read()
        except Exception as e:
            logger.warning("Failed to read %s: %s", text_path, e)
            continue

        if table.num_rows == 0:
            continue

        # Convert to column lists for speed
        eids = table.column("episode_id").to_pylist()
        texts = table.column("turn_text").to_pylist()
        starts = table.column("start_time").to_pylist()
        ends = table.column("end_time").to_pylist()
        durations = table.column("duration").to_pylist()
        turn_counts = table.column("turn_count").to_pylist()
        roles = table.column("inferred_speaker_role").to_pylist()

        # Group rows by episode for this partition
        ep_rows = defaultdict(list)
        for i in range(len(eids)):
            ep_rows[eids[i]].append(i)

        # Per-turn metric arrays for the partition
        tm_episode_ids = []
        tm_turn_counts = []
        tm_word_counts = []
        tm_wps = []
        tm_gap_from_prev = []
        tm_overlap_with_prev = []
        tm_discourse_counts = []
        tm_char_counts = []

        for eid, indices in ep_rows.items():
            # Sort indices by start_time
            indices.sort(key=lambda idx: starts[idx] or 0.0)

            ep_word_count = 0
            ep_host_words = 0
            ep_guest_words = 0
            ep_host_turns = 0
            ep_guest_turns = 0
            ep_dm_count = 0
            ep_durations = []
            ep_gaps = []
            ep_overlaps = []
            speakers_seen = set()

            prev_end = None

            for j, idx in enumerate(indices):
                text = str(texts[idx]) if texts[idx] else ""
                st = float(starts[idx]) if starts[idx] is not None else 0.0
                et = float(ends[idx]) if ends[idx] is not None else 0.0
                dur = float(durations[idx]) if durations[idx] is not None else 0.0
                tc = int(turn_counts[idx]) if turn_counts[idx] is not None else 0
                role = str(roles[idx]) if roles[idx] else ""

                wc = _count_words(text)
                dm = _count_discourse_markers(text)
                cc = len(text)
                wps = wc / dur if dur > 0 else 0.0

                # Gap / overlap from previous turn
                gap = None
                overlap = None
                if prev_end is not None and st > 0:
                    diff = st - prev_end
                    if diff >= 0:
                        gap = diff
                        overlap = 0.0
                    else:
                        gap = 0.0
                        overlap = abs(diff)

                # Accumulate episode-level stats
                ep_word_count += wc
                ep_dm_count += dm
                if dur > 0:
                    ep_durations.append(dur)
                if gap is not None and gap > 0:
                    ep_gaps.append(gap)
                if overlap is not None and overlap > 0:
                    ep_overlaps.append(overlap)

                speakers_seen.add(tc)  # turn_count as proxy; use speaker col if available

                role_lower = role.lower() if role else ""
                if "host" in role_lower:
                    ep_host_words += wc
                    ep_host_turns += 1
                elif "guest" in role_lower:
                    ep_guest_words += wc
                    ep_guest_turns += 1

                # Store turn-level metrics
                tm_episode_ids.append(eid)
                tm_turn_counts.append(tc)
                tm_word_counts.append(wc)
                tm_wps.append(round(wps, 4))
                tm_gap_from_prev.append(round(gap, 4) if gap is not None else None)
                tm_overlap_with_prev.append(
                    round(overlap, 4) if overlap is not None else None
                )
                tm_discourse_counts.append(dm)
                tm_char_counts.append(cc)

                if et > 0:
                    prev_end = et

            # Compute episode-level aggregates
            total_turns = len(indices)
            avg_dur = (
                statistics.mean(ep_durations) if ep_durations else 0.0
            )
            median_dur = (
                statistics.median(ep_durations) if ep_durations else 0.0
            )
            total_dur = sum(ep_durations) if ep_durations else 0.0
            avg_wps = ep_word_count / total_dur if total_dur > 0 else 0.0
            avg_gap = statistics.mean(ep_gaps) if ep_gaps else 0.0
            total_overlap = sum(ep_overlaps)
            dm_rate = (
                (ep_dm_count / ep_word_count * 1000) if ep_word_count > 0 else 0.0
            )

            host_turn_prop = (
                ep_host_turns / total_turns if total_turns > 0 else 0.0
            )
            host_word_prop = (
                ep_host_words / ep_word_count if ep_word_count > 0 else 0.0
            )

            # Host / guest speaking rates
            host_dur = sum(
                float(durations[idx])
                for idx in indices
                if durations[idx] and str(roles[idx]).lower().startswith("host")
            )
            guest_dur = sum(
                float(durations[idx])
                for idx in indices
                if durations[idx] and str(roles[idx]).lower().startswith("guest")
            )
            sr_host = ep_host_words / host_dur if host_dur > 0 else 0.0
            sr_guest = ep_guest_words / guest_dur if guest_dur > 0 else 0.0

            # Count unique speakers from the speaker column if available
            unique_speakers = len(speakers_seen)

            ep_metrics[eid] = {
                "episode_id": eid,
                "podcast_id": podcast_id,
                "total_word_count": ep_word_count,
                "total_turn_count": total_turns,
                "unique_speaker_count": unique_speakers,
                "avg_turn_duration": round(avg_dur, 4),
                "median_turn_duration": round(median_dur, 4),
                "avg_words_per_second": round(avg_wps, 4),
                "host_word_count": ep_host_words,
                "guest_word_count": ep_guest_words,
                "host_turn_proportion": round(host_turn_prop, 4),
                "host_word_proportion": round(host_word_prop, 4),
                "avg_gap_duration": round(avg_gap, 4),
                "total_overlap_duration": round(total_overlap, 4),
                "discourse_marker_count": ep_dm_count,
                "discourse_marker_rate": round(dm_rate, 4),
                "speaking_rate_host": round(sr_host, 4),
                "speaking_rate_guest": round(sr_guest, 4),
            }

        # Write per-partition turn metrics
        if tm_episode_ids:
            metrics_path = os.path.join(turns_dir, pdir, "metrics.parquet")
            tm_table = pa.table(
                {
                    "episode_id": pa.array(tm_episode_ids, type=pa.string()),
                    "turn_count": pa.array(tm_turn_counts, type=pa.int32()),
                    "word_count": pa.array(tm_word_counts, type=pa.int32()),
                    "words_per_second": pa.array(tm_wps, type=pa.float32()),
                    "gap_from_prev": pa.array(tm_gap_from_prev, type=pa.float32()),
                    "overlap_with_prev": pa.array(
                        tm_overlap_with_prev, type=pa.float32()
                    ),
                    "discourse_marker_count": pa.array(
                        tm_discourse_counts, type=pa.int16()
                    ),
                    "char_count": pa.array(tm_char_counts, type=pa.int32()),
                }
            )
            pq.write_table(tm_table, metrics_path, compression="snappy")

    # Write episode-level metrics
    if ep_metrics:
        rows = list(ep_metrics.values())
        ep_table = pa.table(
            {
                "episode_id": pa.array(
                    [r["episode_id"] for r in rows], type=pa.string()
                ),
                "podcast_id": pa.array(
                    [r["podcast_id"] for r in rows], type=pa.string()
                ),
                "total_word_count": pa.array(
                    [r["total_word_count"] for r in rows], type=pa.int32()
                ),
                "total_turn_count": pa.array(
                    [r["total_turn_count"] for r in rows], type=pa.int32()
                ),
                "unique_speaker_count": pa.array(
                    [r["unique_speaker_count"] for r in rows], type=pa.int16()
                ),
                "avg_turn_duration": pa.array(
                    [r["avg_turn_duration"] for r in rows], type=pa.float32()
                ),
                "median_turn_duration": pa.array(
                    [r["median_turn_duration"] for r in rows], type=pa.float32()
                ),
                "avg_words_per_second": pa.array(
                    [r["avg_words_per_second"] for r in rows], type=pa.float32()
                ),
                "host_word_count": pa.array(
                    [r["host_word_count"] for r in rows], type=pa.int32()
                ),
                "guest_word_count": pa.array(
                    [r["guest_word_count"] for r in rows], type=pa.int32()
                ),
                "host_turn_proportion": pa.array(
                    [r["host_turn_proportion"] for r in rows], type=pa.float32()
                ),
                "host_word_proportion": pa.array(
                    [r["host_word_proportion"] for r in rows], type=pa.float32()
                ),
                "avg_gap_duration": pa.array(
                    [r["avg_gap_duration"] for r in rows], type=pa.float32()
                ),
                "total_overlap_duration": pa.array(
                    [r["total_overlap_duration"] for r in rows], type=pa.float32()
                ),
                "discourse_marker_count": pa.array(
                    [r["discourse_marker_count"] for r in rows], type=pa.int32()
                ),
                "discourse_marker_rate": pa.array(
                    [r["discourse_marker_rate"] for r in rows], type=pa.float32()
                ),
                "speaking_rate_host": pa.array(
                    [r["speaking_rate_host"] for r in rows], type=pa.float32()
                ),
                "speaking_rate_guest": pa.array(
                    [r["speaking_rate_guest"] for r in rows], type=pa.float32()
                ),
            }
        )
        pq.write_table(ep_table, out_episode_path, compression="snappy")

    elapsed = time.time() - start
    logger.info(
        "Phase 2 complete: %s episode metrics written to %s in %.1fs",
        f"{len(ep_metrics):,}",
        out_episode_path,
        elapsed,
    )


# ---------------------------------------------------------------------------
# Phase 3: DuckDB Turn Database
# ---------------------------------------------------------------------------
def build_duckdb_search(data_dir: str) -> None:
    """Build metadata/turns_search.duckdb with FTS index."""
    try:
        import duckdb
    except ImportError:
        logger.error(
            "DuckDB is required for Phase 3. Install with: pip install duckdb"
        )
        sys.exit(1)

    meta_dir = os.path.join(data_dir, "metadata")
    turns_dir = os.path.join(data_dir, "turns")
    db_path = os.path.join(meta_dir, "turns_search.duckdb")

    # Remove existing DB if present
    if os.path.exists(db_path):
        os.remove(db_path)
        logger.info("Removed existing %s", db_path)

    logger.info("Phase 3: Building DuckDB full-text search database")
    start = time.time()

    con = duckdb.connect(db_path)

    # Create the turns table (row_id is the FTS key column)
    con.execute("""
        CREATE TABLE turns (
            row_id INTEGER,
            episode_id VARCHAR,
            podcast_id VARCHAR,
            turn_count INTEGER,
            turn_text VARCHAR,
            start_time DOUBLE,
            end_time DOUBLE,
            duration DOUBLE,
            speaker_role VARCHAR,
            speaker_name VARCHAR,
            word_count INTEGER
        )
    """)
    # Running counter for row_id
    next_row_id = [0]

    # Insert from all turn partitions
    partition_dirs = sorted(
        [
            d
            for d in os.listdir(turns_dir)
            if d.startswith("podcast_id=") and os.path.isdir(os.path.join(turns_dir, d))
        ]
    )

    total_inserted = 0
    for pdir in tqdm(partition_dirs, desc="Loading turns into DuckDB"):
        podcast_id = pdir.replace("podcast_id=", "")
        text_path = os.path.join(turns_dir, pdir, "text.parquet")

        if not os.path.exists(text_path):
            continue

        try:
            # Read via PyArrow to handle dictionary-encoded columns,
            # then register as a DuckDB view for insertion
            pa_table = pq.ParquetFile(text_path).read()
            n_rows = pa_table.num_rows
            start_id = next_row_id[0]
            con.register("_tmp_partition", pa_table)
            con.execute(
                f"""
                INSERT INTO turns
                SELECT
                    CAST(({start_id} + row_number() OVER ()) AS INTEGER) AS row_id,
                    CAST(episode_id AS VARCHAR),
                    '{podcast_id}' AS podcast_id,
                    CAST(turn_count AS INTEGER),
                    CAST(turn_text AS VARCHAR),
                    start_time,
                    end_time,
                    duration,
                    CAST(inferred_speaker_role AS VARCHAR) AS speaker_role,
                    CAST(inferred_speaker_name AS VARCHAR) AS speaker_name,
                    CAST(length(CAST(turn_text AS VARCHAR))
                         - length(replace(CAST(turn_text AS VARCHAR), ' ', ''))
                         + 1 AS INTEGER) AS word_count
                FROM _tmp_partition
                WHERE turn_text IS NOT NULL AND CAST(turn_text AS VARCHAR) != ''
                """
            )
            next_row_id[0] += n_rows
            total_inserted += n_rows
            con.unregister("_tmp_partition")
        except Exception as e:
            logger.warning("Failed to insert from %s: %s", text_path, e)

    logger.info("Inserted %s rows, building indexes...", f"{total_inserted:,}")

    # Build standard indexes
    con.execute("CREATE INDEX idx_episode ON turns(episode_id)")
    con.execute("CREATE INDEX idx_podcast ON turns(podcast_id)")
    con.execute("CREATE INDEX idx_role ON turns(speaker_role)")

    # Install and load FTS extension, then build FTS index
    con.execute("INSTALL fts")
    con.execute("LOAD fts")
    con.execute(
        "PRAGMA create_fts_index('turns', 'row_id', 'turn_text', "
        "stemmer='english', stopwords='english')"
    )

    # Verify
    row_count = con.execute("SELECT COUNT(*) FROM turns").fetchone()[0]
    logger.info("DuckDB row count: %s", f"{row_count:,}")

    con.close()

    elapsed = time.time() - start
    logger.info("Phase 3 complete: %s in %.1fs", db_path, elapsed)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Build precomputed indexes for SPORC Parquet dataset"
    )
    parser.add_argument(
        "--data-dir",
        default=DEFAULT_DATA_DIR,
        help="Root directory of the Parquet layout (default: %(default)s)",
    )
    parser.add_argument(
        "--phase",
        type=int,
        choices=[1, 2, 3],
        default=None,
        help="Run only a specific phase (default: all phases)",
    )
    args = parser.parse_args()

    data_dir = args.data_dir
    if not os.path.isdir(data_dir):
        logger.error("Data directory not found: %s", data_dir)
        sys.exit(1)

    overall_start = time.time()

    if args.phase is None or args.phase == 1:
        build_speaker_name_index(data_dir)

    if args.phase is None or args.phase == 2:
        build_episode_and_turn_metrics(data_dir)

    if args.phase is None or args.phase == 3:
        build_duckdb_search(data_dir)

    elapsed = time.time() - overall_start
    logger.info("All phases complete in %.1fs", elapsed)


if __name__ == "__main__":
    main()
