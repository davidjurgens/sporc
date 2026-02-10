#!/usr/bin/env python3
"""
Convert SPORC JSONL.gz data to partitioned Parquet layout.

Reads from local HF cache at /shared/4/models/datasets--blitt--SPoRC/
and writes to /shared/6/projects/sporc/v1/.

Three phases:
  1. Episode pass: build podcast/episode catalogs + per-podcast episode files
  2. Turn pass: build per-podcast turn text + audio feature files
  3. Index pass: build category/hostname indexes + manifest
"""

import argparse
import gzip
import hashlib
import json
import logging
import os
import shutil
import sys
import tempfile
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_HF_CACHE = "/shared/4/models/datasets--blitt--SPoRC/snapshots/96b66034f70dee2bdb8cd1ad44bc3cffa3e0d922"
DEFAULT_OUTPUT_DIR = "/shared/6/projects/sporc/v1"

# Buffer size for turn pass: flush when a podcast accumulates this many turns
TURN_FLUSH_THRESHOLD = 50_000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def podcast_id_from_rss(rss_url: str) -> str:
    """Deterministic 12-char podcast id from RSS URL."""
    return hashlib.md5(rss_url.encode("utf-8")).hexdigest()[:12]


def episode_id_from_mp3(mp3_url: str) -> str:
    """Deterministic 16-char episode id from mp3 URL."""
    return hashlib.md5(mp3_url.encode("utf-8")).hexdigest()[:16]


def hostname_from_rss(rss_url: str) -> str:
    """Extract hostname from an RSS URL."""
    try:
        return urlparse(rss_url).hostname or ""
    except Exception:
        return ""


def safe_str(val, default=""):
    if val is None:
        return default
    return str(val).strip() if val else default


def safe_float(val, default=0.0):
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def safe_int(val, default=0):
    if val is None:
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def safe_list(val):
    """Convert a value to a list, handling SPORC sentinel strings."""
    if val is None:
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        if val in (
            "NO_HOST_PREDICTED",
            "NO_GUEST_PREDICTED",
            "NO_NEITHER_IDENTIFIED",
            "SPEAKER_DATA_UNAVAILABLE",
        ):
            return []
        if val.startswith("[") and val.endswith("]"):
            try:
                return json.loads(val)
            except json.JSONDecodeError:
                pass
        return [val] if val.strip() else []
    return list(val) if hasattr(val, "__iter__") else [val]


def stream_jsonl_gz(path: str):
    """Yield parsed dicts from a gzip JSONL file."""
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                logger.debug("Skipping invalid JSON on line %d", lineno)


# ---------------------------------------------------------------------------
# Phase 1 – Episode pass
# ---------------------------------------------------------------------------
def phase1_episodes(episode_file: str, output_dir: str):
    """
    Stream episode-level data. Produce:
      - metadata/podcast_catalog.parquet
      - metadata/episode_catalog.parquet
      - episodes/podcast_id=<id>/data.parquet  (per-podcast, includes transcript)
      - Return episode_id -> podcast_id mapping for Phase 2
    """
    logger.info("=== Phase 1: Episode pass ===")
    start = time.time()

    meta_dir = os.path.join(output_dir, "metadata")
    episodes_dir = os.path.join(output_dir, "episodes")
    os.makedirs(meta_dir, exist_ok=True)

    # Accumulators
    # podcast_id -> aggregated podcast info
    podcast_agg = {}  # pid -> dict
    # podcast_id -> list of episode rows (for per-podcast parquet)
    podcast_episodes_buf = defaultdict(list)
    # episode catalog rows
    episode_catalog_rows = []
    # mapping for Phase 2
    mp3url_to_pid = {}

    record_count = 0
    seen_mp3urls = set()

    pbar = tqdm(stream_jsonl_gz(episode_file), desc="Phase 1: Episodes", unit=" records",
                total=1_134_058, dynamic_ncols=True)
    for rec in pbar:
        record_count += 1
        if record_count % 50_000 == 0:
            pbar.set_postfix(episodes=len(seen_mp3urls), podcasts=len(podcast_agg))

        mp3url = safe_str(rec.get("mp3url"))
        rss_url = safe_str(rec.get("rssUrl"))
        if not mp3url or not rss_url:
            continue

        # Deduplicate by mp3url
        if mp3url in seen_mp3urls:
            continue
        seen_mp3urls.add(mp3url)

        pid = podcast_id_from_rss(rss_url)
        eid = episode_id_from_mp3(mp3url)
        mp3url_to_pid[mp3url] = pid

        # Collect categories
        cats = []
        for i in range(1, 11):
            c = safe_str(rec.get(f"category{i}"))
            if c:
                cats.append(c)

        host_names = safe_list(rec.get("hostPredictedNames"))
        guest_names = safe_list(rec.get("guestPredictedNames"))
        duration = safe_float(rec.get("durationSeconds"))

        # --- Aggregate podcast info ---
        if pid not in podcast_agg:
            podcast_agg[pid] = {
                "podcast_id": pid,
                "rss_url": rss_url,
                "pod_title": safe_str(rec.get("podTitle")),
                "pod_description": safe_str(rec.get("podDescription")),
                "language": safe_str(rec.get("language"), "en"),
                "explicit": safe_int(rec.get("explicit")),
                "image_url": safe_str(rec.get("imageUrl")),
                "itunes_author": safe_str(rec.get("itunesAuthor")),
                "episode_count": 0,
                "total_duration_seconds": 0.0,
                "all_categories": set(),
                "host_names": set(),
                "earliest_date": None,
                "latest_date": None,
            }

        pa_info = podcast_agg[pid]
        pa_info["episode_count"] += 1
        pa_info["total_duration_seconds"] += duration
        pa_info["all_categories"].update(cats)
        pa_info["host_names"].update(host_names)

        ep_date_raw = rec.get("episodeDateLocalized")
        if ep_date_raw is not None:
            try:
                ep_ts = float(ep_date_raw)
                ep_date_str = datetime.fromtimestamp(ep_ts / 1000).isoformat()
                if pa_info["earliest_date"] is None or ep_date_str < pa_info["earliest_date"]:
                    pa_info["earliest_date"] = ep_date_str
                if pa_info["latest_date"] is None or ep_date_str > pa_info["latest_date"]:
                    pa_info["latest_date"] = ep_date_str
            except (ValueError, TypeError, OSError):
                pass

        # --- Episode catalog row (no transcript) ---
        episode_catalog_rows.append({
            "episode_id": eid,
            "podcast_id": pid,
            "ep_title": safe_str(rec.get("epTitle")),
            "mp3_url": mp3url,
            "duration_seconds": duration,
            "category1": safe_str(rec.get("category1")),
            "category2": safe_str(rec.get("category2")),
            "category3": safe_str(rec.get("category3")),
            "category4": safe_str(rec.get("category4")),
            "category5": safe_str(rec.get("category5")),
            "category6": safe_str(rec.get("category6")),
            "category7": safe_str(rec.get("category7")),
            "category8": safe_str(rec.get("category8")),
            "category9": safe_str(rec.get("category9")),
            "category10": safe_str(rec.get("category10")),
            "host_predicted_names": host_names,
            "guest_predicted_names": guest_names,
            "num_main_speakers": safe_int(rec.get("numMainSpeakers")),
            "language": safe_str(rec.get("language"), "en"),
            "explicit": safe_int(rec.get("explicit")),
            "episode_date": safe_str(rec.get("episodeDateLocalized")),
            "overlap_prop_duration": safe_float(rec.get("overlapPropDuration")),
            "avg_turn_duration": safe_float(rec.get("avgTurnDuration")),
            "total_sp_labels": safe_int(rec.get("totalSpLabels")),
        })

        # --- Per-podcast episode data (includes transcript) ---
        podcast_episodes_buf[pid].append({
            "episode_id": eid,
            "podcast_id": pid,
            "ep_title": safe_str(rec.get("epTitle")),
            "ep_description": safe_str(rec.get("epDescription")),
            "mp3_url": mp3url,
            "duration_seconds": duration,
            "transcript": safe_str(rec.get("transcript")),
            "rss_url": rss_url,
            "pod_title": safe_str(rec.get("podTitle")),
            "pod_description": safe_str(rec.get("podDescription")),
            "category1": safe_str(rec.get("category1")),
            "category2": safe_str(rec.get("category2")),
            "category3": safe_str(rec.get("category3")),
            "category4": safe_str(rec.get("category4")),
            "category5": safe_str(rec.get("category5")),
            "category6": safe_str(rec.get("category6")),
            "category7": safe_str(rec.get("category7")),
            "category8": safe_str(rec.get("category8")),
            "category9": safe_str(rec.get("category9")),
            "category10": safe_str(rec.get("category10")),
            "host_predicted_names": host_names,
            "guest_predicted_names": guest_names,
            "neither_predicted_names": safe_list(rec.get("neitherPredictedNames")),
            "main_ep_speakers": safe_list(rec.get("mainEpSpeakers")),
            "host_speaker_labels": json.dumps(
                rec.get("hostSpeakerLabels")
                if isinstance(rec.get("hostSpeakerLabels"), dict)
                else {}
            ),
            "guest_speaker_labels": json.dumps(
                rec.get("guestSpeakerLabels")
                if isinstance(rec.get("guestSpeakerLabels"), dict)
                else {}
            ),
            "num_main_speakers": safe_int(rec.get("numMainSpeakers")),
            "overlap_prop_duration": safe_float(rec.get("overlapPropDuration")),
            "overlap_prop_turn_count": safe_float(rec.get("overlapPropTurnCount")),
            "avg_turn_duration": safe_float(rec.get("avgTurnDuration")),
            "total_sp_labels": safe_int(rec.get("totalSpLabels")),
            "language": safe_str(rec.get("language"), "en"),
            "explicit": safe_int(rec.get("explicit")),
            "image_url": safe_str(rec.get("imageUrl")),
            "episode_date_localized": safe_str(rec.get("episodeDateLocalized")),
            "oldest_episode_date": safe_str(rec.get("oldestEpisodeDate")),
            "last_update": safe_str(rec.get("lastUpdate")),
            "created_on": safe_str(rec.get("createdOn")),
            "itunes_author": safe_str(rec.get("itunesAuthor")),
            "itunes_owner_name": safe_str(rec.get("itunesOwnerName")),
            "host": safe_str(rec.get("host")),
        })

    logger.info("Phase 1: Finished reading %s records, %s unique episodes, %s podcasts",
                f"{record_count:,}", f"{len(seen_mp3urls):,}", f"{len(podcast_agg):,}")

    # --- Write per-podcast episode files ---
    logger.info("Writing per-podcast episode Parquet files...")
    for pid, rows in tqdm(podcast_episodes_buf.items(), desc="Writing episode partitions",
                          unit=" podcasts", dynamic_ncols=True):
        part_dir = os.path.join(episodes_dir, f"podcast_id={pid}")
        os.makedirs(part_dir, exist_ok=True)
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, os.path.join(part_dir, "data.parquet"), compression="zstd")
    logger.info("  Wrote %s per-podcast episode files", f"{len(podcast_episodes_buf):,}")

    # Free memory
    del podcast_episodes_buf

    # --- Write podcast catalog ---
    logger.info("Writing podcast catalog...")
    podcast_rows = []
    for pid, info in podcast_agg.items():
        cats_list = sorted(info["all_categories"])
        podcast_rows.append({
            "podcast_id": pid,
            "rss_url": info["rss_url"],
            "pod_title": info["pod_title"],
            "pod_description": info["pod_description"],
            "language": info["language"],
            "explicit": info["explicit"],
            "image_url": info["image_url"],
            "itunes_author": info["itunes_author"],
            "episode_count": info["episode_count"],
            "total_duration_seconds": info["total_duration_seconds"],
            "primary_category": cats_list[0] if cats_list else "",
            "all_categories": cats_list,
            "host_names": sorted(info["host_names"]),
            "earliest_date": info["earliest_date"] or "",
            "latest_date": info["latest_date"] or "",
        })

    table = pa.Table.from_pylist(podcast_rows)
    pq.write_table(table, os.path.join(meta_dir, "podcast_catalog.parquet"), compression="zstd")
    logger.info("  Podcast catalog: %s rows", f"{len(podcast_rows):,}")

    # --- Write episode catalog ---
    logger.info("Writing episode catalog...")
    table = pa.Table.from_pylist(episode_catalog_rows)
    pq.write_table(table, os.path.join(meta_dir, "episode_catalog.parquet"), compression="zstd")
    logger.info("  Episode catalog: %s rows", f"{len(episode_catalog_rows):,}")

    elapsed = time.time() - start
    logger.info("Phase 1 complete in %.1f seconds", elapsed)

    return mp3url_to_pid, podcast_agg


# ---------------------------------------------------------------------------
# Phase 2 – Turn pass
# ---------------------------------------------------------------------------
def phase2_turns(turn_file: str, output_dir: str, mp3url_to_pid: dict):
    """
    Stream speaker-turn data. Produce per-podcast:
      - turns/podcast_id=<id>/text.parquet
      - turns/podcast_id=<id>/audio_features.parquet
    """
    logger.info("=== Phase 2: Turn pass ===")
    start = time.time()

    turns_dir = os.path.join(output_dir, "turns")

    # Buffer: podcast_id -> { "text": [rows], "audio": [rows] }
    buffers = defaultdict(lambda: {"text": [], "audio": []})
    buffer_counts = defaultdict(int)
    flushed_pids = set()

    record_count = 0
    matched_count = 0
    unmatched_count = 0

    def flush_podcast(pid):
        """Write buffered turns for a podcast to Parquet."""
        buf = buffers[pid]
        part_dir = os.path.join(turns_dir, f"podcast_id={pid}")
        os.makedirs(part_dir, exist_ok=True)

        text_path = os.path.join(part_dir, "text.parquet")
        audio_path = os.path.join(part_dir, "audio_features.parquet")

        if buf["text"]:
            # Append if file already exists
            new_table = pa.Table.from_pylist(buf["text"])
            if os.path.exists(text_path):
                existing = pq.read_table(text_path)
                combined = pa.concat_tables([existing, new_table])
                pq.write_table(combined, text_path, compression="zstd")
            else:
                pq.write_table(new_table, text_path, compression="zstd")

        if buf["audio"]:
            new_table = pa.Table.from_pylist(buf["audio"])
            if os.path.exists(audio_path):
                existing = pq.read_table(audio_path)
                combined = pa.concat_tables([existing, new_table])
                pq.write_table(combined, audio_path, compression="zstd")
            else:
                pq.write_table(new_table, audio_path, compression="zstd")

        buf["text"].clear()
        buf["audio"].clear()
        buffer_counts[pid] = 0
        flushed_pids.add(pid)

    pbar = tqdm(stream_jsonl_gz(turn_file), desc="Phase 2: Turns", unit=" records",
                total=22_000_000, dynamic_ncols=True)
    for rec in pbar:
        record_count += 1
        if record_count % 100_000 == 0:
            pbar.set_postfix(matched=matched_count, unmatched=unmatched_count,
                             podcasts=len(buffers))

        mp3url = safe_str(rec.get("mp3url"))
        if not mp3url:
            continue

        pid = mp3url_to_pid.get(mp3url)
        if pid is None:
            unmatched_count += 1
            continue
        matched_count += 1

        eid = episode_id_from_mp3(mp3url)
        speaker = rec.get("speaker", [])
        if isinstance(speaker, str):
            speaker = [speaker]

        # Text row
        buffers[pid]["text"].append({
            "episode_id": eid,
            "podcast_id": pid,
            "mp3_url": mp3url,
            "speaker": speaker,
            "turn_text": safe_str(rec.get("turnText")),
            "start_time": safe_float(rec.get("startTime")),
            "end_time": safe_float(rec.get("endTime")),
            "duration": safe_float(rec.get("duration")),
            "turn_count": safe_int(rec.get("turnCount")),
            "inferred_speaker_role": safe_str(rec.get("inferredSpeakerRole")),
            "inferred_speaker_name": safe_str(rec.get("inferredSpeakerName")),
        })

        # Audio features row
        buffers[pid]["audio"].append({
            "episode_id": eid,
            "podcast_id": pid,
            "mp3_url": mp3url,
            "turn_count": safe_int(rec.get("turnCount")),
            "start_time": safe_float(rec.get("startTime")),
            "mfcc1_sma3_mean": safe_float(rec.get("mfcc1_sma3Mean")),
            "mfcc2_sma3_mean": safe_float(rec.get("mfcc2_sma3Mean")),
            "mfcc3_sma3_mean": safe_float(rec.get("mfcc3_sma3Mean")),
            "mfcc4_sma3_mean": safe_float(rec.get("mfcc4_sma3Mean")),
            "f0_semitone_from_27_5hz_sma3nz_mean": safe_float(rec.get("F0semitoneFrom27.5Hz_sma3nzMean")),
            "f1_frequency_sma3nz_mean": safe_float(rec.get("F1frequency_sma3nzMean")),
        })

        buffer_counts[pid] += 1
        if buffer_counts[pid] >= TURN_FLUSH_THRESHOLD:
            flush_podcast(pid)

    # Flush remaining buffers
    logger.info("Flushing remaining %s podcast buffers...", f"{len(buffers):,}")
    for pid in list(buffers.keys()):
        if buffers[pid]["text"] or buffers[pid]["audio"]:
            flush_podcast(pid)

    elapsed = time.time() - start
    logger.info("Phase 2 complete in %.1f seconds", elapsed)
    logger.info("  Total turn records: %s, matched: %s, unmatched: %s",
                f"{record_count:,}", f"{matched_count:,}", f"{unmatched_count:,}")
    logger.info("  Wrote turns for %s podcasts", f"{len(flushed_pids):,}")


# ---------------------------------------------------------------------------
# Phase 3 – Index pass
# ---------------------------------------------------------------------------
def phase3_indexes(output_dir: str, podcast_agg: dict):
    """
    Build category and hostname indexes from the podcast catalog.
    Write manifest.json.
    """
    logger.info("=== Phase 3: Index pass ===")
    start = time.time()

    meta_dir = os.path.join(output_dir, "metadata")

    # --- Category index ---
    logger.info("Building category index...")
    cat_rows = []
    for pid, info in podcast_agg.items():
        for cat in info["all_categories"]:
            cat_rows.append({"category": cat, "podcast_id": pid})

    table = pa.Table.from_pylist(cat_rows)
    pq.write_table(table, os.path.join(meta_dir, "category_index.parquet"), compression="zstd")
    logger.info("  Category index: %s rows", f"{len(cat_rows):,}")

    # --- Hostname index ---
    logger.info("Building hostname index...")
    host_rows = []
    for pid, info in podcast_agg.items():
        hn = hostname_from_rss(info["rss_url"])
        if hn:
            host_rows.append({"hostname": hn, "podcast_id": pid})

    table = pa.Table.from_pylist(host_rows)
    pq.write_table(table, os.path.join(meta_dir, "hostname_index.parquet"), compression="zstd")
    logger.info("  Hostname index: %s rows", f"{len(host_rows):,}")

    # --- Manifest ---
    logger.info("Writing manifest.json...")
    ep_catalog_path = os.path.join(meta_dir, "episode_catalog.parquet")
    ep_count = pq.read_metadata(ep_catalog_path).num_rows if os.path.exists(ep_catalog_path) else 0

    manifest = {
        "version": "1.0",
        "schema_version": 1,
        "creation_date": datetime.now().isoformat(),
        "source": "blitt/SPoRC (HuggingFace)",
        "record_counts": {
            "podcasts": len(podcast_agg),
            "episodes": ep_count,
            "category_index_rows": len(cat_rows),
            "hostname_index_rows": len(host_rows),
        },
        "compression": "zstd",
        "layout": {
            "metadata/podcast_catalog.parquet": "One row per podcast, aggregated stats",
            "metadata/episode_catalog.parquet": "One row per episode, key metadata only (no transcripts)",
            "metadata/category_index.parquet": "category -> podcast_id mapping",
            "metadata/hostname_index.parquet": "hostname -> podcast_id mapping",
            "episodes/podcast_id=<id>/data.parquet": "Full episode data including transcript",
            "turns/podcast_id=<id>/text.parquet": "Turn text, timing, speaker info",
            "turns/podcast_id=<id>/audio_features.parquet": "MFCCs, F0, formants",
        },
        "id_scheme": {
            "podcast_id": "md5(rssUrl)[:12]",
            "episode_id": "md5(mp3url)[:16]",
        },
    }

    with open(os.path.join(output_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)

    elapsed = time.time() - start
    logger.info("Phase 3 complete in %.1f seconds", elapsed)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Convert SPORC JSONL.gz to partitioned Parquet")
    parser.add_argument(
        "--input-dir",
        default=DEFAULT_HF_CACHE,
        help="Directory containing episodeLevelData.jsonl.gz and speakerTurnData.jsonl.gz",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Output directory for Parquet layout",
    )
    parser.add_argument(
        "--skip-turns",
        action="store_true",
        help="Skip Phase 2 (turn processing) for faster testing",
    )
    args = parser.parse_args()

    episode_file = os.path.join(args.input_dir, "episodeLevelData.jsonl.gz")
    turn_file = os.path.join(args.input_dir, "speakerTurnData.jsonl.gz")

    if not os.path.exists(episode_file):
        logger.error("Episode file not found: %s", episode_file)
        sys.exit(1)
    if not args.skip_turns and not os.path.exists(turn_file):
        logger.error("Turn file not found: %s", turn_file)
        sys.exit(1)

    # Write to a temp directory first, then rename atomically
    final_dir = args.output_dir
    tmp_dir = final_dir + ".tmp"

    if os.path.exists(tmp_dir):
        logger.info("Removing stale temp directory: %s", tmp_dir)
        shutil.rmtree(tmp_dir)

    os.makedirs(tmp_dir, exist_ok=True)

    overall_start = time.time()

    try:
        # Phase 1
        mp3url_to_pid, podcast_agg = phase1_episodes(episode_file, tmp_dir)

        # Phase 2
        if not args.skip_turns:
            phase2_turns(turn_file, tmp_dir, mp3url_to_pid)
        else:
            logger.info("Skipping Phase 2 (turns) as requested")

        # Phase 3
        phase3_indexes(tmp_dir, podcast_agg)

        # Atomic rename
        if os.path.exists(final_dir):
            backup = final_dir + ".old"
            if os.path.exists(backup):
                shutil.rmtree(backup)
            os.rename(final_dir, backup)
            logger.info("Moved existing output to %s", backup)

        os.rename(tmp_dir, final_dir)
        logger.info("Output directory: %s", final_dir)

    except Exception:
        logger.exception("Conversion failed")
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
        sys.exit(1)

    overall_elapsed = time.time() - overall_start
    logger.info("=== Conversion complete in %.1f seconds (%.1f minutes) ===", overall_elapsed, overall_elapsed / 60)


if __name__ == "__main__":
    main()
