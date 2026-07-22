"""
Shared fixtures for SPORC test suite.
"""

import json
import os

import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from unittest.mock import MagicMock

from sporc.parquet_backend import ParquetBackend
from sporc.source import LocalDataSource

# Ids follow the real scheme: md5(rss)[:12] and md5(mp3)[:16].
PID_WITH_TURNS = "03b0f2a257fd"
PID_NO_TURNS = "b9c1d2e3f4a5"
EID_WITH_TURNS = "a1b2c3d4e5f60718"
EID_NO_TURNS = "1122334455667788"


def _episode_columns(episode_id, podcast_id, title):
    cols = {
        "episode_id": [episode_id],
        "podcast_id": [podcast_id],
        "ep_title": [title],
        "mp3_url": [f"http://example.com/{episode_id}.mp3"],
        "duration_seconds": [100.0],
        "host_predicted_names": [["Ira Glass"]],
        "guest_predicted_names": [["A Guest"]],
        "num_main_speakers": [2],
        "language": ["en"],
        "explicit": [0],
        "episode_date": ["1577836800000"],
        "overlap_prop_duration": [0.1],
        "avg_turn_duration": [5.0],
        "total_sp_labels": [2],
    }
    for i in range(1, 11):
        cols[f"category{i}"] = ["comedy" if i == 1 else None]
    return cols


@pytest.fixture
def tmp_parquet_layout(tmp_path):
    """
    A real (tiny) SPORC parquet layout on disk.

    Mirrors the published layout, including a podcast that has no turns
    partition and an episode that has one but no turns of its own, so tests can
    exercise the coverage gaps that exist in the real corpus.
    """
    root = tmp_path / "sporc_parquet"
    meta = root / "metadata"
    meta.mkdir(parents=True)

    pq.write_table(pa.table({
        "podcast_id": [PID_WITH_TURNS, PID_NO_TURNS],
        "rss_url": ["https://a.example.com/f.xml", "https://b.example.com/f.xml"],
        "pod_title": ["Turns Podcast", "No Turns Podcast"],
        "pod_description": ["d1", "d2"],
        "language": ["en", "en"],
        "explicit": [0, 0],
        "image_url": ["http://img/1", "http://img/2"],
        "itunes_author": ["A", "B"],
        "episode_count": [2, 1],
        "total_duration_seconds": [200.0, 100.0],
        "primary_category": ["comedy", "news"],
        "all_categories": [["comedy"], ["news"]],
        "host_names": [["Ira Glass"], ["Someone"]],
        "earliest_date": ["2020-01-01", "2020-01-01"],
        "latest_date": ["2020-01-02", "2020-01-02"],
    }), meta / "podcast_catalog.parquet")

    ep_rows = {}
    for cols in (
        _episode_columns(EID_WITH_TURNS, PID_WITH_TURNS, "Has Turns"),
        _episode_columns("cc00dd11ee22ff33", PID_WITH_TURNS, "Partition But No Turns"),
        _episode_columns(EID_NO_TURNS, PID_NO_TURNS, "No Partition"),
    ):
        for k, v in cols.items():
            ep_rows.setdefault(k, []).extend(v)
    pq.write_table(pa.table(ep_rows), meta / "episode_catalog.parquet")

    pq.write_table(pa.table({
        "category": ["comedy", "news"],
        "podcast_id": [PID_WITH_TURNS, PID_NO_TURNS],
    }), meta / "category_index.parquet")
    pq.write_table(pa.table({
        "hostname": ["a.example.com", "b.example.com"],
        "podcast_id": [PID_WITH_TURNS, PID_NO_TURNS],
    }), meta / "hostname_index.parquet")
    pq.write_table(pa.table({
        "name_normalized": ["ira glass"],
        "name_original": ["Ira Glass"],
        "role": ["host"],
        "episode_id": [EID_WITH_TURNS],
        "podcast_id": [PID_WITH_TURNS],
    }), meta / "speaker_name_index.parquet")

    # From dataset 1.1 the trees are packed: many podcasts per part file, one
    # row group each, located through metadata/shard_map.parquet. The fixture
    # writes two podcasts into a single part so that the row-group addressing is
    # actually exercised rather than degenerating to one podcast per file.
    shard_rows = []

    def _write_part(tree, directory, filename, per_podcast):
        """Write one part with a row group per podcast, recording the map."""
        d = root / directory
        d.mkdir(parents=True, exist_ok=True)
        writer = None
        try:
            for rg, (pid, table) in enumerate(per_podcast):
                if writer is None:
                    writer = pq.ParquetWriter(d / filename, table.schema)
                writer.write_table(table)
                shard_rows.append({
                    "podcast_id": pid, "tree": tree, "part": filename,
                    "row_group": rg, "num_rows": table.num_rows,
                })
        finally:
            if writer is not None:
                writer.close()

    episode_tables = []
    for pid, eids in ((PID_WITH_TURNS, [EID_WITH_TURNS, "cc00dd11ee22ff33"]),
                      (PID_NO_TURNS, [EID_NO_TURNS])):
        rows = {}
        for eid in eids:
            cols = _episode_columns(eid, pid, f"Episode {eid[:4]}")
            cols.update({
                "ep_description": ["desc"],
                "transcript": ["hello world"],
                "rss_url": ["https://a.example.com/f.xml"],
                "pod_title": ["Turns Podcast"],
                "pod_description": ["d1"],
                "neither_predicted_names": [[]],
                "main_ep_speakers": [["SPEAKER_00"]],
                "host_speaker_labels": ['{"Ira Glass": "SPEAKER_00"}'],
                "guest_speaker_labels": ["{}"],
                "overlap_prop_turn_count": [0.1],
                "image_url": ["http://img/1"],
                "episode_date_localized": ["2020-01-01"],
                "oldest_episode_date": ["2020-01-01"],
                "last_update": ["2020-01-01"],
                "created_on": ["2020-01-01"],
                "itunes_author": ["A"],
                "itunes_owner_name": ["Owner"],
                "host": ["h"],
            })
            for k, v in cols.items():
                rows.setdefault(k, []).extend(v)
        episode_tables.append((pid, pa.table(rows)))

    _write_part("episodes", "episodes", "part-000-000.parquet", episode_tables)

    # Only PID_WITH_TURNS has turns, and only for one of its two episodes --
    # exactly the shape of the real corpus, where a podcast present in the
    # catalog may be absent from the turns tree entirely.
    turns = pa.table({
        "episode_id": [EID_WITH_TURNS, EID_WITH_TURNS],
        "podcast_id": [PID_WITH_TURNS, PID_WITH_TURNS],
        "speaker": [["SPEAKER_00"], ["SPEAKER_01"]],
        "turn_text": ["hello world", "goodbye now"],
        "start_time": [0.0, 2.5],
        "end_time": [2.0, 4.0],
        "duration": [2.0, 1.5],
        "turn_count": [0, 1],
        # Aligned tokens, deliberately not equal to the word count: this is the
        # column the dataset used to call word_count, and the whole point of the
        # rename is that the two are different numbers.
        "token_count": [3, 3],
        "inferred_speaker_name": ["Ira Glass", "A Guest"],
        "inferred_speaker_role": ["host", "guest"],
        "speakers_recomputed": [True, True],
    })
    _write_part("turns_text", "turns/text", "part-000-000.parquet",
                [(PID_WITH_TURNS, turns)])

    acoustics = pa.table({
        "episode_id": [EID_WITH_TURNS, EID_WITH_TURNS],
        "podcast_id": [PID_WITH_TURNS, PID_WITH_TURNS],
        "turn_count": [0, 1],
        "mfcc1_sma3_mean": [1.0, 2.0],
        "mfcc1_sma3_stdev": [0.1, 0.2],
        "f0_semitone_from_27_5hz_sma3nz_mean": [30.0, 31.0],
        "f0_semitone_from_27_5hz_sma3nz_stdev": [1.0, 1.1],
    })
    _write_part("acoustics", "acoustics", "part-000-000.parquet",
                [(PID_WITH_TURNS, acoustics)])

    metrics = pa.table({
        "episode_id": [EID_WITH_TURNS, EID_WITH_TURNS],
        "turn_count": pa.array([0, 1], pa.int32()),
        "word_count": pa.array([2, 2], pa.int32()),
        "words_per_second": pa.array([1.0, 1.33], pa.float32()),
        "gap_from_prev": pa.array([None, 0.5], pa.float32()),
        "overlap_with_prev": pa.array([None, 0.0], pa.float32()),
        "discourse_marker_count": pa.array([0, 0], pa.int16()),
        "char_count": pa.array([11, 11], pa.int32()),
    })
    _write_part("turns_metrics", "turns/metrics", "part-000-000.parquet",
                [(PID_WITH_TURNS, metrics)])

    # One row group per tree, which is how the published map is written and how
    # the client finds its rows without scanning the others.
    smap = pa.table({
        "podcast_id": [r["podcast_id"] for r in shard_rows],
        "tree": [r["tree"] for r in shard_rows],
        "part": [r["part"] for r in shard_rows],
        "row_group": [r["row_group"] for r in shard_rows],
        "num_rows": [r["num_rows"] for r in shard_rows],
    })
    writer = pq.ParquetWriter(meta / "shard_map.parquet", smap.schema)
    try:
        import pyarrow.compute as pc
        for tree in sorted(set(smap.column("tree").to_pylist())):
            writer.write_table(smap.filter(pc.equal(smap.column("tree"), tree)))
    finally:
        writer.close()

    (root / "manifest.json").write_text(json.dumps({
        "version": "1.1",
        "record_counts": {"podcasts": 2, "episodes": 3},
    }))
    return str(root)


@pytest.fixture
def sample_speaker_index_df():
    """5-row DataFrame simulating speaker_name_index.parquet."""
    return pd.DataFrame(
        {
            "name_normalized": [
                "john smith",
                "john smith",
                "jane doe",
                "jane doe",
                "bob jones",
            ],
            "name_original": [
                "John Smith",
                "John Smith",
                "Jane Doe",
                "Jane Doe",
                "Bob Jones",
            ],
            "role": ["host", "host", "guest", "host", "guest"],
            "episode_id": ["ep1", "ep2", "ep1", "ep3", "ep3"],
            "podcast_id": ["pod1", "pod1", "pod1", "pod2", "pod2"],
        }
    )


@pytest.fixture
def sample_episode_metrics_df():
    """3-row DataFrame with episode metric columns."""
    return pd.DataFrame(
        {
            "episode_id": ["ep1", "ep2", "ep3"],
            "podcast_id": ["pod1", "pod1", "pod2"],
            "total_word_count": [3000, 5000, 8000],
            "total_turn_count": [50, 80, 120],
            "unique_speaker_count": [2, 3, 4],
            "avg_turn_duration": [5.0, 6.5, 7.2],
            "median_turn_duration": [4.5, 6.0, 7.0],
            "avg_words_per_second": [2.5, 3.0, 3.5],
            "host_word_count": [1500, 2500, 4000],
            "guest_word_count": [1500, 2500, 4000],
            "host_turn_proportion": [0.5, 0.4, 0.6],
            "host_word_proportion": [0.5, 0.5, 0.6],
            "avg_gap_duration": [0.5, 1.0, 1.5],
            "total_overlap_duration": [2.0, 3.5, 5.0],
            "discourse_marker_count": [30, 75, 160],
            "discourse_marker_rate": [10.0, 15.0, 20.0],
            "speaking_rate_host": [2.4, 2.8, 3.2],
            "speaking_rate_guest": [2.6, 3.2, 3.8],
        }
    )


@pytest.fixture
def sample_turn_dicts():
    """3-turn list of dicts simulating Parquet turn rows."""
    return [
        {
            "episode_id": "ep1",
            "podcast_id": "pod1",
            "turn_count": 0,
            "turn_text": "Hello and welcome to the show.",
            "start_time": 0.0,
            "end_time": 5.0,
            "duration": 5.0,
            "speaker_role": "host",
            "speaker_name": "John",
            "mp3_url": "https://example.com/ep1.mp3",
        },
        {
            "episode_id": "ep1",
            "podcast_id": "pod1",
            "turn_count": 1,
            "turn_text": "Thanks for having me on today.",
            "start_time": 5.5,
            "end_time": 9.0,
            "duration": 3.5,
            "speaker_role": "guest",
            "speaker_name": "Jane",
            "mp3_url": "https://example.com/ep1.mp3",
        },
        {
            "episode_id": "ep1",
            "podcast_id": "pod1",
            "turn_count": 2,
            "turn_text": "So um you know I think like this is really great.",
            "start_time": 9.5,
            "end_time": 15.0,
            "duration": 5.5,
            "speaker_role": "host",
            "speaker_name": "John",
            "mp3_url": "https://example.com/ep1.mp3",
        },
    ]


@pytest.fixture
def mock_duckdb_result():
    """Factory fixture returning a configurable MagicMock DuckDB result."""

    def _make(columns, rows):
        result = MagicMock()
        result.description = [(col,) for col in columns]
        result.fetchall.return_value = rows
        return result

    return _make


@pytest.fixture
def mock_duckdb_connection(mock_duckdb_result):
    """MagicMock DuckDB connection with .execute() pre-wired."""
    con = MagicMock()
    # Default empty result
    default_result = mock_duckdb_result([], [])
    con.execute.return_value = default_result
    return con


@pytest.fixture
def mock_parquet_backend(sample_speaker_index_df, sample_episode_metrics_df):
    """ParquetBackend with skipped __init__ and pre-set internal state."""
    backend = ParquetBackend.__new__(ParquetBackend)
    backend.data_dir = "/fake/data"
    backend._meta_dir = "/fake/data/metadata"
    backend._source = LocalDataSource("/fake/data")
    backend._speaker_index_df = None
    backend._episode_metrics_df = None
    backend._search_db_con = None
    backend._podcast_df = None
    backend._episode_df = None
    backend._num_podcasts = 0
    backend._num_episodes = 0
    backend._cache_validated = False
    backend._missing_turns_warned = set()
    backend._turn_partition_exists = {}
    backend._turn_episode_ids = {}
    backend._shard_map = None
    backend._has_text_db = False
    return backend
