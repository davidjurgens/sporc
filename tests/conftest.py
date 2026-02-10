"""
Shared fixtures for SPORC test suite.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock

from sporc.parquet_backend import ParquetBackend


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
    backend._speaker_index_df = None
    backend._episode_metrics_df = None
    backend._search_db_con = None
    backend._podcast_df = None
    backend._episode_df = None
    backend._num_podcasts = 0
    backend._num_episodes = 0
    return backend
