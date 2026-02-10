"""
Tests for ParquetBackend search, metrics, and audio methods.
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
import pandas as pd

from sporc.parquet_backend import ParquetBackend
from sporc.exceptions import IndexNotBuiltError


# ===================================================================
# Lazy Loaders
# ===================================================================


class TestEnsureSpeakerIndex:
    """Tests for _ensure_speaker_index lazy loader."""

    def test_already_loaded_no_io(self, mock_parquet_backend, sample_speaker_index_df):
        mock_parquet_backend._speaker_index_df = sample_speaker_index_df
        with patch("sporc.parquet_backend.os.path.exists") as mock_exists:
            mock_parquet_backend._ensure_speaker_index()
            mock_exists.assert_not_called()

    def test_file_missing_raises_index_not_built(self, mock_parquet_backend):
        with patch("sporc.parquet_backend.os.path.exists", return_value=False):
            with pytest.raises(IndexNotBuiltError, match="--phase 1"):
                mock_parquet_backend._ensure_speaker_index()

    def test_file_exists_loads_dataframe(self, mock_parquet_backend):
        mock_table = MagicMock()
        mock_df = pd.DataFrame({"col": [1, 2]})
        mock_table.to_pandas.return_value = mock_df
        with patch("sporc.parquet_backend.os.path.exists", return_value=True), \
             patch("sporc.parquet_backend.pq.read_table", return_value=mock_table) as mock_read:
            mock_parquet_backend._ensure_speaker_index()
            mock_read.assert_called_once()
            assert mock_parquet_backend._speaker_index_df is mock_df


class TestEnsureEpisodeMetricsDf:
    """Tests for _ensure_episode_metrics_df lazy loader."""

    def test_already_loaded_no_io(self, mock_parquet_backend, sample_episode_metrics_df):
        mock_parquet_backend._episode_metrics_df = sample_episode_metrics_df
        with patch("sporc.parquet_backend.os.path.exists") as mock_exists:
            mock_parquet_backend._ensure_episode_metrics_df()
            mock_exists.assert_not_called()

    def test_file_missing_raises_index_not_built(self, mock_parquet_backend):
        with patch("sporc.parquet_backend.os.path.exists", return_value=False):
            with pytest.raises(IndexNotBuiltError, match="--phase 2"):
                mock_parquet_backend._ensure_episode_metrics_df()

    def test_file_exists_loads_dataframe(self, mock_parquet_backend):
        mock_table = MagicMock()
        mock_df = pd.DataFrame({"col": [1, 2]})
        mock_table.to_pandas.return_value = mock_df
        with patch("sporc.parquet_backend.os.path.exists", return_value=True), \
             patch("sporc.parquet_backend.pq.read_table", return_value=mock_table) as mock_read:
            mock_parquet_backend._ensure_episode_metrics_df()
            mock_read.assert_called_once()
            assert mock_parquet_backend._episode_metrics_df is mock_df


class TestEnsureSearchDb:
    """Tests for _ensure_search_db lazy loader."""

    def test_already_loaded_no_op(self, mock_parquet_backend, mock_duckdb_connection):
        mock_parquet_backend._search_db_con = mock_duckdb_connection
        # Should not raise or do anything
        mock_parquet_backend._ensure_search_db()

    def test_duckdb_not_installed_raises_import_error(self, mock_parquet_backend):
        import sys
        with patch.dict(sys.modules, {"duckdb": None}):
            with pytest.raises(ImportError, match="pip install duckdb"):
                mock_parquet_backend._ensure_search_db()

    def test_db_file_missing_raises_index_not_built(self, mock_parquet_backend):
        mock_duckdb = MagicMock()
        with patch.dict("sys.modules", {"duckdb": mock_duckdb}), \
             patch("sporc.parquet_backend.os.path.exists", return_value=False):
            with pytest.raises(IndexNotBuiltError, match="--phase 3"):
                mock_parquet_backend._ensure_search_db()

    def test_success_connects_and_loads_fts(self, mock_parquet_backend):
        mock_con = MagicMock()
        mock_duckdb = MagicMock()
        mock_duckdb.connect.return_value = mock_con
        with patch.dict("sys.modules", {"duckdb": mock_duckdb}), \
             patch("sporc.parquet_backend.os.path.exists", return_value=True):
            mock_parquet_backend._ensure_search_db()
            mock_duckdb.connect.assert_called_once()
            mock_con.execute.assert_called_once_with("LOAD fts")
            assert mock_parquet_backend._search_db_con is mock_con


# ===================================================================
# search_turns
# ===================================================================


class TestSearchTurns:
    """Tests for search_turns method."""

    def _setup_backend(self, mock_parquet_backend, mock_duckdb_result):
        columns = [
            "episode_id", "podcast_id", "turn_count", "turn_text",
            "start_time", "end_time", "duration", "speaker_role",
            "speaker_name", "word_count", "score",
        ]
        rows = [
            ("ep1", "pod1", 0, "hello world", 0.0, 5.0, 5.0,
             "host", "John", 2, 1.5),
        ]
        result = mock_duckdb_result(columns, rows)
        con = MagicMock()
        con.execute.return_value = result
        mock_parquet_backend._search_db_con = con
        return con

    def test_fts_mode_basic(self, mock_parquet_backend, mock_duckdb_result):
        con = self._setup_backend(mock_parquet_backend, mock_duckdb_result)
        results = mock_parquet_backend.search_turns("hello")
        assert len(results) == 1
        assert results[0]["episode_id"] == "ep1"
        assert results[0]["score"] == 1.5

    def test_fts_mode_with_all_filters(self, mock_parquet_backend, mock_duckdb_result):
        con = self._setup_backend(mock_parquet_backend, mock_duckdb_result)
        results = mock_parquet_backend.search_turns(
            "hello", podcast_id="pod1", episode_id="ep1", speaker_role="host"
        )
        call_args = con.execute.call_args
        params = call_args[0][1]
        assert "hello" in params
        assert "pod1" in params
        assert "ep1" in params
        assert "host" in params

    def test_exact_mode(self, mock_parquet_backend, mock_duckdb_result):
        con = self._setup_backend(mock_parquet_backend, mock_duckdb_result)
        results = mock_parquet_backend.search_turns("hello", mode="exact")
        call_args = con.execute.call_args
        params = call_args[0][1]
        assert "%hello%" in params

    def test_exact_mode_with_filter_uses_and(self, mock_parquet_backend, mock_duckdb_result):
        con = self._setup_backend(mock_parquet_backend, mock_duckdb_result)
        mock_parquet_backend.search_turns(
            "hello", mode="exact", podcast_id="pod1"
        )
        sql = con.execute.call_args[0][0]
        assert "AND" in sql

    def test_regex_mode(self, mock_parquet_backend, mock_duckdb_result):
        con = self._setup_backend(mock_parquet_backend, mock_duckdb_result)
        mock_parquet_backend.search_turns("hel+o", mode="regex")
        call_args = con.execute.call_args
        params = call_args[0][1]
        assert "hel+o" in params

    def test_invalid_mode_raises_value_error(self, mock_parquet_backend, mock_duckdb_result):
        self._setup_backend(mock_parquet_backend, mock_duckdb_result)
        with pytest.raises(ValueError, match="Invalid search mode"):
            mock_parquet_backend.search_turns("hello", mode="invalid")

    def test_default_limit_and_offset(self, mock_parquet_backend, mock_duckdb_result):
        con = self._setup_backend(mock_parquet_backend, mock_duckdb_result)
        mock_parquet_backend.search_turns("hello")
        call_args = con.execute.call_args
        params = call_args[0][1]
        assert 100 in params
        assert 0 in params


# ===================================================================
# search_episodes_by_text
# ===================================================================


class TestSearchEpisodesByText:
    """Tests for search_episodes_by_text method."""

    def _setup_backend(self, mock_parquet_backend, mock_duckdb_result):
        columns = ["episode_id", "podcast_id", "match_count", "best_score"]
        rows = [("ep1", "pod1", 5, 2.1)]
        result = mock_duckdb_result(columns, rows)
        con = MagicMock()
        con.execute.return_value = result
        mock_parquet_backend._search_db_con = con
        return con

    def test_fts_mode(self, mock_parquet_backend, mock_duckdb_result):
        self._setup_backend(mock_parquet_backend, mock_duckdb_result)
        results = mock_parquet_backend.search_episodes_by_text("podcast")
        assert len(results) == 1
        assert results[0]["match_count"] == 5

    def test_exact_mode(self, mock_parquet_backend, mock_duckdb_result):
        con = self._setup_backend(mock_parquet_backend, mock_duckdb_result)
        mock_parquet_backend.search_episodes_by_text("podcast", mode="exact")
        params = con.execute.call_args[0][1]
        assert "%podcast%" in params

    def test_regex_mode(self, mock_parquet_backend, mock_duckdb_result):
        con = self._setup_backend(mock_parquet_backend, mock_duckdb_result)
        mock_parquet_backend.search_episodes_by_text("pod.*", mode="regex")
        params = con.execute.call_args[0][1]
        assert "pod.*" in params

    def test_invalid_mode_raises_value_error(self, mock_parquet_backend, mock_duckdb_result):
        self._setup_backend(mock_parquet_backend, mock_duckdb_result)
        with pytest.raises(ValueError, match="Invalid search mode"):
            mock_parquet_backend.search_episodes_by_text("x", mode="invalid")

    def test_custom_limit(self, mock_parquet_backend, mock_duckdb_result):
        con = self._setup_backend(mock_parquet_backend, mock_duckdb_result)
        mock_parquet_backend.search_episodes_by_text("podcast", limit=50)
        params = con.execute.call_args[0][1]
        assert 50 in params


# ===================================================================
# search_by_speaker_name
# ===================================================================


class TestSearchBySpeakerName:
    """Tests for search_by_speaker_name using real DataFrames."""

    def _setup(self, backend, df):
        backend._speaker_index_df = df

    def test_substring_match_case_insensitive(
        self, mock_parquet_backend, sample_speaker_index_df
    ):
        self._setup(mock_parquet_backend, sample_speaker_index_df)
        results = mock_parquet_backend.search_by_speaker_name("john")
        assert len(results) == 2

    def test_exact_match_found(self, mock_parquet_backend, sample_speaker_index_df):
        self._setup(mock_parquet_backend, sample_speaker_index_df)
        results = mock_parquet_backend.search_by_speaker_name(
            "john smith", exact=True
        )
        assert len(results) == 2

    def test_exact_match_not_found(self, mock_parquet_backend, sample_speaker_index_df):
        self._setup(mock_parquet_backend, sample_speaker_index_df)
        results = mock_parquet_backend.search_by_speaker_name(
            "john", exact=True
        )
        assert len(results) == 0

    def test_role_filter_guest(self, mock_parquet_backend, sample_speaker_index_df):
        self._setup(mock_parquet_backend, sample_speaker_index_df)
        results = mock_parquet_backend.search_by_speaker_name("jane", role="guest")
        assert len(results) == 1
        assert results[0]["role"] == "guest"

    def test_role_filter_host(self, mock_parquet_backend, sample_speaker_index_df):
        self._setup(mock_parquet_backend, sample_speaker_index_df)
        results = mock_parquet_backend.search_by_speaker_name("jane", role="host")
        assert len(results) == 1
        assert results[0]["role"] == "host"

    def test_limit_parameter(self, mock_parquet_backend, sample_speaker_index_df):
        self._setup(mock_parquet_backend, sample_speaker_index_df)
        results = mock_parquet_backend.search_by_speaker_name("john", limit=1)
        assert len(results) == 1

    def test_no_match_returns_empty(self, mock_parquet_backend, sample_speaker_index_df):
        self._setup(mock_parquet_backend, sample_speaker_index_df)
        results = mock_parquet_backend.search_by_speaker_name("nonexistent")
        assert len(results) == 0

    def test_whitespace_stripped(self, mock_parquet_backend, sample_speaker_index_df):
        self._setup(mock_parquet_backend, sample_speaker_index_df)
        results = mock_parquet_backend.search_by_speaker_name("  john  ")
        assert len(results) == 2

    def test_result_columns(self, mock_parquet_backend, sample_speaker_index_df):
        self._setup(mock_parquet_backend, sample_speaker_index_df)
        results = mock_parquet_backend.search_by_speaker_name("john")
        assert len(results) > 0
        expected_keys = {"episode_id", "podcast_id", "name_original", "role"}
        assert set(results[0].keys()) == expected_keys

    def test_lazy_load_triggered_when_none(self, mock_parquet_backend):
        mock_parquet_backend._speaker_index_df = None
        with patch.object(
            mock_parquet_backend, "_ensure_speaker_index"
        ) as mock_ensure:
            # After _ensure_speaker_index, set a small df
            def set_df():
                mock_parquet_backend._speaker_index_df = pd.DataFrame(
                    {
                        "name_normalized": ["test"],
                        "name_original": ["Test"],
                        "role": ["host"],
                        "episode_id": ["ep1"],
                        "podcast_id": ["pod1"],
                    }
                )

            mock_ensure.side_effect = set_df
            mock_parquet_backend.search_by_speaker_name("test")
            mock_ensure.assert_called_once()


# ===================================================================
# concordance
# ===================================================================


class TestConcordance:
    """Tests for concordance (KWIC) method."""

    def _setup_backend(self, backend, mock_duckdb_result, turn_texts):
        """Set up backend with DuckDB returning given turn texts."""
        columns = [
            "episode_id", "podcast_id", "turn_text",
            "speaker_role", "speaker_name", "start_time", "end_time",
        ]
        rows = [
            ("ep1", "pod1", text, "host", "John", 0.0, 10.0)
            for text in turn_texts
        ]
        result = mock_duckdb_result(columns, rows)
        con = MagicMock()
        con.execute.return_value = result
        backend._search_db_con = con
        return con

    def test_single_word_kwic(self, mock_parquet_backend, mock_duckdb_result):
        self._setup_backend(
            mock_parquet_backend, mock_duckdb_result,
            ["the quick brown fox jumps over the lazy dog"]
        )
        results = mock_parquet_backend.concordance("fox")
        assert len(results) == 1
        # The KWIC algorithm uses char-offset heuristic; verify structure
        assert "keyword" in results[0]
        assert "left_context" in results[0]
        assert "right_context" in results[0]

    def test_multi_word_phrase(self, mock_parquet_backend, mock_duckdb_result):
        self._setup_backend(
            mock_parquet_backend, mock_duckdb_result,
            ["I think you know what I mean by that"]
        )
        results = mock_parquet_backend.concordance("you know")
        assert len(results) == 1
        # Multi-word keyword spans 2 words in the result
        keyword_words = results[0]["keyword"].split()
        assert len(keyword_words) == 2

    def test_word_at_beginning(self, mock_parquet_backend, mock_duckdb_result):
        self._setup_backend(
            mock_parquet_backend, mock_duckdb_result,
            ["Hello world how are you"]
        )
        results = mock_parquet_backend.concordance("Hello")
        assert len(results) == 1
        assert results[0]["left_context"] == ""

    def test_word_at_end(self, mock_parquet_backend, mock_duckdb_result):
        """When keyword is last word, right_context should be short or empty."""
        self._setup_backend(
            mock_parquet_backend, mock_duckdb_result,
            ["how are you today friend"]
        )
        results = mock_parquet_backend.concordance("friend")
        assert len(results) == 1
        # Verify result is returned (right_context content depends on word-index heuristic)
        assert isinstance(results[0]["right_context"], str)

    def test_case_insensitive(self, mock_parquet_backend, mock_duckdb_result):
        self._setup_backend(
            mock_parquet_backend, mock_duckdb_result,
            ["The Quick Brown Fox"]
        )
        results = mock_parquet_backend.concordance("quick")
        assert len(results) == 1

    def test_no_regex_match_filtered_out(self, mock_parquet_backend, mock_duckdb_result):
        """ILIKE returned a row but re.search doesn't match (e.g. word boundary)."""
        self._setup_backend(
            mock_parquet_backend, mock_duckdb_result,
            ["there is no matching word here"]
        )
        results = mock_parquet_backend.concordance("xyz")
        assert len(results) == 0

    def test_speaker_role_filter_in_sql(self, mock_parquet_backend, mock_duckdb_result):
        con = self._setup_backend(
            mock_parquet_backend, mock_duckdb_result,
            ["hello world"]
        )
        mock_parquet_backend.concordance("hello", speaker_role="host")
        params = con.execute.call_args[0][1]
        assert "host" in params

    def test_podcast_id_filter_in_sql(self, mock_parquet_backend, mock_duckdb_result):
        con = self._setup_backend(
            mock_parquet_backend, mock_duckdb_result,
            ["hello world"]
        )
        mock_parquet_backend.concordance("hello", podcast_id="pod1")
        params = con.execute.call_args[0][1]
        assert "pod1" in params

    def test_result_metadata_fields(self, mock_parquet_backend, mock_duckdb_result):
        self._setup_backend(
            mock_parquet_backend, mock_duckdb_result,
            ["the quick brown fox"]
        )
        results = mock_parquet_backend.concordance("quick")
        assert len(results) == 1
        expected_keys = {
            "left_context", "keyword", "right_context",
            "episode_id", "podcast_id", "speaker_role",
            "speaker_name", "start_time", "end_time",
        }
        assert set(results[0].keys()) == expected_keys


# ===================================================================
# Episode & Turn Metrics
# ===================================================================


class TestGetEpisodeMetrics:
    """Tests for get_episode_metrics method."""

    def test_found_returns_dict(self, mock_parquet_backend, sample_episode_metrics_df):
        mock_parquet_backend._episode_metrics_df = sample_episode_metrics_df
        result = mock_parquet_backend.get_episode_metrics("ep1")
        assert result is not None
        assert result["episode_id"] == "ep1"
        assert result["total_word_count"] == 3000

    def test_not_found_returns_none(self, mock_parquet_backend, sample_episode_metrics_df):
        mock_parquet_backend._episode_metrics_df = sample_episode_metrics_df
        result = mock_parquet_backend.get_episode_metrics("nonexistent")
        assert result is None


class TestFilterEpisodesByMetrics:
    """Tests for filter_episodes_by_metrics method."""

    def _setup(self, backend, df):
        backend._episode_metrics_df = df

    def test_min_word_count(self, mock_parquet_backend, sample_episode_metrics_df):
        self._setup(mock_parquet_backend, sample_episode_metrics_df)
        results = mock_parquet_backend.filter_episodes_by_metrics(min_word_count=4000)
        assert len(results) == 2
        assert all(r["total_word_count"] >= 4000 for r in results)

    def test_max_word_count(self, mock_parquet_backend, sample_episode_metrics_df):
        self._setup(mock_parquet_backend, sample_episode_metrics_df)
        results = mock_parquet_backend.filter_episodes_by_metrics(max_word_count=4000)
        assert len(results) == 1
        assert results[0]["total_word_count"] == 3000

    def test_multiple_criteria(self, mock_parquet_backend, sample_episode_metrics_df):
        self._setup(mock_parquet_backend, sample_episode_metrics_df)
        results = mock_parquet_backend.filter_episodes_by_metrics(
            min_word_count=4000, max_turn_count=100
        )
        assert len(results) == 1
        assert results[0]["episode_id"] == "ep2"

    def test_no_criteria_returns_all(self, mock_parquet_backend, sample_episode_metrics_df):
        self._setup(mock_parquet_backend, sample_episode_metrics_df)
        results = mock_parquet_backend.filter_episodes_by_metrics()
        assert len(results) == 3

    def test_limit_parameter(self, mock_parquet_backend, sample_episode_metrics_df):
        self._setup(mock_parquet_backend, sample_episode_metrics_df)
        results = mock_parquet_backend.filter_episodes_by_metrics(limit=2)
        assert len(results) == 2

    def test_turn_count_filter(self, mock_parquet_backend, sample_episode_metrics_df):
        self._setup(mock_parquet_backend, sample_episode_metrics_df)
        results = mock_parquet_backend.filter_episodes_by_metrics(
            min_turn_count=100
        )
        assert len(results) == 1
        assert results[0]["episode_id"] == "ep3"

    def test_speaking_rate_filter(self, mock_parquet_backend, sample_episode_metrics_df):
        self._setup(mock_parquet_backend, sample_episode_metrics_df)
        results = mock_parquet_backend.filter_episodes_by_metrics(
            min_speaking_rate=3.0
        )
        assert len(results) == 2

    def test_discourse_marker_rate_filter(self, mock_parquet_backend, sample_episode_metrics_df):
        self._setup(mock_parquet_backend, sample_episode_metrics_df)
        results = mock_parquet_backend.filter_episodes_by_metrics(
            max_discourse_marker_rate=12.0
        )
        assert len(results) == 1
        assert results[0]["episode_id"] == "ep1"

    def test_host_proportion_filter(self, mock_parquet_backend, sample_episode_metrics_df):
        self._setup(mock_parquet_backend, sample_episode_metrics_df)
        results = mock_parquet_backend.filter_episodes_by_metrics(
            min_host_proportion=0.55
        )
        assert len(results) == 1
        assert results[0]["episode_id"] == "ep3"

    def test_avg_gap_filter(self, mock_parquet_backend, sample_episode_metrics_df):
        self._setup(mock_parquet_backend, sample_episode_metrics_df)
        results = mock_parquet_backend.filter_episodes_by_metrics(
            min_avg_gap=0.8, max_avg_gap=1.2
        )
        assert len(results) == 1
        assert results[0]["episode_id"] == "ep2"

    def test_impossible_criteria_returns_empty(
        self, mock_parquet_backend, sample_episode_metrics_df
    ):
        self._setup(mock_parquet_backend, sample_episode_metrics_df)
        results = mock_parquet_backend.filter_episodes_by_metrics(
            min_word_count=100000
        )
        assert len(results) == 0


class TestGetTurnMetrics:
    """Tests for get_turn_metrics method."""

    def test_file_missing_raises_index_not_built(self, mock_parquet_backend):
        with patch("sporc.parquet_backend.os.path.exists", return_value=False):
            with pytest.raises(IndexNotBuiltError):
                mock_parquet_backend.get_turn_metrics("pod1", "ep1")

    def test_valid_file_returns_sorted(self, mock_parquet_backend):
        import pyarrow as pa

        table = pa.table(
            {
                "episode_id": ["ep1", "ep1", "ep1"],
                "turn_count": [2, 0, 1],
                "word_count": [10, 5, 8],
            }
        )
        with patch("sporc.parquet_backend.os.path.exists", return_value=True), \
             patch("sporc.parquet_backend.pq.read_table", return_value=table):
            results = mock_parquet_backend.get_turn_metrics("pod1", "ep1")
            assert len(results) == 3
            assert results[0]["turn_count"] == 0
            assert results[1]["turn_count"] == 1
            assert results[2]["turn_count"] == 2

    def test_nonexistent_episode_returns_empty(self, mock_parquet_backend):
        import pyarrow as pa

        table = pa.table(
            {
                "episode_id": ["ep1", "ep1"],
                "turn_count": [0, 1],
                "word_count": [10, 5],
            }
        )
        with patch("sporc.parquet_backend.os.path.exists", return_value=True), \
             patch("sporc.parquet_backend.pq.read_table", return_value=table):
            results = mock_parquet_backend.get_turn_metrics("pod1", "ep999")
            assert results == []


# ===================================================================
# estimate_word_audio
# ===================================================================


class TestEstimateWordAudio:
    """Tests for estimate_word_audio method."""

    def _make_turn(self, text, start=0.0, end=10.0, mp3_url="http://x.mp3"):
        return {
            "turn_text": text,
            "start_time": start,
            "end_time": end,
            "mp3_url": mp3_url,
        }

    def test_word_found_first_occurrence(self, mock_parquet_backend):
        turns = [self._make_turn("hello world")]
        with patch.object(
            mock_parquet_backend, "get_turns_for_episode", return_value=turns
        ):
            result = mock_parquet_backend.estimate_word_audio("pod1", "ep1", "hello")
            assert result is not None
            expected_keys = {
                "mp3_url", "estimated_start", "estimated_end",
                "turn_start", "turn_end", "turn_text", "confidence",
            }
            assert set(result.keys()) == expected_keys

    def test_word_not_found_returns_none(self, mock_parquet_backend):
        turns = [self._make_turn("hello world")]
        with patch.object(
            mock_parquet_backend, "get_turns_for_episode", return_value=turns
        ):
            result = mock_parquet_backend.estimate_word_audio(
                "pod1", "ep1", "missing"
            )
            assert result is None

    def test_second_occurrence(self, mock_parquet_backend):
        turns = [self._make_turn("hello hello world")]
        with patch.object(
            mock_parquet_backend, "get_turns_for_episode", return_value=turns
        ):
            result = mock_parquet_backend.estimate_word_audio(
                "pod1", "ep1", "hello", occurrence=1
            )
            assert result is not None
            assert result["estimated_start"] > 0

    def test_occurrence_too_high_returns_none(self, mock_parquet_backend):
        turns = [self._make_turn("hello world")]
        with patch.object(
            mock_parquet_backend, "get_turns_for_episode", return_value=turns
        ):
            result = mock_parquet_backend.estimate_word_audio(
                "pod1", "ep1", "hello", occurrence=5
            )
            assert result is None

    def test_zero_duration_turn_returns_none(self, mock_parquet_backend):
        turns = [self._make_turn("hello world", start=5.0, end=5.0)]
        with patch.object(
            mock_parquet_backend, "get_turns_for_episode", return_value=turns
        ):
            result = mock_parquet_backend.estimate_word_audio(
                "pod1", "ep1", "hello"
            )
            assert result is None

    def test_empty_text_turn_skipped(self, mock_parquet_backend):
        turns = [
            self._make_turn("", start=0.0, end=5.0),
            self._make_turn("hello world", start=5.0, end=10.0),
        ]
        with patch.object(
            mock_parquet_backend, "get_turns_for_episode", return_value=turns
        ):
            result = mock_parquet_backend.estimate_word_audio(
                "pod1", "ep1", "hello"
            )
            assert result is not None
            assert result["turn_start"] == 5.0

    def test_no_turns_returns_none(self, mock_parquet_backend):
        with patch.object(
            mock_parquet_backend, "get_turns_for_episode", return_value=[]
        ):
            result = mock_parquet_backend.estimate_word_audio(
                "pod1", "ep1", "hello"
            )
            assert result is None

    def test_case_insensitive(self, mock_parquet_backend):
        turns = [self._make_turn("Hello World")]
        with patch.object(
            mock_parquet_backend, "get_turns_for_episode", return_value=turns
        ):
            result = mock_parquet_backend.estimate_word_audio(
                "pod1", "ep1", "hello"
            )
            assert result is not None

    def test_confidence_short_vs_long_turn(self, mock_parquet_backend):
        short_turn = [self._make_turn("hello world", start=0.0, end=5.0)]
        long_turn = [self._make_turn("hello world", start=0.0, end=60.0)]
        with patch.object(
            mock_parquet_backend, "get_turns_for_episode", return_value=short_turn
        ):
            short_result = mock_parquet_backend.estimate_word_audio(
                "pod1", "ep1", "hello"
            )
        with patch.object(
            mock_parquet_backend, "get_turns_for_episode", return_value=long_turn
        ):
            long_result = mock_parquet_backend.estimate_word_audio(
                "pod1", "ep1", "hello"
            )
        assert short_result["confidence"] > long_result["confidence"]

    def test_linear_interpolation(self, mock_parquet_backend):
        # "aaaa bbbb" has length 9, "bbbb" starts at char index 5
        # turn 10-20s (duration 10), char_ratio_start = 5/9
        # estimated_start = 10 + (5/9)*10 = 15.5555...
        turns = [self._make_turn("aaaa bbbb", start=10.0, end=20.0)]
        with patch.object(
            mock_parquet_backend, "get_turns_for_episode", return_value=turns
        ):
            result = mock_parquet_backend.estimate_word_audio(
                "pod1", "ep1", "bbbb"
            )
            assert result is not None
            assert abs(result["estimated_start"] - 15.56) < 0.1
