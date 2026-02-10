"""
Tests for SPORCDataset parquet-mode wrappers and mode gating.
"""

import pytest
from unittest.mock import MagicMock

from sporc.dataset import SPORCDataset
from sporc.exceptions import SPORCError, IndexNotBuiltError


def _make_dataset(parquet_mode=True):
    """Create a SPORCDataset instance with skipped __init__."""
    ds = SPORCDataset.__new__(SPORCDataset)
    ds._parquet_mode = parquet_mode
    ds._parquet_backend = MagicMock()
    ds.streaming = False
    ds._selective_mode = False
    ds._local_mode = False
    ds._episodes = []
    return ds


# ===================================================================
# _require_parquet_mode
# ===================================================================


class TestRequireParquetMode:
    """Tests for _require_parquet_mode gate."""

    def test_not_parquet_mode_raises(self):
        ds = _make_dataset(parquet_mode=False)
        with pytest.raises(SPORCError, match="requires parquet mode"):
            ds._require_parquet_mode("search_turns")

    def test_parquet_mode_passes(self):
        ds = _make_dataset(parquet_mode=True)
        ds._require_parquet_mode("search_turns")

    def test_error_message_includes_method_name(self):
        ds = _make_dataset(parquet_mode=False)
        with pytest.raises(SPORCError, match="search_turns"):
            ds._require_parquet_mode("search_turns")


# ===================================================================
# Wrapper Delegation
# ===================================================================


class TestWrapperDelegation:
    """Tests that each wrapper delegates to the backend with exact args."""

    def test_search_turns_delegates(self):
        ds = _make_dataset()
        ds.search_turns("hello", mode="exact", podcast_id="p1",
                        episode_id="e1", speaker_role="host",
                        limit=50, offset=10)
        ds._parquet_backend.search_turns.assert_called_once_with(
            "hello", mode="exact", podcast_id="p1",
            episode_id="e1", speaker_role="host",
            limit=50, offset=10,
        )

    def test_search_episodes_by_text_delegates(self):
        ds = _make_dataset()
        ds.search_episodes_by_text("query", mode="regex", limit=25)
        ds._parquet_backend.search_episodes_by_text.assert_called_once_with(
            "query", mode="regex", limit=25,
        )

    def test_search_by_speaker_name_delegates(self):
        ds = _make_dataset()
        ds.search_by_speaker_name("John", role="host", exact=True, limit=10)
        ds._parquet_backend.search_by_speaker_name.assert_called_once_with(
            "John", role="host", exact=True, limit=10,
        )

    def test_concordance_delegates(self):
        ds = _make_dataset()
        ds.concordance("um", context_words=5, speaker_role="guest",
                       podcast_id="p1", limit=50)
        ds._parquet_backend.concordance.assert_called_once_with(
            "um", context_words=5, speaker_role="guest",
            podcast_id="p1", limit=50,
        )

    def test_get_episode_metrics_delegates(self):
        ds = _make_dataset()
        ds.get_episode_metrics("ep1")
        ds._parquet_backend.get_episode_metrics.assert_called_once_with("ep1")

    def test_filter_episodes_by_metrics_delegates(self):
        ds = _make_dataset()
        ds.filter_episodes_by_metrics(min_word_count=100, max_turn_count=50)
        ds._parquet_backend.filter_episodes_by_metrics.assert_called_once_with(
            min_word_count=100, max_turn_count=50,
        )

    def test_get_turn_metrics_delegates(self):
        ds = _make_dataset()
        ds.get_turn_metrics("pod1", "ep1")
        ds._parquet_backend.get_turn_metrics.assert_called_once_with(
            "pod1", "ep1"
        )

    def test_estimate_word_audio_delegates(self):
        ds = _make_dataset()
        ds.estimate_word_audio("pod1", "ep1", "hello", occurrence=2)
        ds._parquet_backend.estimate_word_audio.assert_called_once_with(
            "pod1", "ep1", "hello", occurrence=2,
        )

    def test_get_episode_metrics_none_propagation(self):
        ds = _make_dataset()
        ds._parquet_backend.get_episode_metrics.return_value = None
        result = ds.get_episode_metrics("nonexistent")
        assert result is None

    def test_estimate_word_audio_none_propagation(self):
        ds = _make_dataset()
        ds._parquet_backend.estimate_word_audio.return_value = None
        result = ds.estimate_word_audio("pod1", "ep1", "missing")
        assert result is None


# ===================================================================
# IndexNotBuiltError Propagation
# ===================================================================


class TestIndexNotBuiltErrorPropagation:
    """Tests that IndexNotBuiltError from backend propagates through wrapper."""

    def _setup_raise(self, ds, method_name):
        getattr(ds._parquet_backend, method_name).side_effect = IndexNotBuiltError(
            f"Index not built for {method_name}"
        )

    def test_search_turns_propagates(self):
        ds = _make_dataset()
        self._setup_raise(ds, "search_turns")
        with pytest.raises(IndexNotBuiltError):
            ds.search_turns("hello")

    def test_get_turn_metrics_propagates(self):
        ds = _make_dataset()
        self._setup_raise(ds, "get_turn_metrics")
        with pytest.raises(IndexNotBuiltError):
            ds.get_turn_metrics("pod1", "ep1")

    def test_concordance_propagates(self):
        ds = _make_dataset()
        self._setup_raise(ds, "concordance")
        with pytest.raises(IndexNotBuiltError):
            ds.concordance("um")

    def test_search_by_speaker_name_propagates(self):
        ds = _make_dataset()
        self._setup_raise(ds, "search_by_speaker_name")
        with pytest.raises(IndexNotBuiltError):
            ds.search_by_speaker_name("John")

    def test_get_episode_metrics_propagates(self):
        ds = _make_dataset()
        self._setup_raise(ds, "get_episode_metrics")
        with pytest.raises(IndexNotBuiltError):
            ds.get_episode_metrics("ep1")

    def test_filter_episodes_by_metrics_propagates(self):
        ds = _make_dataset()
        self._setup_raise(ds, "filter_episodes_by_metrics")
        with pytest.raises(IndexNotBuiltError):
            ds.filter_episodes_by_metrics(min_word_count=100)


# ===================================================================
# Non-Parquet Mode Rejection
# ===================================================================


class TestNonParquetModeRejection:
    """All 8 wrappers raise SPORCError when _parquet_mode=False."""

    def test_search_turns_rejected(self):
        ds = _make_dataset(parquet_mode=False)
        with pytest.raises(SPORCError, match="requires parquet mode"):
            ds.search_turns("hello")

    def test_search_episodes_by_text_rejected(self):
        ds = _make_dataset(parquet_mode=False)
        with pytest.raises(SPORCError, match="requires parquet mode"):
            ds.search_episodes_by_text("hello")

    def test_search_by_speaker_name_rejected(self):
        ds = _make_dataset(parquet_mode=False)
        with pytest.raises(SPORCError, match="requires parquet mode"):
            ds.search_by_speaker_name("John")

    def test_concordance_rejected(self):
        ds = _make_dataset(parquet_mode=False)
        with pytest.raises(SPORCError, match="requires parquet mode"):
            ds.concordance("um")

    def test_get_episode_metrics_rejected(self):
        ds = _make_dataset(parquet_mode=False)
        with pytest.raises(SPORCError, match="requires parquet mode"):
            ds.get_episode_metrics("ep1")

    def test_filter_episodes_by_metrics_rejected(self):
        ds = _make_dataset(parquet_mode=False)
        with pytest.raises(SPORCError, match="requires parquet mode"):
            ds.filter_episodes_by_metrics(min_word_count=100)

    def test_get_turn_metrics_rejected(self):
        ds = _make_dataset(parquet_mode=False)
        with pytest.raises(SPORCError, match="requires parquet mode"):
            ds.get_turn_metrics("pod1", "ep1")

    def test_estimate_word_audio_rejected(self):
        ds = _make_dataset(parquet_mode=False)
        with pytest.raises(SPORCError, match="requires parquet mode"):
            ds.estimate_word_audio("pod1", "ep1", "hello")
