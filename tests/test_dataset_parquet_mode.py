"""
Tests for SPORCDataset parquet-mode wrappers and initialization.
"""

import pytest
from unittest.mock import MagicMock, patch

from sporc.dataset import SPORCDataset
from sporc.exceptions import SPORCError, IndexNotBuiltError


def _make_dataset():
    """Create a SPORCDataset instance with skipped __init__."""
    ds = SPORCDataset.__new__(SPORCDataset)
    ds._parquet_backend = MagicMock()
    ds._loaded = True
    return ds


# ===================================================================
# __init__ download logic
# ===================================================================


class TestInit:
    """Tests for __init__ download logic."""

    @patch("sporc.parquet_backend.ParquetBackend")
    @patch("huggingface_hub.snapshot_download", return_value="/fake/cache/dir")
    def test_snapshot_download_called_when_no_parquet_dir(self, mock_download, mock_backend):
        """snapshot_download is called when parquet_dir is None."""
        ds = SPORCDataset()
        mock_download.assert_called_once_with(
            repo_id="blitt/SPoRC",
            repo_type="dataset",
            token=None,
            cache_dir=None,
        )
        mock_backend.assert_called_once_with("/fake/cache/dir")

    @patch("sporc.parquet_backend.ParquetBackend")
    @patch("huggingface_hub.snapshot_download")
    def test_snapshot_download_not_called_when_parquet_dir_provided(self, mock_download, mock_backend):
        """snapshot_download is NOT called when parquet_dir is given."""
        ds = SPORCDataset(parquet_dir="/my/local/dir")
        mock_download.assert_not_called()
        mock_backend.assert_called_once_with("/my/local/dir")

    @patch("sporc.parquet_backend.ParquetBackend")
    @patch("huggingface_hub.snapshot_download", return_value="/fake/cache/dir")
    def test_auth_token_and_cache_dir_passed_through(self, mock_download, mock_backend):
        """use_auth_token and cache_dir are passed to snapshot_download."""
        ds = SPORCDataset(use_auth_token="hf_TOKEN", cache_dir="/custom/cache")
        mock_download.assert_called_once_with(
            repo_id="blitt/SPoRC",
            repo_type="dataset",
            token="hf_TOKEN",
            cache_dir="/custom/cache",
        )


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
