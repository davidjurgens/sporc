"""
Tests for SPORCDataset parquet-mode wrappers and initialization.
"""

import pytest
from unittest.mock import MagicMock, patch

from sporc.dataset import SPORCDataset
from sporc.exceptions import SPORCError, IndexNotBuiltError
from sporc.source import HubDataSource


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

    # -- lazy path (the default): metadata up front, partitions on demand ----

    @patch("sporc.parquet_backend.ParquetBackend")
    @patch("huggingface_hub.hf_hub_download", side_effect=lambda filename, **kw: f"/fake/cache/dir/{filename}")
    def test_lazy_downloads_metadata_only(self, mock_dl, mock_backend):
        """By default only the catalogs are fetched, not the ~57GB of partitions."""
        SPORCDataset()
        got = [c.kwargs["filename"] for c in mock_dl.call_args_list]
        assert got == SPORCDataset._CORE_METADATA + SPORCDataset._OPTIONAL_METADATA
        # No partition or legacy file is touched at load time.
        assert not any(f.startswith(("episodes/", "turns/")) for f in got)
        assert not any(f.endswith(".jsonl.gz") for f in got)
        assert "metadata/turns_search.duckdb" not in got
        assert mock_backend.call_args.args[0] == "/fake/cache/dir"

    @patch("sporc.parquet_backend.ParquetBackend")
    @patch("huggingface_hub.snapshot_download")
    @patch("huggingface_hub.hf_hub_download", side_effect=lambda filename, **kw: f"/fake/cache/dir/{filename}")
    def test_lazy_never_enumerates_repo(self, mock_dl, mock_snapshot, mock_backend):
        """snapshot_download is avoided: it lists all ~685k files, which takes hours."""
        SPORCDataset()
        mock_snapshot.assert_not_called()

    @patch("sporc.parquet_backend.ParquetBackend")
    @patch("huggingface_hub.hf_hub_download", side_effect=lambda filename, **kw: f"/fake/cache/dir/{filename}")
    def test_lazy_builds_hub_source(self, mock_dl, mock_backend):
        """The backend gets a HubDataSource so partitions can be fetched later."""
        SPORCDataset(use_auth_token="hf_TOKEN", cache_dir="/custom/cache")
        source = mock_backend.call_args.kwargs["source"]
        assert isinstance(source, HubDataSource)
        assert source.repo_id == "blitt/SPoRC"
        assert source.token == "hf_TOKEN"
        assert source.allow_downloads is True

    @patch("sporc.parquet_backend.ParquetBackend")
    @patch("huggingface_hub.hf_hub_download", side_effect=lambda filename, **kw: f"/fake/cache/dir/{filename}")
    def test_allow_downloads_false_pins_source(self, mock_dl, mock_backend):
        """allow_downloads=False is passed through to the source."""
        SPORCDataset(allow_downloads=False)
        assert mock_backend.call_args.kwargs["source"].allow_downloads is False

    @patch("sporc.parquet_backend.ParquetBackend")
    @patch("huggingface_hub.hf_hub_download", side_effect=lambda filename, **kw: f"/fake/cache/dir/{filename}")
    def test_lazy_with_search_db_fetches_it(self, mock_dl, mock_backend):
        """include_search_db pulls the search DB alongside the catalogs."""
        SPORCDataset(include_search_db=True)
        got = [c.kwargs["filename"] for c in mock_dl.call_args_list]
        assert "metadata/turns_search.duckdb" in got

    @patch("sporc.dataset.SPORCDataset.prefetch")
    @patch("sporc.parquet_backend.ParquetBackend")
    @patch("huggingface_hub.hf_hub_download", side_effect=lambda filename, **kw: f"/fake/cache/dir/{filename}")
    def test_subset_triggers_prefetch(self, mock_dl, mock_backend, mock_prefetch):
        """A subset= argument is prefetched at load time."""
        SPORCDataset(subset=["03b0f2a257fd"])
        mock_prefetch.assert_called_once_with(["03b0f2a257fd"])

    @patch("sporc.parquet_backend.ParquetBackend")
    @patch("huggingface_hub.hf_hub_download", side_effect=lambda filename, **kw: f"/fake/cache/dir/{filename}")
    def test_no_subset_no_prefetch(self, mock_dl, mock_backend):
        """Without subset=, nothing is prefetched."""
        with patch.object(SPORCDataset, "prefetch") as mock_prefetch:
            SPORCDataset()
            mock_prefetch.assert_not_called()

    # -- eager path: lazy=False downloads the whole corpus ------------------

    @patch("sporc.parquet_backend.ParquetBackend")
    @patch("huggingface_hub.snapshot_download", return_value="/fake/cache/dir")
    def test_eager_excludes_legacy_and_search_db(self, mock_download, mock_backend):
        """lazy=False downloads everything except legacy exports and the search DB."""
        SPORCDataset(lazy=False)
        mock_download.assert_called_once_with(
            repo_id="blitt/SPoRC",
            repo_type="dataset",
            token=None,
            cache_dir=None,
            ignore_patterns=["*.jsonl.gz", "metadata/turns_search.duckdb"],
        )
        # No lazy source: everything is already on disk.
        assert mock_backend.call_args.kwargs["source"] is None

    @patch("sporc.parquet_backend.ParquetBackend")
    @patch("huggingface_hub.snapshot_download", return_value="/fake/cache/dir")
    def test_eager_legacy_jsonlines_never_downloaded(self, mock_download, mock_backend):
        """The unused pre-1.0 jsonlines exports (~23GB) are always excluded."""
        SPORCDataset(lazy=False)
        assert "*.jsonl.gz" in mock_download.call_args.kwargs["ignore_patterns"]

    @patch("sporc.parquet_backend.ParquetBackend")
    @patch("huggingface_hub.snapshot_download", return_value="/fake/cache/dir")
    def test_eager_search_db_included_on_request(self, mock_download, mock_backend):
        """include_search_db=True opts in but still skips legacy files."""
        SPORCDataset(lazy=False, include_search_db=True)
        patterns = mock_download.call_args.kwargs["ignore_patterns"]
        assert "metadata/turns_search.duckdb" not in patterns
        assert "*.jsonl.gz" in patterns

    @patch("sporc.parquet_backend.ParquetBackend")
    @patch("huggingface_hub.snapshot_download", return_value="/fake/cache/dir")
    def test_eager_ignore_patterns_override(self, mock_download, mock_backend):
        """An explicit ignore_patterns replaces the defaults."""
        SPORCDataset(lazy=False, ignore_patterns=["nothing/*"])
        assert mock_download.call_args.kwargs["ignore_patterns"] == ["nothing/*"]

    # -- local path: never downloads ---------------------------------------

    @patch("sporc.parquet_backend.ParquetBackend")
    @patch("huggingface_hub.snapshot_download")
    def test_snapshot_download_not_called_when_parquet_dir_provided(self, mock_download, mock_backend):
        """snapshot_download is NOT called when parquet_dir is given."""
        SPORCDataset(parquet_dir="/my/local/dir")
        mock_download.assert_not_called()
        assert mock_backend.call_args.args[0] == "/my/local/dir"
        assert mock_backend.call_args.kwargs["source"] is None

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
