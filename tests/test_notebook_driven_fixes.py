"""
Tests for the fixes that executing the tutorial notebooks turned up.

Each of these covers something that shipped untested: the episode-partition
cache, the two warnings on APIs that mislead, prefetch()'s local-source no-op,
and make_subset's partition check. The cache test reads a real layout on
purpose -- it asserts how many times a file is opened, which a mocked backend
cannot observe.
"""

import logging
import os
import sys
import warnings

import pyarrow.parquet as pq
import pytest

import sporc.parquet_backend as pb_module
from sporc.dataset import SPORCDataset
from sporc.parquet_backend import ParquetBackend

from conftest import EID_WITH_TURNS, PID_NO_TURNS, PID_WITH_TURNS


@pytest.fixture
def count_partition_reads(monkeypatch):
    """Count real opens of an episodes partition file."""
    calls = []
    real = pq.ParquetFile

    def counting(path, *a, **kw):
        if "episodes" in str(path):
            calls.append(str(path))
        return real(path, *a, **kw)

    monkeypatch.setattr(pb_module.pq, "ParquetFile", counting)
    return calls


@pytest.mark.integration
class TestEpisodePartitionCache:
    """
    build_episode_object re-read the whole partition -- transcripts and all --
    for every episode, so a 40-episode podcast parsed the same file 40 times.
    """

    def test_repeated_builds_read_the_partition_once(
        self, tmp_parquet_layout, count_partition_reads
    ):
        backend = ParquetBackend(tmp_parquet_layout)

        for _ in range(5):
            backend.build_episode_object(PID_WITH_TURNS, EID_WITH_TURNS)

        assert len(count_partition_reads) == 1, (
            f"expected 1 partition read, got {len(count_partition_reads)}"
        )

    def test_second_episode_of_same_podcast_reuses_the_read(
        self, tmp_parquet_layout, count_partition_reads
    ):
        backend = ParquetBackend(tmp_parquet_layout)

        backend.build_episode_object(PID_WITH_TURNS, EID_WITH_TURNS)
        backend.build_episode_object(PID_WITH_TURNS, "cc00dd11ee22ff33")

        assert len(count_partition_reads) == 1

    def test_cache_returns_equal_rows_on_hit(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)

        first = backend.build_episode_object(PID_WITH_TURNS, EID_WITH_TURNS)
        second = backend.build_episode_object(PID_WITH_TURNS, EID_WITH_TURNS)

        # A cache that served a different object than the cold path would be
        # worse than no cache. Episode carries no id, so mp3_url stands in --
        # it is what the episode id is derived from.
        assert first.mp3_url == second.mp3_url
        assert first.title == second.title
        assert first.transcript == second.transcript

    def test_cache_is_bounded(self, tmp_parquet_layout, monkeypatch):
        # Partitions hold full transcripts, so an unbounded cache would grow
        # over a whole-corpus pass until it exhausted memory.
        monkeypatch.setattr(pb_module, "_EPISODE_PARTITION_CACHE_SIZE", 2)
        backend = ParquetBackend(tmp_parquet_layout)

        for pid in (PID_WITH_TURNS, PID_NO_TURNS, "aaaabbbbcccc", "ddddeeeeffff"):
            backend._read_podcast_episodes_partition(pid)

        assert len(backend._episode_partition_cache) <= 2

    def test_eviction_falls_back_to_a_real_read(
        self, tmp_parquet_layout, count_partition_reads, monkeypatch
    ):
        monkeypatch.setattr(pb_module, "_EPISODE_PARTITION_CACHE_SIZE", 1)
        backend = ParquetBackend(tmp_parquet_layout)

        backend.build_episode_object(PID_WITH_TURNS, EID_WITH_TURNS)
        # Evict PID_WITH_TURNS (this reads a partition of its own).
        backend._read_podcast_episodes_partition(PID_NO_TURNS)
        ep = backend.build_episode_object(PID_WITH_TURNS, EID_WITH_TURNS)

        reads = [p for p in count_partition_reads if PID_WITH_TURNS in p]
        assert len(reads) == 2, "an evicted podcast must be re-read, not lost"
        assert ep is not None


@pytest.mark.integration
class TestMisleadingApiWarnings:
    """
    Both of these APIs return something plausible and wrong. The warning is the
    feature, so it needs a test.
    """

    def test_estimate_word_audio_warns_it_is_not_alignment(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)

        with pytest.warns(DeprecationWarning, match="not acoustic alignment"):
            backend.estimate_word_audio(PID_WITH_TURNS, EID_WITH_TURNS, "the")

    def test_estimate_word_audio_points_at_the_replacement(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)

        with pytest.warns(DeprecationWarning, match=r"phonetics\.align_turn"):
            backend.estimate_word_audio(PID_WITH_TURNS, EID_WITH_TURNS, "the")

    def test_guest_search_warns_that_hits_may_be_mentions(self, tmp_parquet_layout):
        # guest_predicted_names lists people who were *named*, so this returns
        # episodes where someone was only mentioned. George Floyd came back as a
        # guest on 237 podcasts; corpus-wide only ~2.5% of cross-podcast guests
        # were ever diarized.
        backend = ParquetBackend(tmp_parquet_layout)

        with pytest.warns(UserWarning, match="2.5%"):
            backend.search_by_speaker_name("A Guest", role="guest")

    def test_guest_warning_names_the_field_to_validate_against(
        self, tmp_parquet_layout
    ):
        backend = ParquetBackend(tmp_parquet_layout)

        with pytest.warns(UserWarning, match="guest_speaker_labels"):
            backend.search_by_speaker_name("A Guest", role="guest")

    @pytest.mark.parametrize("role", [None, "host"])
    def test_non_guest_roles_do_not_warn(self, tmp_parquet_layout, role):
        # Only the guest path is unsound; warning on host lookups would train
        # users to ignore the warning that matters.
        backend = ParquetBackend(tmp_parquet_layout)

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            backend.search_by_speaker_name("Ira Glass", role=role)

        assert not [w for w in record if issubclass(w.category, UserWarning)]


@pytest.mark.integration
class TestPrefetchOnLocalSource:
    def test_prefetch_warns_it_cannot_fetch_from_a_local_dir(
        self, tmp_parquet_layout, caplog
    ):
        # Without the warning this looks like a successful fetch: it downloads
        # nothing, yet still returns a nonzero "files".
        sporc = SPORCDataset(parquet_dir=tmp_parquet_layout)

        with caplog.at_level(logging.WARNING):
            result = sporc.prefetch({"podcast_ids": [PID_WITH_TURNS]})

        assert "nothing to fetch" in caplog.text
        assert result["podcasts"] == 1

    def test_files_counts_availability_not_downloads(self, tmp_parquet_layout):
        # "files" counts partitions present afterwards. Nothing was downloaded
        # here -- the source is a local directory -- so any nonzero value is
        # availability, and reading it as a download count overstates the work
        # done (on a Hub source it silently includes already-cached files too).
        sporc = SPORCDataset(parquet_dir=tmp_parquet_layout)

        result = sporc.prefetch({"podcast_ids": [PID_WITH_TURNS]})

        assert result["files"] > 0


class TestResolvePresent:
    """
    make_subset skipped absent partitions silently, emitting a subset whose
    catalog advertised podcasts whose data was never copied -- the exact
    catalog/data disagreement the script exists to prevent.
    """

    @staticmethod
    def _resolve_present():
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
        from make_subset import resolve_present

        return resolve_present

    def test_splits_present_from_missing(self, tmp_parquet_layout):
        resolve_present = self._resolve_present()

        present, missing = resolve_present(
            tmp_parquet_layout, [PID_WITH_TURNS, "not_a_real_podcast"]
        )

        assert present == [PID_WITH_TURNS]
        assert missing == ["not_a_real_podcast"]

    def test_reports_missing_rather_than_dropping_quietly(
        self, tmp_parquet_layout, caplog
    ):
        resolve_present = self._resolve_present()

        with caplog.at_level(logging.WARNING):
            resolve_present(tmp_parquet_layout, ["gone_a", "gone_b"])

        assert caplog.records, "absent partitions must be reported, not skipped"
