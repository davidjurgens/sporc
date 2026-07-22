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

        backend.build_episode_object(PID_WITH_TURNS, EID_WITH_TURNS)   # read 1
        # Evict PID_WITH_TURNS (this reads for itself).
        backend._read_podcast_episodes_partition(PID_NO_TURNS)          # read 2
        ep = backend.build_episode_object(PID_WITH_TURNS, EID_WITH_TURNS)  # read 3

        # Reads cannot be attributed by path any more: podcasts share a part
        # file and are told apart by row group, so the id is not in the name.
        # Three opens is the point -- the third is the evicted podcast being
        # re-read rather than silently returning nothing.
        assert len(count_partition_reads) == 3, (
            "an evicted podcast must be re-read, not lost")
        assert ep is not None


@pytest.fixture
def count_tree_reads(monkeypatch):
    """Count real opens of the turn and acoustic part files, by tree."""
    calls = []
    real = pq.ParquetFile

    def counting(path, *a, **kw):
        p = str(path)
        if "turns" in p or "acoustics" in p:
            calls.append(p)
        return real(path, *a, **kw)

    monkeypatch.setattr(pb_module.pq, "ParquetFile", counting)
    return calls


@pytest.mark.integration
class TestTreeRowGroupCache:
    """
    The episode partition got a cache; turns and acoustics did not, so reading a
    podcast's episodes re-read both once per episode -- 80 row-group reads for a
    40-episode show instead of 2.
    """

    def test_repeated_turn_reads_hit_the_file_once_per_tree(
        self, tmp_parquet_layout, count_tree_reads
    ):
        backend = ParquetBackend(tmp_parquet_layout)

        for _ in range(5):
            backend.get_turns_for_episode(PID_WITH_TURNS, EID_WITH_TURNS,
                                          include_audio=True)

        # One for turns/text, one for acoustics, and no more.
        assert len(count_tree_reads) == 2, (
            f"expected 2 reads, got {len(count_tree_reads)}: {count_tree_reads}"
        )

    def test_cached_read_returns_the_same_rows(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)

        first = backend.get_turns_for_episode(PID_WITH_TURNS, EID_WITH_TURNS)
        second = backend.get_turns_for_episode(PID_WITH_TURNS, EID_WITH_TURNS)

        assert [r["turn_text"] for r in first] == [r["turn_text"] for r in second]

    def test_column_projection_does_not_poison_the_cache(self, tmp_parquet_layout):
        # _episode_ids_with_turns reads a single column. If that were cached
        # under the same key, every later reader would get a table with one
        # column and turns would come back textless.
        backend = ParquetBackend(tmp_parquet_layout)

        backend._episode_ids_with_turns(PID_WITH_TURNS)
        table = backend._read_tree("turns_text", PID_WITH_TURNS)

        assert "turn_text" in table.schema.names

    def test_cache_is_bounded(self, tmp_parquet_layout, monkeypatch):
        monkeypatch.setattr(pb_module, "_TREE_CACHE_SIZE", 1)
        backend = ParquetBackend(tmp_parquet_layout)

        backend._read_tree("turns_text", PID_WITH_TURNS)
        backend._read_tree("acoustics", PID_WITH_TURNS)

        assert len(backend._tree_cache) <= 1

    def test_absent_podcast_is_not_cached_as_a_table(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)

        assert backend._read_tree("turns_text", PID_NO_TURNS) is None
        assert ("turns_text", PID_NO_TURNS) not in backend._tree_cache


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


@pytest.mark.integration
class TestSubsetRoundTrip:
    """
    A subset has to be a dataset, not a pile of files.

    make_subset writes the packed layout itself -- parts, row groups, and its
    own shard map -- so the same client code reads a subset and the full corpus.
    Building one and then opening it is the only check that actually proves it.
    """

    @staticmethod
    def _build():
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
        from make_subset import build

        return build

    def test_subset_opens_and_serves_turns(self, tmp_parquet_layout, tmp_path):
        out = str(tmp_path / "subset")
        self._build()(tmp_parquet_layout, out, [PID_WITH_TURNS],
                      diarized_only=False)

        ds = SPORCDataset(parquet_dir=out)
        pod = ds.search_podcast("Turns Podcast")
        assert pod.podcast_id == PID_WITH_TURNS

        episode = next(e for e in pod.episodes if e.episode_id == EID_WITH_TURNS)
        turns = episode.turns
        assert len(turns) == 2
        assert turns[0].text == "hello world"

    def test_subset_excludes_unselected_podcasts(self, tmp_parquet_layout,
                                                 tmp_path):
        out = str(tmp_path / "subset2")
        self._build()(tmp_parquet_layout, out, [PID_WITH_TURNS],
                      diarized_only=False)

        ds = SPORCDataset(parquet_dir=out)
        # The catalog must not advertise a podcast whose rows were not copied.
        assert PID_NO_TURNS not in ds._parquet_backend.get_all_podcast_ids()
