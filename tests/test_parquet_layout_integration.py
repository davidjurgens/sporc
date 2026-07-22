"""
Tests that exercise a real Parquet layout rather than a mocked backend.

The rest of the suite mocks ParquetBackend, which let a NoneType crash in
get_all_podcasts() survive a fully-passing run: it only fires on the warm-cache
path, which a mock never takes. These tests read real files instead.
"""

import os
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sporc.dataset import SPORCDataset
from sporc.parquet_backend import ParquetBackend
from sporc.source import LocalDataSource

from conftest import EID_WITH_TURNS, PID_NO_TURNS, PID_WITH_TURNS

pytestmark = pytest.mark.integration


@pytest.fixture
def warm(tmp_parquet_layout):
    """A layout whose index cache has already been built and saved."""
    ParquetBackend(tmp_parquet_layout)          # cold: builds and saves cache
    assert os.path.exists(os.path.join(tmp_parquet_layout, "metadata",
                                       "_index_cache.pkl"))
    return tmp_parquet_layout


class TestWarmCachePath:
    """The path a second run takes, where the cache is loaded from disk."""

    def test_cache_is_used(self, warm):
        b = ParquetBackend(warm)
        assert b._cache_validated is True
        # The heavy DataFrame stays unmaterialized until something needs it.
        assert b._podcast_df is None

    def test_get_all_podcasts_without_prior_call(self, warm):
        """Regression: crashed with TypeError when called first on a warm cache."""
        ds = SPORCDataset(parquet_dir=warm)
        assert len(ds.get_all_podcasts()) == 2

    def test_iterate_podcasts_without_prior_call(self, warm):
        """Regression: same NoneType crash via the iterator."""
        ds = SPORCDataset(parquet_dir=warm)
        assert len(list(ds.iterate_podcasts())) == 2

    def test_podcast_ids_need_no_dataframe(self, warm):
        b = ParquetBackend(warm)
        assert b.get_all_podcast_ids() == [PID_WITH_TURNS, PID_NO_TURNS]
        assert b._podcast_df is None

    def test_ids_match_dataframe_order(self, warm):
        b = ParquetBackend(warm)
        from_index = b.get_all_podcast_ids()
        b._ensure_podcast_df()
        assert from_index == b._podcast_df["podcast_id"].tolist()


class TestCacheFingerprint:
    """The cache must survive a download but not a real data change."""

    def test_survives_mtime_change(self, warm):
        meta = os.path.join(warm, "metadata")
        for name in os.listdir(meta):
            if name.endswith(".parquet"):
                p = os.path.join(meta, name)
                data = open(p, "rb").read()
                open(p, "wb").write(data)      # identical bytes, new mtime
                os.utime(p, (time.time() + 10, time.time() + 10))
        b = ParquetBackend(warm)
        assert b._cache_validated is True

    def test_detects_content_change(self, warm):
        p = os.path.join(warm, "metadata", "hostname_index.parquet")
        pq.write_table(pa.table({"hostname": ["c.example.com"],
                                 "podcast_id": ["ffffffffffff"]}), p)
        b = ParquetBackend(warm)
        # Rebuilt from the changed file rather than trusting the stale cache.
        assert "c.example.com" in b._hostname_to_pids

    def test_fingerprint_is_content_not_mtime(self, tmp_parquet_layout):
        meta = os.path.join(tmp_parquet_layout, "metadata")
        before = ParquetBackend._fingerprint(meta)
        for name in os.listdir(meta):
            if name.endswith(".parquet"):
                os.utime(os.path.join(meta, name), (time.time() + 99,) * 2)
        assert ParquetBackend._fingerprint(meta) == before

    def test_stat_signature_tracks_mtime(self, tmp_parquet_layout):
        meta = os.path.join(tmp_parquet_layout, "metadata")
        before = ParquetBackend._stat_signature(meta)
        os.utime(os.path.join(meta, "hostname_index.parquet"),
                 (time.time() + 99,) * 2)
        assert ParquetBackend._stat_signature(meta) != before


class TestTurnCoverage:
    """Empty turns must be distinguishable from absent turn data."""

    def test_episode_with_turns(self, tmp_parquet_layout):
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        pod = ds.search_podcast("Turns Podcast")
        ep = next(e for e in pod.episodes if e.title == "Episode a1b2")
        assert len(ep.turns) == 2
        assert ep.has_turn_data is True

    def test_podcast_without_turns_partition(self, tmp_parquet_layout):
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        ep = ds.search_podcast("No Turns Podcast").episodes[0]
        assert ep.turns == []
        assert ep.has_turn_data is False

    def test_episode_uncovered_within_covered_podcast(self, tmp_parquet_layout):
        """A turns partition existing does not mean every episode is in it."""
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        pod = ds.search_podcast("Turns Podcast")
        ep = next(e for e in pod.episodes if e.title == "Episode cc00")
        assert ep.turns == []
        assert ep.has_turn_data is False

    def test_backend_podcast_level_check(self, tmp_parquet_layout):
        b = ParquetBackend(tmp_parquet_layout)
        assert b.has_turn_data(PID_WITH_TURNS) is True
        assert b.has_turn_data(PID_NO_TURNS) is False

    def test_backend_episode_level_check(self, tmp_parquet_layout):
        b = ParquetBackend(tmp_parquet_layout)
        assert b.episode_has_turn_data(PID_WITH_TURNS, EID_WITH_TURNS) is True
        assert b.episode_has_turn_data(PID_WITH_TURNS, "cc00dd11ee22ff33") is False

    def test_missing_turns_warns_once_per_podcast(self, tmp_parquet_layout, caplog):
        b = ParquetBackend(tmp_parquet_layout)
        b.get_turns_for_episode(PID_NO_TURNS, "x")
        b.get_turns_for_episode(PID_NO_TURNS, "y")
        assert sum("No turn data for podcast_id" in r.message
                   for r in caplog.records) == 1


class TestLazySource:
    """A source that cannot reach absent data must say so, not return empty."""

    def test_missing_partition_reads_as_empty_locally(self, tmp_parquet_layout):
        b = ParquetBackend(tmp_parquet_layout,
                           source=LocalDataSource(tmp_parquet_layout))
        assert b.get_turns_for_episode(PID_NO_TURNS, "anything") == []

    def test_prefetch_reports_what_exists(self, tmp_parquet_layout):
        b = ParquetBackend(tmp_parquet_layout)
        got = b.ensure_podcast_data(PID_WITH_TURNS)
        # Paths are part files now, not per-podcast directories, and only the
        # trees that actually hold this podcast are listed.
        assert got == {
            "episodes/part-000-000.parquet": True,
            "turns/text/part-000-000.parquet": True,
            "acoustics/part-000-000.parquet": True,
            "turns/metrics/part-000-000.parquet": True,
        }

    def test_prefetch_skips_trees_without_the_podcast(self, tmp_parquet_layout):
        b = ParquetBackend(tmp_parquet_layout)
        got = b.ensure_podcast_data(PID_NO_TURNS)
        # This podcast has episodes but no turns, so only the episode part is
        # named. Listing a turns part for it would fetch ~100 MB for nothing.
        assert got == {"episodes/part-000-000.parquet": True}


class TestSearchEpisodesFetching:
    """max_episodes must bound the partitions read, not just the result."""

    def _counting_backend(self, ds):
        reads = []
        original = ds._parquet_backend._source.path
        ds._parquet_backend._source.path = lambda rel: (
            reads.append(rel), original(rel))[1]
        return reads

    def test_limit_bounds_partition_reads(self, tmp_parquet_layout):
        """Regression: the limit was applied after building every match, which
        fetched ~1 GB across 14k partitions to return ten episodes."""
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        reads = self._counting_backend(ds)
        eps = ds.search_episodes(category="comedy", max_episodes=1)
        assert len(eps) == 1
        assert len([r for r in reads if r.startswith("episodes/")]) == 1

    def test_random_mode_also_bounded(self, tmp_parquet_layout):
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        reads = self._counting_backend(ds)
        eps = ds.search_episodes(category="comedy", max_episodes=1,
                                 sampling_mode="random")
        assert len(eps) == 1
        assert len([r for r in reads if r.startswith("episodes/")]) == 1

    def test_no_limit_returns_all_matches(self, tmp_parquet_layout):
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        assert len(ds.search_episodes(category="comedy")) == 2


class TestSearchWithoutIndex:
    """Text search must work on a subset, which has no 26GB FTS index."""

    def test_search_turns_scans(self, tmp_parquet_layout):
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        hits = ds.search_turns("hello")
        assert len(hits) == 1
        assert hits[0]["turn_text"] == "hello world"
        # Same shape as the indexed path, so results are interchangeable.
        for k in ("episode_id", "podcast_id", "turn_count", "speaker_role",
                  "speaker_name", "word_count", "score"):
            assert k in hits[0]

    def test_exact_and_regex_modes(self, tmp_parquet_layout):
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        assert len(ds.search_turns("WORLD", mode="exact")) == 1     # case-insensitive
        assert len(ds.search_turns("go+dbye", mode="regex")) == 1

    def test_fts_requires_all_terms(self, tmp_parquet_layout):
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        assert len(ds.search_turns("hello world")) == 1
        assert ds.search_turns("hello goodbye") == []

    def test_speaker_role_filter(self, tmp_parquet_layout):
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        assert len(ds.search_turns("hello", speaker_role="host")) == 1
        assert ds.search_turns("hello", speaker_role="guest") == []

    def test_no_match_is_empty(self, tmp_parquet_layout):
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        assert ds.search_turns("nonexistentxyz") == []

    def test_search_episodes_by_text_scans(self, tmp_parquet_layout):
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        out = ds.search_episodes_by_text("hello")
        assert len(out) == 1 and out[0]["match_count"] == 1

    def test_concordance_keyword_is_the_search_term(self, tmp_parquet_layout):
        """Regression: the keyword was the word BEFORE the match whenever the
        match started at a word boundary."""
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        hits = ds.concordance("world", context_words=3)
        assert len(hits) == 1
        assert hits[0]["keyword"] == "world"
        assert hits[0]["left_context"] == "hello"
        assert hits[0]["right_context"] == ""

    def test_concordance_first_word(self, tmp_parquet_layout):
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        hits = ds.concordance("hello")
        assert hits[0]["keyword"] == "hello"
        assert hits[0]["left_context"] == ""

    def test_concordance_mid_word_match(self, tmp_parquet_layout):
        """A match starting inside a token still centers on that token."""
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        hits = ds.concordance("orld")
        assert hits[0]["keyword"] == "world"


class TestPrefetchResolution:
    """subset= entries resolve to podcasts before anything is fetched."""

    def test_prefetch_by_title_and_id(self, tmp_parquet_layout):
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        out = ds.prefetch(["Turns Podcast", PID_NO_TURNS])
        assert out["podcasts"] == 2
        assert out["unresolved"] == []

    def test_prefetch_by_episode_id(self, tmp_parquet_layout):
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        out = ds.prefetch([EID_WITH_TURNS])
        assert out["podcasts"] == 1

    def test_prefetch_dedupes_podcasts(self, tmp_parquet_layout):
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        out = ds.prefetch({"podcast_ids": [PID_WITH_TURNS, PID_WITH_TURNS]})
        assert out["podcasts"] == 1

    def test_prefetch_reports_unknown_id(self, tmp_parquet_layout):
        """A well-formed but unknown id is reported, not fetched blindly."""
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        out = ds.prefetch(["Turns Podcast", "ffffffffffff"])
        assert out["unresolved"] == ["ffffffffffff"]
        assert out["podcasts"] == 1

    def test_prefetch_reports_unknown_title(self, tmp_parquet_layout):
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        out = ds.prefetch([PID_WITH_TURNS, "No Such Show Anywhere"])
        assert out["unresolved"] == ["No Such Show Anywhere"]
        assert out["podcasts"] == 1

    def test_prefetch_all_unresolved_raises(self, tmp_parquet_layout):
        ds = SPORCDataset(parquet_dir=tmp_parquet_layout)
        with pytest.raises(ValueError, match="resolved to no podcasts"):
            ds.prefetch(["ffffffffffff"])


@pytest.mark.integration
class TestWordCountAgreesAcrossTrees:
    """
    turns/text.word_count counts aligned tokens and turns/metrics.word_count
    counts words. Turn.word_count has to be the second one, or a turn does not
    add up to the episode totals built from the same definition.
    """

    def test_turn_word_count_matches_the_metrics_tree(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)
        episode = backend.build_episode_object(PID_WITH_TURNS, EID_WITH_TURNS)

        metrics = backend._read_tree("turns_metrics", PID_WITH_TURNS)
        expected = dict(zip(metrics.column("turn_count").to_pylist(),
                            metrics.column("word_count").to_pylist()))

        assert episode.turns, "fixture must supply turns"
        for turn in episode.turns:
            assert turn.word_count == expected[turn.turn_count], (
                f"turn {turn.turn_count}: Turn.word_count={turn.word_count} "
                f"but turns/metrics says {expected[turn.turn_count]}"
            )


@pytest.mark.integration
class TestTokenCountComesFromTheRenamedColumn:
    """
    turns/text carries the aligner's count. It was called word_count, which
    collided with a different measure of the same name in turns/metrics, so the
    dataset renamed it to token_count.
    """

    def test_token_count_is_read_from_the_tree(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)
        episode = backend.build_episode_object(PID_WITH_TURNS, EID_WITH_TURNS)

        # The fixture stores 3 tokens against 2-word text on purpose.
        assert [t.token_count for t in episode.turns] == [3, 3]
        assert [t.word_count for t in episode.turns] == [2, 2]

    def test_a_pre_rename_layout_still_reads(self, tmp_parquet_layout, tmp_path):
        # Layouts built before the rename spell it word_count. It meant tokens
        # there too, so it is read rather than dropped on the floor.
        import shutil
        alt = tmp_path / "old_layout"
        shutil.copytree(tmp_parquet_layout, alt)
        part = alt / "turns" / "text" / "part-000-000.parquet"
        table = pq.ParquetFile(str(part)).read()
        renamed = ["word_count" if n == "token_count" else n
                   for n in table.schema.names]
        pq.write_table(table.rename_columns(renamed), str(part),
                       row_group_size=table.num_rows)

        backend = ParquetBackend(str(alt))
        episode = backend.build_episode_object(PID_WITH_TURNS, EID_WITH_TURNS)

        assert [t.token_count for t in episode.turns] == [3, 3]
