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


@pytest.mark.integration
class TestDuplicateTurnsDoNotFanOut:
    """
    81,807 turns are stored more than once, duplicated verbatim in dataset 1.0
    and carried forward. They appear in the acoustics tree too, so joining the
    two multiplied them: four copies of a turn against four acoustic rows came
    back as sixteen.
    """

    @staticmethod
    def _duplicate_a_turn(layout, tree, subdir):
        """Store the first turn of the fixture episode twice, as 1.0 does."""
        path = os.path.join(layout, subdir, "part-000-000.parquet")
        table = pq.ParquetFile(path).read()
        doubled = pa.concat_tables([table, table.slice(0, 1)])
        # The fixture is one podcast per part, so one row group is still right.
        pq.write_table(doubled, path, row_group_size=doubled.num_rows)

        smap = os.path.join(layout, "metadata", "shard_map.parquet")
        sm = pq.ParquetFile(smap).read().to_pandas()
        sm.loc[sm.tree == tree, "num_rows"] = doubled.num_rows
        pq.write_table(pa.Table.from_pandas(sm, preserve_index=False), smap)

    def test_a_duplicated_turn_is_not_multiplied_by_the_audio_join(
        self, tmp_parquet_layout
    ):
        self._duplicate_a_turn(tmp_parquet_layout, "turns_text", "turns/text")
        self._duplicate_a_turn(tmp_parquet_layout, "acoustics", "acoustics")

        backend = ParquetBackend(tmp_parquet_layout, load_audio_features=True)
        rows = backend.get_turns_for_episode(PID_WITH_TURNS, EID_WITH_TURNS,
                                             include_audio=True)

        # Three rows in the data (two turns, one stored twice) must stay three,
        # not become four via the 2x2 join on the duplicated turn_count.
        assert len(rows) == 3, [r["turn_count"] for r in rows]

    def test_the_audio_still_lands_on_the_turns(self, tmp_parquet_layout):
        self._duplicate_a_turn(tmp_parquet_layout, "turns_text", "turns/text")
        self._duplicate_a_turn(tmp_parquet_layout, "acoustics", "acoustics")

        backend = ParquetBackend(tmp_parquet_layout, load_audio_features=True)
        episode = backend.build_episode_object(PID_WITH_TURNS, EID_WITH_TURNS)

        assert any(t.get_audio_features() for t in episode.turns), (
            "de-duplicating the acoustics must not drop the join")


class TestMockBackendMirrorsInit:
    """
    mock_parquet_backend hand-lists the state __init__ sets, so every new
    attribute silently drifts out of it and surfaces later as an AttributeError
    in a test that has nothing to do with the change. Three had already drifted.
    """

    def test_fixture_sets_everything_init_does(self, mock_parquet_backend):
        import re
        import inspect
        from sporc.parquet_backend import ParquetBackend

        source = inspect.getsource(ParquetBackend.__init__)
        expected = set(re.findall(r"self\.(_?[a-z][a-z0-9_]*)\s*(?::[^=]+)?=",
                                  source))

        missing = sorted(a for a in expected
                         if not hasattr(mock_parquet_backend, a))
        assert not missing, (
            f"mock_parquet_backend is missing {missing}; add them to the "
            f"fixture in conftest.py so it still stands in for a real backend")


@pytest.mark.integration
class TestUnknownSearchCriteriaAreRefused:
    """
    search_episodes(**criteria) dropped keys it did not recognise, so a typo or
    an unsupported filter returned the whole catalog. On the full corpus that is
    1.1 million episodes handed back as if they matched.
    """

    def test_a_typo_raises_rather_than_matching_everything(
        self, tmp_parquet_layout
    ):
        backend = ParquetBackend(tmp_parquet_layout)
        everything = len(backend.search_episodes())

        with pytest.raises(TypeError, match="min_duraton"):
            backend.search_episodes(min_duraton=60)

        assert everything > 0, "fixture must have episodes for this to mean much"

    def test_limit_is_not_a_criterion(self, tmp_parquet_layout):
        # It reads like one, and silently returned every episode.
        backend = ParquetBackend(tmp_parquet_layout)

        with pytest.raises(TypeError, match="limit"):
            backend.search_episodes(limit=3)

    def test_a_row_cap_guess_is_pointed_at_max_episodes(
        self, tmp_parquet_layout
    ):
        # Refusing limit= is only half an answer: the criteria list does not
        # contain a row cap, so the message has to name the one that is.
        backend = ParquetBackend(tmp_parquet_layout)

        for wrong in ("limit", "n", "top_k", "count", "max_results",
                      "num_episodes"):
            with pytest.raises(TypeError, match="max_episodes"):
                backend.search_episodes(**{wrong: 3})

    def test_supported_criteria_still_filter(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)

        assert backend.search_episodes(min_duration=0)
        assert backend.search_episodes(max_duration=0) == []

    def test_the_error_names_what_is_supported(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)

        with pytest.raises(TypeError, match="min_duration"):
            backend.search_episodes(nonsense=1)


@pytest.mark.integration
class TestProjectionAvoidsWholePartFetch:
    """
    A column-projected read of a part not yet on disk should range-read just
    the podcast's row group, rather than fetching the whole part with path().
    On the Hub that is the difference between a few hundred KB and ~100 MB per
    probe; here we assert the dispatch, and that it returns identical data.
    """

    def test_projection_on_absent_part_uses_row_group_read(
            self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)
        source = backend._source

        # Pretend nothing is on disk, so _read_tree takes the range-read branch,
        # and record what it reaches for. The base LocalDataSource serves
        # read_row_group_columns from the real file, so the data is still real.
        calls = {"path": [], "rowgroup": []}
        real_path = source.path
        real_rgc = source.read_row_group_columns

        def spy_path(rel):
            calls["path"].append(rel)
            return real_path(rel)

        def spy_rgc(rel, rg, cols):
            calls["rowgroup"].append((rel, rg, tuple(cols)))
            return real_rgc(rel, rg, cols)

        source.exists_locally = lambda rel: False
        source.path = spy_path
        source.read_row_group_columns = spy_rgc

        ids = backend._episode_ids_with_turns(PID_WITH_TURNS)

        assert ids == frozenset([EID_WITH_TURNS])
        # The probe dispatched to the row-group projection read for the turns
        # part. (Whether that read avoids the whole-part fetch is a Hub property
        # -- LocalDataSource resolves it through path() either way -- so it is
        # asserted separately against a HubDataSource below.)
        assert any("turns" in rel for rel, _, _ in calls["rowgroup"]), \
            "expected a row-group projection read of the turns part"

    def test_projection_matches_a_full_read(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)
        source = backend._source
        loc = backend.shard_map.locate("turns_text", PID_WITH_TURNS)
        rel = backend.shard_map.relpath("turns_text", loc.part)

        projected = source.read_row_group_columns(
            rel, loc.row_group, ["episode_id"])
        full = pq.ParquetFile(source.path(rel)).read_row_group(
            loc.row_group, columns=["episode_id"])

        assert projected.column("episode_id").to_pylist() == \
            full.column("episode_id").to_pylist()

    def test_full_read_still_fetches_and_caches_the_part(
            self, tmp_parquet_layout):
        """A columns=None read keeps the whole-part path -- clustered access
        depends on the part being on disk and cached, not range-read."""
        backend = ParquetBackend(tmp_parquet_layout)
        source = backend._source

        fetched = []
        real_path = source.path
        source.path = lambda rel: (fetched.append(rel), real_path(rel))[1]

        first = backend._read_tree("turns_text", PID_WITH_TURNS)
        assert first is not None and first.num_rows > 0
        assert fetched, "full read should resolve the part via path()"

        # Second full read of the same podcast is served from the tree cache.
        fetched.clear()
        again = backend._read_tree("turns_text", PID_WITH_TURNS)
        assert again is first
        assert fetched == [], "cached full read should not touch the source"

    def test_hub_projection_range_reads_and_skips_path(self, tmp_parquet_layout):
        """On a HubDataSource, a projected read of an absent part opens the file
        through the range-capable filesystem and never calls hf_hub_download."""
        from sporc.source import HubDataSource

        backend = ParquetBackend(tmp_parquet_layout)
        loc = backend.shard_map.locate("turns_text", PID_WITH_TURNS)
        rel = backend.shard_map.relpath("turns_text", loc.part)
        real_part = os.path.join(tmp_parquet_layout, rel)

        # A Hub source whose snapshot root is empty, so the part is "absent" and
        # the range branch is taken. Its filesystem is faked to open the real
        # fixture file, and any whole-file download is made to fail loudly.
        empty_root = os.path.join(tmp_parquet_layout, "empty_snapshot")
        os.makedirs(empty_root, exist_ok=True)
        source = HubDataSource("blitt/SPoRC", empty_root, allow_downloads=True)

        opened = []

        class FakeFS:
            def open(self, path, mode="rb"):
                opened.append(path)
                return open(real_part, mode)

        source._fs = FakeFS()
        source._download_with_retry = lambda *a, **k: pytest.fail(
            "range read must not fall back to a whole-file download")

        table = source.read_row_group_columns(rel, loc.row_group, ["episode_id"])

        assert opened == [f"datasets/blitt/SPoRC/{rel}"]
        assert source.fetch_count == 1
        expected = pq.ParquetFile(real_part).read_row_group(
            loc.row_group, columns=["episode_id"])
        assert table.column("episode_id").to_pylist() == \
            expected.column("episode_id").to_pylist()
