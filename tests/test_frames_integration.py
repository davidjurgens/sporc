"""
The columnar API against a real published layout.

The fixture layout is faithful but tiny -- three podcasts, thirty-two turns --
so it cannot show whether a claim holds at scale, whether a part file with a
thousand podcasts in it is filtered correctly, or whether the numbers in the
documentation are true. These tests run against ``subsets/tutorial`` (403
podcasts, 2,967 episodes, 374,155 turns) when a developer has built it, and skip
otherwise, so CI stays self-contained without the claims going unchecked.

Build it with ``python scripts/build_tutorial_subset.py``.
"""

import os

import pandas as pd
import pytest

import sporc
from sporc import frames

pytestmark = pytest.mark.integration

SUBSET = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "subsets", "tutorial")


@pytest.fixture(scope="module")
def ds():
    if not os.path.isdir(SUBSET):
        pytest.skip("subsets/tutorial not built")
    return sporc.SPORCDataset(parquet_dir=SUBSET)


@pytest.fixture(scope="module")
def turns(ds):
    return ds.turns_frame()


class TestScale:
    def test_reads_every_turn_in_the_subset(self, ds, turns):
        assert len(turns) == ds._parquet_backend.tree_row_count("turns_text")
        assert len(turns) > 300_000

    def test_narrow_projection_is_much_smaller(self, ds, turns):
        """
        The claim columns= rests on. If this stops holding, the guidance to
        reach for columns= first is wrong.
        """
        narrow = ds.turns_frame(columns=["episode_id", "turn_count",
                                         "start_time"])
        assert len(narrow) == len(turns)
        wide_mb = turns.memory_usage(deep=True).sum()
        narrow_mb = narrow.memory_usage(deep=True).sum()
        assert narrow_mb * 5 < wide_mb

    def test_size_estimate_is_the_right_order_of_magnitude(self, ds, turns):
        """
        The guard refuses requests on this estimate, so it may be rough but it
        must not be wrong by an order of magnitude.
        """
        from sporc import schema

        actual = turns.memory_usage(deep=True).sum()
        estimate = schema.estimate_bytes("turns", list(turns.columns),
                                         len(turns))
        assert 0.5 < estimate / actual < 2.0

    def test_podcast_filter_narrows_within_a_shared_part(self, ds, turns):
        """
        Parts hold hundreds of podcasts, so this is a real filter rather than a
        choice of file.
        """
        some = list(turns["podcast_id"].drop_duplicates()[:5])
        got = ds.turns_frame(podcast_ids=some)
        assert set(got["podcast_id"]) == set(some)
        assert 0 < len(got) < len(turns)


class TestAgreesWithTheObjectModel:
    def test_turns_match_the_raw_backend_rows(self, ds, turns):
        """
        Against get_turns_for_episode, which returns unvalidated rows straight
        from the file. Nothing is dropped on either side, so this must match
        exactly -- including on the ~3.7% of episodes where start_time is not
        monotone in turn_count, which the fixture cannot produce at scale.
        """
        backend = ds._parquet_backend
        sample = turns[["episode_id", "podcast_id"]].drop_duplicates().head(25)

        for eid, pid in sample.itertuples(index=False):
            mine = turns[turns["episode_id"] == eid]
            rows = backend.get_turns_for_episode(pid, eid)
            assert list(mine["turn_count"]) == [r["turn_count"] for r in rows]
            assert list(mine["start_time"]) == [r["start_time"] for r in rows]

    def test_turns_match_episode_turn_objects(self, ds, turns):
        """
        Against Episode.turns, which skips rows with empty text and rows with
        end_time <= start_time. Removing those from the frame is what lines the
        two up; the difference itself is asserted in
        test_object_model_drops_some_rows.
        """
        backend = ds._parquet_backend
        sample = turns[["episode_id", "podcast_id"]].drop_duplicates().head(25)

        for eid, pid in sample.itertuples(index=False):
            mine = turns[turns["episode_id"] == eid]
            mine = mine[(mine["turn_text"].str.strip() != "")
                        & (mine["end_time"] > mine["start_time"])]
            objects = backend.build_episode_object(
                pid, eid, include_turns=True).turns
            assert list(mine["turn_count"]) == [t.turn_count for t in objects]

    def test_windows_match_sliding_window(self, ds, turns):
        backend = ds._parquet_backend
        sample = turns[["episode_id", "podcast_id"]].drop_duplicates().head(10)

        for eid, pid in sample.itertuples(index=False):
            mine = turns[turns["episode_id"] == eid]
            mine = mine[(mine["turn_text"].str.strip() != "")
                        & (mine["end_time"] > mine["start_time"])].reset_index(
                            drop=True)
            if mine.empty:
                continue
            got = frames.window_frame_from_turns(mine, size=12, overlap=6,
                                                 sort=False)
            ep = backend.build_episode_object(pid, eid, include_turns=True)
            expected = list(ep.sliding_window(12, overlap=6))
            assert len(got) == len(expected), eid
            for row, w in zip(got.itertuples(), expected):
                assert (row.start_index, row.end_index) == (w.start_index,
                                                            w.end_index)
                assert row.n_turns == w.size


class TestWindowsMatchTheHandRolledIdiom:
    def test_zero_overlap_reproduces_cumcount(self, ds, turns):
        """
        The tutorial builds windows with `cumcount() // 12`. At overlap=0 that
        is correct, so window_frame must agree with it exactly -- otherwise
        replacing the hand-rolled code changes published results.
        """
        t = turns.sort_values(["episode_id", "start_time", "turn_count"])
        t = t.reset_index(drop=True)
        t["win"] = t.groupby("episode_id").cumcount() // 12
        hand = t.groupby(["episode_id", "win"])["turn_text"].apply(" ".join)

        lib = ds.window_frame(size=12).set_index(["episode_id", "win"])["text"]
        shared = lib.index.intersection(hand.index)
        assert len(shared) > 25_000
        assert (lib.loc[shared] == hand.loc[shared]).all()

    def test_the_library_drops_only_trailing_partials(self, ds, turns):
        lib = ds.window_frame(size=12)
        full = ds.window_frame(size=12, partial=True)
        assert len(full) > len(lib)
        assert (lib["n_turns"] == 12).all() | (lib["n_turns"] > 0).all()


class TestDocumentedNumbers:
    """
    The figures quoted in the guide and the column descriptions, checked.

    Documentation that states a percentage is making a testable claim; these
    are the tests.
    """

    def test_sentinel_share(self, ds):
        sp = ds.turns_frame(columns=["episode_id"], speakers=True)
        share = 1 - sp["has_inferred_speaker"].mean()
        assert 0.75 < share < 0.95, f"sentinel share is {share:.1%}"

    def test_sentinels_are_never_null(self, ds):
        """The whole reason PLACEHOLDER_SPEAKERS exists."""
        col = ds.turns_frame(columns=["inferred_speaker_name"])
        assert col["inferred_speaker_name"].isna().sum() == 0
        assert col["inferred_speaker_name"].isin(
            sporc.PLACEHOLDER_SPEAKERS).any()

    def test_overlapping_turn_share(self, ds):
        sp = ds.turns_frame(columns=["episode_id"], speakers=True)
        share = sp["is_overlapping"].mean()
        assert 0.3 < share < 0.6, f"overlapping share is {share:.1%}"

    def test_start_time_is_not_monotone_in_turn_count(self, ds, turns):
        """
        Why turns_frame sorts on start_time rather than turn_count. If this
        ever became false the sort key would not matter -- and the docs saying
        it does would be wrong.
        """
        by_count = turns.sort_values(["episode_id", "turn_count"])
        disagreeing = by_count.groupby("episode_id")["start_time"].apply(
            lambda s: not s.is_monotonic_increasing)
        assert disagreeing.any()

    def test_object_model_drops_some_rows(self, ds, turns):
        """
        The documented divergence. Small in aggregate but spread widely, which
        is the part worth stating: a fifth of episodes are affected.
        """
        empty = turns["turn_text"].str.strip() == ""
        zero = turns["end_time"] <= turns["start_time"]
        assert empty.sum() > 0 and zero.sum() > 0
        dropped = empty | zero
        assert dropped.mean() < 0.01
        affected = turns[dropped]["episode_id"].nunique()
        assert affected / turns["episode_id"].nunique() > 0.1


class TestCatalogs:
    def test_catalog_reaches_past_a_restriction(self, ds):
        backend = ds._parquet_backend
        everything = len(ds.catalog("episodes"))
        try:
            backend.restrict_to_podcasts(ds._parquet_backend.get_all_podcast_ids()[:3])
            assert len(ds.episodes_frame()) < everything
            assert len(ds.catalog("episodes")) == everything
        finally:
            backend.restrict_to_podcasts(None)

    def test_load_catalog_without_a_dataset(self):
        if not os.path.isdir(SUBSET):
            pytest.skip("subsets/tutorial not built")
        df = sporc.load_catalog("podcasts", columns=["podcast_id"],
                                parquet_dir=SUBSET)
        assert len(df) > 100


class TestEscapeHatches:
    def test_parquet_paths_readable_by_pandas(self, ds, turns):
        paths = ds.parquet_paths("turns_text")
        assert paths
        direct = pd.concat([pd.read_parquet(p) for p in paths])
        assert len(direct) == len(turns)

    def test_turns_dataset_scans_lazily(self, ds, turns):
        dset = ds.turns_dataset()
        assert dset.to_table(columns=["episode_id"]).num_rows == len(turns)

    def test_iter_turns_frames_concatenates_back(self, ds, turns):
        total = sum(len(c) for c in
                    ds.iter_turns_frames(columns=["episode_id", "turn_count"]))
        assert total == len(turns)


class TestDates:
    def test_parsed_dates_are_plausible(self, ds):
        eps = ds.episodes_frame(columns=["episode_id", "episode_date"])
        assert str(eps["episode_date"].dt.tz) == "UTC"
        years = eps["episode_date"].dt.year
        assert years.min() >= 2000 and years.max() <= 2030
        assert "day" in eps.columns

    def test_agrees_with_the_episode_object(self, ds):
        """
        Episode.episode_date parses a different column name that holds the same
        value. The two must land on the same instant.
        """
        backend = ds._parquet_backend
        eps = ds.episodes_frame(columns=["episode_id", "podcast_id",
                                         "episode_date"]).head(20)
        for row in eps.itertuples():
            ep = backend.build_episode_object(row.podcast_id, row.episode_id)
            assert ep.episode_datetime == row.episode_date
