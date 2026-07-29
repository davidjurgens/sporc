"""
The columnar API: turns_frame and friends.

Run against the real (tiny) Parquet layout from tmp_parquet_layout rather than
mocks, because most of what can go wrong here is about the files: which part a
podcast lives in, whether a projection reaches the right columns, whether a join
key is unique.
"""

import logging

import pandas as pd
import pytest

from conftest import (
    EID_LONG, EID_WITH_TURNS, LONG_TURN_COUNT, PID_NO_TURNS, PID_TURNS_2,
    PID_WITH_TURNS,
)
from sporc import frames
from sporc.exceptions import FrameTooLargeError
from sporc.parquet_backend import ParquetBackend


@pytest.fixture
def backend(tmp_parquet_layout):
    return ParquetBackend(tmp_parquet_layout)


TOTAL_TURNS = 2 + LONG_TURN_COUNT


class TestTurnsFrame:
    def test_returns_every_turn(self, backend):
        df = frames.turns_frame(backend)
        assert len(df) == TOTAL_TURNS
        assert set(df["podcast_id"]) == {PID_WITH_TURNS, PID_TURNS_2}
        # The podcast with no turns partition contributes nothing, and that is
        # not an error -- roughly a third of the real corpus is like this.
        assert PID_NO_TURNS not in set(df["podcast_id"])

    def test_agrees_with_the_object_model(self, backend):
        """
        The two routes must return the same turns in the same order.

        This is the property that makes the frame API a substitute for walking
        objects rather than a second, subtly different answer.
        """
        df = frames.turns_frame(backend)
        mine = df[df["episode_id"] == EID_LONG]

        rows = backend.get_turns_for_episode(PID_TURNS_2, EID_LONG)
        assert len(mine) == len(rows)
        # get_turns_for_episode sorts by start_time, and so does turns_frame.
        assert list(mine["turn_count"]) == [r["turn_count"] for r in rows]
        assert list(mine["turn_text"]) == [r["turn_text"] for r in rows]

    def test_sorted_by_time_not_by_turn_count(self, backend):
        """
        The fixture inverts start_time for turns 7 and 8, as ~3.7% of real
        episodes do. Sorting on turn_count -- the obvious choice, since it is
        the join key -- would put them the other way round.
        """
        df = frames.turns_frame(backend)
        mine = df[df["episode_id"] == EID_LONG].reset_index(drop=True)
        assert mine["start_time"].is_monotonic_increasing
        assert list(mine["turn_count"][6:9]) == [6, 8, 7]

    def test_sort_false_gives_file_order(self, backend):
        df = frames.turns_frame(backend, sort=False)
        mine = df[df["episode_id"] == EID_LONG]
        assert list(mine["turn_count"]) == list(range(LONG_TURN_COUNT))

    def test_is_not_cached(self, backend):
        """
        Notebooks mutate what they get back, so two calls must not share an
        object -- otherwise a cell's result depends on whether an earlier cell
        ran.
        """
        first = frames.turns_frame(backend)
        first["scratch"] = 1
        second = frames.turns_frame(backend)
        assert "scratch" not in second.columns


class TestColumnProjection:
    def test_returns_only_what_was_asked_for(self, backend):
        df = frames.turns_frame(backend, columns=["episode_id", "turn_text"])
        assert list(df.columns) == ["episode_id", "turn_text", "start_time"]

    def test_start_time_is_added_back_for_sorting(self, backend):
        df = frames.turns_frame(backend, columns=["episode_id"])
        assert "start_time" in df.columns

    def test_sort_false_keeps_the_projection_exact(self, backend):
        df = frames.turns_frame(backend, columns=["episode_id", "turn_text"],
                                sort=False)
        assert list(df.columns) == ["episode_id", "turn_text"]

    def test_join_keys_are_added_back(self, backend):
        df = frames.turns_frame(backend, columns=["turn_text"], metrics=True)
        assert {"episode_id", "turn_count"} <= set(df.columns)

    def test_unknown_column_fails_before_reading(self, backend):
        with pytest.raises(ValueError) as exc:
            frames.turns_frame(backend, columns=["turn_txt"])
        assert "turn_text" in str(exc.value)


class TestJoins:
    def test_metrics_left_join_preserves_rows(self, backend):
        """
        PID_TURNS_2 has turns but no metrics. An inner join would delete all 30
        of them silently; a left join keeps them with NaN, which is visible.
        """
        plain = frames.turns_frame(backend)
        joined = frames.turns_frame(backend, metrics=True)
        assert len(joined) == len(plain) == TOTAL_TURNS
        assert "word_count" in joined.columns
        missing = joined[joined["episode_id"] == EID_LONG]["word_count"]
        assert missing.isna().all()

    def test_acoustics_duplicate_rows_do_not_fan_out(self, backend):
        """
        The fixture stores turn 0's acoustics twice, verbatim, as 81,807 turns
        in the real corpus are. Merging without de-duplicating multiplies rows.
        """
        joined = frames.turns_frame(backend, acoustics=True)
        assert len(joined) == TOTAL_TURNS
        got = joined[(joined["episode_id"] == EID_WITH_TURNS)
                     & (joined["turn_count"] == 0)]
        assert len(got) == 1
        assert got["mfcc1_sma3_mean"].iloc[0] == 1.0

    def test_both_joins_together(self, backend):
        joined = frames.turns_frame(backend, metrics=True, acoustics=True)
        assert len(joined) == TOTAL_TURNS
        assert {"word_count", "mfcc1_sma3_mean"} <= set(joined.columns)

    def test_partial_coverage_is_logged(self, backend, caplog):
        """A left join hides missing rows as NaN, so it has to say so."""
        with caplog.at_level(logging.INFO, logger="sporc.frames"):
            frames.turns_frame(backend, acoustics=True)
        assert "have no acoustics row" in caplog.text


class TestSpeakerColumns:
    def test_matches_the_turn_properties(self, backend):
        """
        The vectorised helpers must agree with Turn's, row for row -- they are
        the same predicates and people will mix the two routes.
        """
        df = frames.turns_frame(backend, speakers=True)
        df = df[df["episode_id"] == EID_LONG]
        # The object model skips empty-text and zero-duration turns, so drop
        # those rows to line the two up. See TestEmptyTextTurnsDiverge in
        # test_window_frame.py for the difference itself.
        df = df[(df["turn_text"] != "")
                & (df["end_time"] > df["start_time"])].reset_index(drop=True)
        turns = backend.build_episode_object(PID_TURNS_2, EID_LONG,
                                             include_turns=True).turns

        assert len(df) == len(turns)
        for row, turn in zip(df.itertuples(), turns):
            assert row.is_overlapping == turn.is_overlapping
            assert row.is_host == turn.is_host
            assert row.is_guest == turn.is_guest
            assert row.has_inferred_speaker == turn.has_inferred_speaker
            assert row.has_inferred_role == turn.has_inferred_role
            expected = turn.primary_speaker
            got = row.primary_speaker
            assert (got == expected) or (expected is None and pd.isna(got))

    def test_counts_overlapping_and_empty_speakers(self, backend):
        df = frames.turns_frame(backend, speakers=True)
        mine = df[df["episode_id"] == EID_LONG]
        assert (mine["n_speakers"] == 2).sum() == 1   # the overlapping turn
        assert (mine["n_speakers"] == 0).sum() == 1   # attributed to nobody

    def test_sentinels_are_not_treated_as_speakers(self, backend):
        """The bug this whole API exists to prevent: sentinels are not nulls."""
        df = frames.turns_frame(backend, speakers=True)
        mine = df[df["episode_id"] == EID_LONG]
        assert mine["inferred_speaker_name"].notna().all()
        assert not mine["has_inferred_speaker"].all()
        assert mine["has_inferred_speaker"].sum() == 12   # 6 hosts + 6 guests

    def test_works_on_a_frame_from_anywhere(self):
        df = pd.DataFrame({
            "speaker": [["A"], ["A", "B"], []],
            "inferred_speaker_name": ["Real", "NO_INFERRED_SPEAKER", None],
            "inferred_speaker_role": ["host", "NO_INFERRED_ROLE", "guest"],
        })
        out = frames.add_speaker_columns(df)
        assert list(out["is_overlapping"]) == [False, True, False]
        assert list(out["has_inferred_speaker"]) == [True, False, False]
        assert list(out["is_guest"]) == [False, False, True]
        # The input is untouched by default.
        assert "is_host" not in df.columns


class TestRestriction:
    def test_frames_honour_subset_pinning(self, backend):
        backend.restrict_to_podcasts([PID_TURNS_2])
        df = frames.turns_frame(backend)
        assert set(df["podcast_id"]) == {PID_TURNS_2}
        assert len(df) == LONG_TURN_COUNT

    def test_podcast_ids_narrows_within_a_shared_part(self, backend):
        """
        Both podcasts with turns share one part file, so this is a real filter
        rather than a choice of which file to open.
        """
        df = frames.turns_frame(backend, podcast_ids=[PID_WITH_TURNS])
        assert set(df["podcast_id"]) == {PID_WITH_TURNS}
        assert len(df) == 2

    def test_podcast_ids_cannot_escape_a_restriction(self, backend):
        backend.restrict_to_podcasts([PID_TURNS_2])
        df = frames.turns_frame(backend, podcast_ids=[PID_WITH_TURNS])
        assert df.empty

    def test_episode_ids_narrows_rows(self, backend):
        df = frames.turns_frame(backend, episode_ids=[EID_WITH_TURNS])
        assert set(df["episode_id"]) == {EID_WITH_TURNS}

    def test_catalog_ignores_the_restriction(self, backend):
        """
        The reason catalog() exists: a cross-genre question needs the whole
        index while the loaded dataset is one slice.
        """
        backend.restrict_to_podcasts([PID_TURNS_2])
        assert len(frames.episodes_frame(backend)) == 1
        assert len(frames.catalog_frame(backend, "episodes")) == 4


class TestGuard:
    def test_refuses_an_oversized_request(self, backend, monkeypatch):
        monkeypatch.setattr(frames, "FRAME_WARN_BYTES", 1)
        monkeypatch.setattr(frames, "FRAME_MAX_BYTES", 2)
        with pytest.raises(FrameTooLargeError) as exc:
            frames.turns_frame(backend)
        msg = str(exc.value)
        assert "turn_text" in msg           # names the widest column
        assert "columns=" in msg            # and every way out
        assert "allow_large=True" in msg
        assert "iter_turns_frames" in msg

    def test_allow_large_overrides(self, backend, monkeypatch):
        monkeypatch.setattr(frames, "FRAME_MAX_BYTES", 2)
        assert len(frames.turns_frame(backend, allow_large=True)) == TOTAL_TURNS

    def test_narrow_columns_pass_where_wide_ones_fail(self, backend, monkeypatch):
        """The point of estimating bytes rather than counting rows."""
        monkeypatch.setattr(frames, "FRAME_WARN_BYTES", 1)
        monkeypatch.setattr(frames, "FRAME_MAX_BYTES", 32 * TOTAL_TURNS)
        with pytest.raises(FrameTooLargeError):
            frames.turns_frame(backend)
        assert not frames.turns_frame(
            backend, columns=["turn_count", "start_time"]).empty

    def test_warns_between_the_thresholds(self, backend, monkeypatch, caplog):
        monkeypatch.setattr(frames, "FRAME_WARN_BYTES", 1)
        with caplog.at_level(logging.WARNING, logger="sporc.frames"):
            frames.turns_frame(backend)
        assert "would materialize" in caplog.text


class TestIterTurnsFrames:
    def test_concatenates_back_to_turns_frame(self, backend):
        chunks = list(frames.iter_turns_frames(backend))
        assert chunks
        combined = pd.concat(chunks, ignore_index=True)
        whole = frames.turns_frame(backend)
        assert len(combined) == len(whole)
        assert set(combined["turn_text"]) == set(whole["turn_text"])

    def test_needs_no_guard(self, backend, monkeypatch):
        monkeypatch.setattr(frames, "FRAME_MAX_BYTES", 1)
        assert sum(len(c) for c in frames.iter_turns_frames(backend)) == TOTAL_TURNS

    def test_joins_within_a_chunk(self, backend):
        chunks = list(frames.iter_turns_frames(backend, metrics=True))
        combined = pd.concat(chunks, ignore_index=True)
        assert len(combined) == TOTAL_TURNS
        assert "word_count" in combined.columns

    def test_yields_nothing_for_a_podcast_without_turns(self, backend):
        assert list(frames.iter_turns_frames(
            backend, podcast_ids=[PID_NO_TURNS])) == []


class TestEpisodesFrame:
    def test_reads_the_catalog_not_the_tree(self, backend):
        df = frames.episodes_frame(backend)
        assert len(df) == 4
        # The catalog has no transcripts; that column is why the tree is 16 GB.
        assert "transcript" not in df.columns

    def test_parses_the_millisecond_epoch(self, backend):
        df = frames.episodes_frame(backend)
        assert df["episode_date"].dt.year.eq(2020).all()
        assert str(df["episode_date"].dt.tz) == "UTC"
        assert "day" in df.columns

    def test_the_trap_this_replaces(self):
        """
        Recorded so the reason for parse_dates=True does not get lost.

        The column is a string holding milliseconds. Casting it to an integer
        and parsing -- the obvious thing to do -- reads the value as
        nanoseconds and silently yields 1970. Nothing raises.
        """
        raw = pd.Series(["1577836800000"])
        assert pd.to_datetime(raw.astype("Int64")).dt.year.iloc[0] == 1970
        assert pd.to_datetime(raw.astype("Int64"), unit="ms",
                              utc=True).dt.year.iloc[0] == 2020

    def test_parse_dates_can_be_turned_off(self, backend):
        df = frames.episodes_frame(backend, parse_dates=False)
        assert df["episode_date"].iloc[0] == "1577836800000"

    def test_does_not_poison_the_backend_catalog(self, backend):
        """
        The frame comes from the backend's own cached catalog. Returning a view
        would let a notebook cell change what search_episodes() sees.
        """
        before = len(backend.search_episodes())
        df = frames.episodes_frame(backend)
        df["scratch"] = 1
        df.loc[:, "ep_title"] = "clobbered"
        assert len(backend.search_episodes()) == before
        assert all(e["ep_title"] != "clobbered"
                   for e in backend.search_episodes())

    def test_metrics_join(self, backend):
        df = frames.episodes_frame(backend, metrics=True)
        assert "total_word_count" in df.columns
        assert len(df) == 4


class TestPodcastsAndMetricsFrames:
    def test_podcasts_frame(self, backend):
        df = frames.podcasts_frame(backend)
        assert len(df) == 3
        assert set(df["podcast_id"]) == {PID_WITH_TURNS, PID_NO_TURNS,
                                         PID_TURNS_2}

    def test_podcasts_frame_projection(self, backend):
        df = frames.podcasts_frame(backend, columns=["podcast_id", "pod_title"])
        assert list(df.columns) == ["podcast_id", "pod_title"]

    def test_podcasts_frame_does_not_poison_the_cache(self, backend):
        df = frames.podcasts_frame(backend)
        df.loc[:, "pod_title"] = "clobbered"
        assert backend.get_podcast_by_id(PID_WITH_TURNS)["pod_title"] != "clobbered"

    def test_podcasts_frame_honours_restriction(self, backend):
        backend.restrict_to_podcasts([PID_TURNS_2])
        assert list(frames.podcasts_frame(backend)["podcast_id"]) == [PID_TURNS_2]

    def test_episode_metrics_frame(self, backend):
        df = frames.episode_metrics_frame(backend)
        assert "total_word_count" in df.columns


class TestTurnsDataset:
    def test_scans_lazily(self, backend):
        dset = frames.turns_dataset(backend)
        table = dset.to_table(columns=["episode_id"])
        assert table.num_rows == TOTAL_TURNS

    def test_raises_when_nothing_is_available(self, backend):
        with pytest.raises(FileNotFoundError):
            frames.turns_dataset(backend, podcast_ids=[PID_NO_TURNS])


class TestParquetPaths:
    def test_returns_real_files(self, backend):
        import os

        paths = backend.part_paths("turns_text")
        assert paths and all(os.path.exists(p) for p in paths)

    def test_readable_with_plain_pandas(self, backend):
        """The escape hatch has to actually work with the obvious tool."""
        paths = backend.part_paths("turns_text")
        df = pd.concat([pd.read_parquet(p) for p in paths])
        assert len(df) == TOTAL_TURNS
