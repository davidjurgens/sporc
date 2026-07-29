"""
Conversation windows, built columnar.

The load-bearing test here is the equivalence property: for every episode and
every (size, overlap), window_frame must produce the windows that
Episode.sliding_window produces. That is what makes the fast route a substitute
for the slow one rather than a second, subtly different answer -- and it is what
forces the sort-order question to be settled rather than papered over.
"""

import pandas as pd
import pytest

from conftest import (
    EID_LONG, EID_WITH_TURNS, LONG_TURN_COUNT, PID_TURNS_2, PID_WITH_TURNS,
)
from sporc import frames
from sporc.parquet_backend import ParquetBackend

# Sizes and overlaps spanning the interesting cases: no overlap, heavy overlap,
# a window larger than the episode, and a step that does not divide the length.
GRID = [
    (1, 0), (3, 0), (4, 0), (7, 0), (12, 0), (30, 0), (60, 0),
    (3, 1), (3, 2), (4, 2), (5, 3), (12, 6), (12, 11), (30, 15), (60, 30),
]


@pytest.fixture
def backend(tmp_parquet_layout):
    return ParquetBackend(tmp_parquet_layout)


def _reference(backend, podcast_id, episode_id, size, overlap):
    """The windows Episode.sliding_window yields, as plain tuples."""
    ep = backend.build_episode_object(podcast_id, episode_id,
                                      include_turns=True)
    return [(w.start_index, w.end_index, w.size,
             tuple(t.turn_count for t in w.turns))
            for w in ep.sliding_window(size, overlap=overlap)]


def _comparable_turns(backend, podcast_id):
    """
    A turns frame holding exactly the turns the object model can represent.

    Building Episode.turns skips rows with empty text and rows with
    end_time <= start_time. The frame keeps both, which is faithful to the
    file. Comparing the two routes therefore means removing that known
    difference first -- otherwise every window after a skipped turn is offset
    by one and the real question, whether the windowing arithmetic agrees, is
    buried.
    """
    df = frames.turns_frame(backend, podcast_ids=[podcast_id])
    keep = ((df["turn_text"].fillna("").str.strip() != "")
            & (df["end_time"] > df["start_time"]))
    return df[keep].reset_index(drop=True)


class TestMatchesSlidingWindow:
    """The acceptance gate."""

    @pytest.mark.parametrize("size,overlap", GRID)
    def test_same_windows_as_the_object_model(self, backend, size, overlap):
        expected = _reference(backend, PID_TURNS_2, EID_LONG, size, overlap)

        turns = _comparable_turns(backend, PID_TURNS_2)
        df = frames.window_frame_from_turns(turns, size=size, overlap=overlap,
                                            sort=False)
        assert len(df) == len(expected), (
            f"window count differs at size={size} overlap={overlap}")

        order = list(turns["turn_count"])
        for row, (start, end, n, counts) in zip(df.itertuples(), expected):
            assert (row.start_index, row.end_index, row.n_turns) == (
                start, end, n)
            assert tuple(order[row.start_index:row.end_index]) == counts

    @pytest.mark.parametrize("size,overlap", [(1, 0), (3, 0), (3, 1), (12, 6)])
    def test_short_episode_yields_one_window(self, backend, size, overlap):
        """
        The two-turn episode is shorter than most window sizes. sliding_window
        emits one short window there, and window_frame must do the same.
        """
        expected = _reference(backend, PID_WITH_TURNS, EID_WITH_TURNS,
                              size, overlap)
        df = frames.window_frame(backend, size=size, overlap=overlap,
                                 episode_ids=[EID_WITH_TURNS])
        assert len(df) == len(expected)

    def test_text_matches(self, backend):
        ep = backend.build_episode_object(PID_TURNS_2, EID_LONG,
                                          include_turns=True)
        expected = [w.get_text() for w in ep.sliding_window(5, overlap=2)]
        turns = _comparable_turns(backend, PID_TURNS_2)
        df = frames.window_frame_from_turns(turns, size=5, overlap=2,
                                            sort=False)
        assert list(df["text"]) == expected


class TestEmptyTextTurnsDiverge:
    """
    The one place the two routes legitimately disagree, pinned down.

    Not a bug being tolerated: it is the object model that loses rows, and the
    frame that is faithful to the file. Recorded here so the difference is a
    documented property rather than a surprise in someone's turn counts.
    """

    def test_the_frame_keeps_what_the_object_model_drops(self, backend):
        df = frames.turns_frame(backend, podcast_ids=[PID_TURNS_2])
        turns = backend.build_episode_object(
            PID_TURNS_2, EID_LONG, include_turns=True).turns
        assert len(df) == LONG_TURN_COUNT
        # Two rows skipped: turn 12 has empty text, turn 20 zero duration.
        assert len(turns) == LONG_TURN_COUNT - 2
        assert (df["turn_text"] == "").sum() == 1
        assert (df["end_time"] <= df["start_time"]).sum() == 1
        assert {12, 20}.isdisjoint({t.turn_count for t in turns})

    def test_window_text_joins_empties_rather_than_skipping(self, backend):
        """
        An empty turn contributes an empty string to the join, so the text is
        the plain concatenation of the window's rows -- what `" ".join` over
        the same frame gives, and consistent with n_turns.
        """
        df = frames.window_frame(backend, size=30, podcast_ids=[PID_TURNS_2])
        assert df["n_turns"].iloc[0] == LONG_TURN_COUNT
        assert "turn 11 text  turn 13 text" in df["text"].iloc[0]


class TestOverlap:
    def test_cumcount_shortcut_is_wrong_under_overlap(self, backend):
        """
        Guards against a regression to `cumcount() // size`, which looks right
        and silently produces non-overlapping windows. This is the case the
        request asked the library to own.
        """
        turns = frames.turns_frame(backend, podcast_ids=[PID_TURNS_2])
        shortcut = turns.groupby("episode_id").cumcount() // 12
        n_shortcut = shortcut.nunique()

        df = frames.window_frame(backend, size=12, overlap=6,
                                 podcast_ids=[PID_TURNS_2])
        assert len(df) != n_shortcut
        assert len(df) == 4     # 30 turns, step 6: starts at 0, 6, 12, 18

    def test_windows_actually_share_turns(self, backend):
        df = frames.window_frame(backend, size=12, overlap=6,
                                 podcast_ids=[PID_TURNS_2])
        first, second = df.iloc[0], df.iloc[1]
        assert second.start_index == first.start_index + 6
        assert second.start_index < first.end_index

    def test_zero_overlap_matches_the_cumcount_idiom(self, backend):
        """With no overlap the hand-rolled version is right, and must agree."""
        turns = frames.turns_frame(backend, podcast_ids=[PID_TURNS_2])
        turns["win"] = turns.groupby("episode_id").cumcount() // 10
        expected = turns.groupby("win")["turn_text"].apply(" ".join)

        df = frames.window_frame(backend, size=10, overlap=0,
                                 podcast_ids=[PID_TURNS_2])
        assert list(df["text"]) == list(expected)


class TestPartial:
    def test_default_drops_the_trailing_window(self, backend):
        """30 turns at size 7 covers 28; sliding_window drops the last two."""
        df = frames.window_frame(backend, size=7, podcast_ids=[PID_TURNS_2])
        assert len(df) == 4
        assert df["end_index"].max() == 28

    def test_partial_keeps_it(self, backend):
        df = frames.window_frame(backend, size=7, partial=True,
                                 podcast_ids=[PID_TURNS_2])
        assert len(df) == 5
        assert df["end_index"].max() == LONG_TURN_COUNT
        assert df["n_turns"].iloc[-1] == 2

    def test_partial_adds_nothing_when_it_divides_evenly(self, backend):
        a = frames.window_frame(backend, size=10, podcast_ids=[PID_TURNS_2])
        b = frames.window_frame(backend, size=10, partial=True,
                                podcast_ids=[PID_TURNS_2])
        assert len(a) == len(b) == 3


class TestOutput:
    def test_columns(self, backend):
        df = frames.window_frame(backend, size=5, podcast_ids=[PID_TURNS_2])
        assert {"episode_id", "podcast_id", "win", "n_turns", "start_index",
                "end_index", "start_time", "end_time", "duration",
                "n_unique_speakers", "n_host_turns", "n_guest_turns",
                "text"} <= set(df.columns)

    def test_timings_match_the_turn_window(self, backend):
        ep = backend.build_episode_object(PID_TURNS_2, EID_LONG,
                                          include_turns=True)
        expected = [(w.time_range[0], w.time_range[1], w.duration)
                    for w in ep.sliding_window(6)]
        df = frames.window_frame_from_turns(
            _comparable_turns(backend, PID_TURNS_2), size=6, sort=False)
        got = list(zip(df["start_time"], df["end_time"], df["duration"]))
        assert got == pytest.approx(expected)

    def test_speaker_counts(self, backend):
        df = frames.window_frame(backend, size=30, podcast_ids=[PID_TURNS_2])
        assert len(df) == 1
        row = df.iloc[0]
        assert row["n_unique_speakers"] == 2
        assert row["n_host_turns"] == 6      # every fifth turn
        assert row["n_guest_turns"] == 6

    def test_text_false_omits_the_column(self, backend):
        df = frames.window_frame(backend, size=5, text=False,
                                 podcast_ids=[PID_TURNS_2])
        assert "text" not in df.columns

    def test_extra_columns_are_averaged(self, backend):
        df = frames.window_frame(backend, size=5, columns=["token_count"],
                                 podcast_ids=[PID_TURNS_2])
        # Every fixture turn has token_count 3, so the per-window mean is 3.
        assert df["token_count"].eq(3.0).all()

    def test_windows_span_episodes_independently(self, backend):
        df = frames.window_frame(backend, size=2)
        assert set(df["episode_id"]) == {EID_WITH_TURNS, EID_LONG}
        # Windows never straddle an episode boundary.
        for eid, group in df.groupby("episode_id"):
            assert list(group["win"]) == list(range(len(group)))


class TestValidation:
    @pytest.mark.parametrize("size,overlap", [(0, 0), (-1, 0), (5, 5), (5, 6),
                                              (5, -1)])
    def test_bad_arguments_are_refused(self, backend, size, overlap):
        with pytest.raises(ValueError):
            frames.window_frame(backend, size=size, overlap=overlap,
                                podcast_ids=[PID_TURNS_2])

    def test_empty_input(self, backend):
        df = frames.window_frame(backend, size=5, podcast_ids=["ffffffffffff"])
        assert df.empty


class TestFromTurns:
    """The same routine, usable on a frame built anywhere."""

    def test_works_on_a_hand_built_frame(self):
        turns = pd.DataFrame({
            "episode_id": ["a"] * 6 + ["b"] * 3,
            "turn_count": list(range(6)) + list(range(3)),
            "start_time": [float(i) for i in range(6)] + [0.0, 1.0, 2.0],
            "end_time": [float(i) + 1 for i in range(6)] + [1.0, 2.0, 3.0],
            "turn_text": [f"t{i}" for i in range(9)],
        })
        out = frames.window_frame_from_turns(turns, size=2, overlap=0)
        assert list(out["episode_id"]) == ["a", "a", "a", "b"]
        assert list(out["text"]) == ["t0 t1", "t2 t3", "t4 t5", "t6 t7"]

    def test_custom_group_column(self):
        turns = pd.DataFrame({
            "speaker_id": ["x", "x", "x", "y"],
            "turn_count": [0, 1, 2, 0],
            "turn_text": ["a", "b", "c", "d"],
        })
        out = frames.window_frame_from_turns(turns, size=2, group="speaker_id")
        assert list(out["speaker_id"]) == ["x", "y"]

    def test_missing_group_column_is_refused(self):
        with pytest.raises(ValueError, match="episode_id"):
            frames.window_frame_from_turns(
                pd.DataFrame({"turn_count": [0, 1]}), size=2)
