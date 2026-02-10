"""
Tests for sliding window functionality in the Episode class.
"""

import unittest
from unittest.mock import Mock, patch
import sys
import os

# Add the parent directory to the path to import sporc
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sporc.episode import Episode, TurnWindow, TimeRangeBehavior
from sporc.turn import Turn


class TestTurnWindow(unittest.TestCase):
    """Test the TurnWindow class."""

    def setUp(self):
        """Set up test data."""
        # Create mock turns
        self.turns = [
            Turn(
                speaker=["SPEAKER_00"],
                text="Hello, welcome to the podcast.",
                start_time=0.0,
                end_time=2.5,
                duration=2.5,
                turn_count=0,
                inferred_speaker_name="Host",
                inferred_speaker_role="host"
            ),
            Turn(
                speaker=["SPEAKER_01"],
                text="Thanks for having me.",
                start_time=2.5,
                end_time=4.0,
                duration=1.5,
                turn_count=1,
                inferred_speaker_name="Guest",
                inferred_speaker_role="guest"
            ),
            Turn(
                speaker=["SPEAKER_00"],
                text="Let's talk about the topic.",
                start_time=4.0,
                end_time=7.0,
                duration=3.0,
                turn_count=2,
                inferred_speaker_name="Host",
                inferred_speaker_role="host"
            ),
            Turn(
                speaker=["SPEAKER_01"],
                text="That sounds interesting.",
                start_time=7.0,
                end_time=9.0,
                duration=2.0,
                turn_count=3,
                inferred_speaker_name="Guest",
                inferred_speaker_role="guest"
            ),
            Turn(
                speaker=["SPEAKER_00"],
                text="What do you think?",
                start_time=9.0,
                end_time=11.0,
                duration=2.0,
                turn_count=4,
                inferred_speaker_name="Host",
                inferred_speaker_role="host"
            )
        ]

        # Create a TurnWindow
        self.window = TurnWindow(
            turns=self.turns,
            window_index=0,
            start_index=0,
            end_index=5,
            total_windows=3,
            overlap_size=2
        )

    def test_basic_properties(self):
        """Test basic window properties."""
        self.assertEqual(self.window.size, 5)
        self.assertEqual(self.window.window_index, 0)
        self.assertEqual(self.window.start_index, 0)
        self.assertEqual(self.window.end_index, 5)
        self.assertEqual(self.window.total_windows, 3)
        self.assertEqual(self.window.overlap_size, 2)

    def test_window_position_properties(self):
        """Test window position properties."""
        self.assertTrue(self.window.is_first)
        self.assertFalse(self.window.is_last)
        self.assertTrue(self.window.has_overlap)

        # Test last window
        last_window = TurnWindow(
            turns=self.turns[:3],
            window_index=2,
            start_index=3,
            end_index=5,
            total_windows=3,
            overlap_size=2
        )
        self.assertFalse(last_window.is_first)
        self.assertTrue(last_window.is_last)

    def test_overlap_turns(self):
        """Test overlap turns functionality."""
        # First window should have no overlap turns
        self.assertEqual(len(self.window.overlap_turns), 0)

        # Second window should have overlap turns
        second_window = TurnWindow(
            turns=self.turns[1:],
            window_index=1,
            start_index=1,
            end_index=5,
            total_windows=3,
            overlap_size=2
        )
        self.assertEqual(len(second_window.overlap_turns), 2)
        self.assertEqual(second_window.overlap_turns[0], self.turns[1])
        self.assertEqual(second_window.overlap_turns[1], self.turns[2])

    def test_new_turns(self):
        """Test new turns functionality."""
        # First window should have all turns as new
        self.assertEqual(len(self.window.new_turns), 5)
        self.assertEqual(self.window.new_turns, self.turns)

        # Second window should have only non-overlap turns as new
        second_window = TurnWindow(
            turns=self.turns[1:],
            window_index=1,
            start_index=1,
            end_index=5,
            total_windows=3,
            overlap_size=2
        )
        self.assertEqual(len(second_window.new_turns), 2)  # 4 turns total - 2 overlap = 2 new
        self.assertEqual(second_window.new_turns, self.turns[3:])  # turns[3:] = last 2 turns

    def test_time_range(self):
        """Test time range calculation."""
        time_range = self.window.time_range
        self.assertEqual(time_range[0], 0.0)
        self.assertEqual(time_range[1], 11.0)

        # Test empty window
        empty_window = TurnWindow(
            turns=[],
            window_index=0,
            start_index=0,
            end_index=0,
            total_windows=1,
            overlap_size=0
        )
        self.assertEqual(empty_window.time_range, (0.0, 0.0))

    def test_duration(self):
        """Test duration calculation."""
        self.assertEqual(self.window.duration, 11.0)

        # Test empty window
        empty_window = TurnWindow(
            turns=[],
            window_index=0,
            start_index=0,
            end_index=0,
            total_windows=1,
            overlap_size=0
        )
        self.assertEqual(empty_window.duration, 0.0)

    def test_speaker_distribution(self):
        """Test speaker distribution calculation."""
        speaker_dist = self.window.get_speaker_distribution()
        expected = {"SPEAKER_00": 3, "SPEAKER_01": 2}
        self.assertEqual(speaker_dist, expected)

    def test_role_distribution(self):
        """Test role distribution calculation."""
        role_dist = self.window.get_role_distribution()
        expected = {"host": 3, "guest": 2}
        self.assertEqual(role_dist, expected)

    def test_get_text(self):
        """Test text combination."""
        text = self.window.get_text()
        expected = "Hello, welcome to the podcast. Thanks for having me. Let's talk about the topic. That sounds interesting. What do you think?"
        self.assertEqual(text, expected)

        # Test with custom separator
        text_with_sep = self.window.get_text(separator=" | ")
        expected_with_sep = "Hello, welcome to the podcast. | Thanks for having me. | Let's talk about the topic. | That sounds interesting. | What do you think?"
        self.assertEqual(text_with_sep, expected_with_sep)

    def test_to_dict(self):
        """Test dictionary conversion."""
        window_dict = self.window.to_dict()

        self.assertEqual(window_dict['window_index'], 0)
        self.assertEqual(window_dict['size'], 5)
        self.assertEqual(window_dict['is_first'], True)
        self.assertEqual(window_dict['is_last'], False)
        self.assertEqual(window_dict['has_overlap'], True)
        self.assertEqual(window_dict['time_range'], (0.0, 11.0))
        self.assertEqual(window_dict['duration'], 11.0)
        self.assertEqual(window_dict['num_turns'], 5)
        self.assertEqual(window_dict['num_overlap_turns'], 0)
        self.assertEqual(window_dict['num_new_turns'], 5)

    def test_string_representations(self):
        """Test string representations."""
        str_repr = str(self.window)
        self.assertIn("TurnWindow", str_repr)
        self.assertIn("index=0", str_repr)
        self.assertIn("turns=5", str_repr)

        repr_repr = repr(self.window)
        self.assertIn("TurnWindow", repr_repr)
        self.assertIn("window_index=0", repr_repr)


class TestEpisodeSlidingWindow(unittest.TestCase):
    """Test sliding window functionality in the Episode class."""

    def setUp(self):
        """Set up test episode with turns."""
        # Create a test episode
        self.episode = Episode(
            title="Test Episode",
            description="A test episode",
            mp3_url="test.mp3",
            duration_seconds=60.0,
            transcript="Test transcript",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="test.rss"
        )

        # Create test turns
        self.turns = [
            Turn(
                speaker=["SPEAKER_00"],
                text=f"Turn {i}",
                start_time=i * 2.0,
                end_time=(i + 1) * 2.0,
                duration=2.0,
                turn_count=i,
                inferred_speaker_name=f"Speaker_{i % 2}",
                inferred_speaker_role="host" if i % 2 == 0 else "guest"
            )
            for i in range(10)  # 10 turns
        ]

        # Load turns directly into episode
        self.episode._turns = sorted(self.turns, key=lambda t: t.start_time)
        self.episode._turns_loaded = True

    def test_sliding_window_basic(self):
        """Test basic sliding window functionality."""
        windows = list(self.episode.sliding_window(window_size=3, overlap=1))

        self.assertEqual(len(windows), 4)  # Should create 4 windows

        # Check first window
        first_window = windows[0]
        self.assertEqual(first_window.window_index, 0)
        self.assertEqual(first_window.size, 3)
        self.assertEqual(first_window.start_index, 0)
        self.assertEqual(first_window.end_index, 3)
        self.assertEqual(first_window.overlap_size, 0)  # First window has no overlap
        self.assertEqual(len(first_window.new_turns), 3)
        self.assertEqual(len(first_window.overlap_turns), 0)

        # Check second window
        second_window = windows[1]
        self.assertEqual(second_window.window_index, 1)
        self.assertEqual(second_window.size, 3)
        self.assertEqual(second_window.start_index, 2)
        self.assertEqual(second_window.end_index, 5)
        self.assertEqual(second_window.overlap_size, 1)
        self.assertEqual(len(second_window.new_turns), 2)
        self.assertEqual(len(second_window.overlap_turns), 1)

    def test_sliding_window_no_overlap(self):
        """Test sliding window with no overlap."""
        windows = list(self.episode.sliding_window(window_size=3, overlap=0))

        self.assertEqual(len(windows), 3)  # Should create 3 windows (10 turns, size 3, step 3)

        for i, window in enumerate(windows):
            self.assertEqual(window.window_index, i)
            self.assertEqual(window.size, 3)
            self.assertEqual(window.overlap_size, 0 if i == 0 else 0)
            self.assertEqual(len(window.new_turns), 3)
            self.assertEqual(len(window.overlap_turns), 0)

    def test_sliding_window_high_overlap(self):
        """Test sliding window with high overlap."""
        windows = list(self.episode.sliding_window(window_size=5, overlap=3))

        self.assertEqual(len(windows), 3)  # Should create 3 windows (10 turns, size 5, step 2)

        # Check overlap behavior
        for i, window in enumerate(windows[1:], 1):
            self.assertEqual(window.overlap_size, 3)
            self.assertEqual(len(window.overlap_turns), 3)
            self.assertEqual(len(window.new_turns), 2)

    def test_sliding_window_validation(self):
        """Test sliding window parameter validation."""
        # Test invalid window size
        with self.assertRaises(ValueError):
            list(self.episode.sliding_window(window_size=0, overlap=0))

        # Test invalid overlap
        with self.assertRaises(ValueError):
            list(self.episode.sliding_window(window_size=5, overlap=-1))

        # Test window size <= overlap
        with self.assertRaises(ValueError):
            list(self.episode.sliding_window(window_size=5, overlap=5))

        # Test invalid start/end indices
        with self.assertRaises(ValueError):
            list(self.episode.sliding_window(window_size=3, overlap=0, start_index=-1))

        with self.assertRaises(ValueError):
            list(self.episode.sliding_window(window_size=3, overlap=0, end_index=100))

    def test_sliding_window_by_time(self):
        """Test time-based sliding window."""
        windows = list(self.episode.sliding_window_by_time(
            window_duration=6.0,  # 3 turns worth of time
            overlap_duration=2.0   # 1 turn worth of overlap
        ))

        self.assertGreater(len(windows), 0)

        for window in windows:
            self.assertGreaterEqual(window.duration, 0)
            self.assertGreaterEqual(len(window.turns), 0)

    def test_sliding_window_by_time_validation(self):
        """Test time-based sliding window validation."""
        # Test invalid window duration
        with self.assertRaises(ValueError):
            list(self.episode.sliding_window_by_time(window_duration=0))

        # Test invalid overlap duration
        with self.assertRaises(ValueError):
            list(self.episode.sliding_window_by_time(window_duration=10, overlap_duration=-1))

        # Test window duration <= overlap duration
        with self.assertRaises(ValueError):
            list(self.episode.sliding_window_by_time(window_duration=5, overlap_duration=5))

    def test_get_window_statistics(self):
        """Test window statistics calculation."""
        stats = self.episode.get_window_statistics(window_size=3, overlap=1)

        self.assertEqual(stats['total_turns'], 10)
        self.assertEqual(stats['window_size'], 3)
        self.assertEqual(stats['overlap'], 1)
        self.assertEqual(stats['step_size'], 2)
        self.assertGreater(stats['total_windows'], 0)
        self.assertGreater(stats['avg_window_duration'], 0)
        self.assertGreater(stats['avg_turn_duration'], 0)

    def test_get_window_statistics_validation(self):
        """Test window statistics validation."""
        with self.assertRaises(ValueError):
            self.episode.get_window_statistics(window_size=3, overlap=3)

    def test_sliding_window_without_turns_loaded(self):
        """Test that sliding window fails when turns are not loaded."""
        episode_without_turns = Episode(
            title="Test Episode",
            description="A test episode",
            mp3_url="test.mp3",
            duration_seconds=60.0,
            transcript="Test transcript",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="test.rss"
        )

        with self.assertRaises(RuntimeError):
            list(episode_without_turns.sliding_window(window_size=3, overlap=1))

        with self.assertRaises(RuntimeError):
            list(episode_without_turns.sliding_window_by_time(window_duration=10))

        with self.assertRaises(RuntimeError):
            episode_without_turns.get_window_statistics(window_size=3, overlap=1)


if __name__ == '__main__':
    unittest.main()