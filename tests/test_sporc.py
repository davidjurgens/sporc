"""
Test suite for the SPORC package.
"""

import pytest
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from sporc import (
    SPORCDataset,
    Podcast,
    Episode,
    Turn,
    SPORCError,
    AuthenticationError,
    DatasetAccessError,
    NotFoundError
)
from sporc.episode import TimeRangeBehavior


class TestTurn:
    """Test the Turn class."""

    def test_turn_creation(self):
        """Test creating a basic Turn object."""
        turn = Turn(
            speaker=["SPEAKER_00"],
            text="Hello, this is a test turn.",
            start_time=0.0,
            end_time=5.0,
            duration=5.0,
            turn_count=1
        )

        assert turn.primary_speaker == "SPEAKER_00"
        assert turn.word_count == 6
        assert turn.duration == 5.0
        assert not turn.is_overlapping
        assert turn.words_per_second == 1.2

    def test_turn_with_audio_features(self):
        """Test creating a Turn with audio features."""
        turn = Turn(
            speaker=["SPEAKER_00"],
            text="Test turn with audio features.",
            start_time=0.0,
            end_time=3.0,
            duration=3.0,
            turn_count=1,
            mfcc1_sma3_mean=20.5,
            mfcc2_sma3_mean=13.4,
            f0_semitone_from_27_5hz_sma3nz_mean=13.97,
            f1_frequency_sma3nz_mean=717.3
        )

        features = turn.get_audio_features()
        assert 'mfcc1_sma3_mean' in features
        assert 'mfcc2_sma3_mean' in features
        assert 'f0_semitone_from_27_5hz_sma3nz_mean' in features
        assert 'f1_frequency_sma3nz_mean' in features

    def test_overlapping_turn(self):
        """Test a turn with multiple speakers (overlapping)."""
        turn = Turn(
            speaker=["SPEAKER_00", "SPEAKER_01"],
            text="Overlapping speech.",
            start_time=0.0,
            end_time=2.0,
            duration=2.0,
            turn_count=1
        )

        assert turn.is_overlapping
        assert turn.primary_speaker == "SPEAKER_00"

    def test_turn_validation(self):
        """Test turn validation."""
        # Test negative start time
        with pytest.raises(ValueError, match="Start time cannot be negative"):
            Turn(
                speaker=["SPEAKER_00"],
                text="Test",
                start_time=-1.0,
                end_time=5.0,
                duration=6.0,
                turn_count=1
            )

        # Test end time before start time
        with pytest.raises(ValueError, match="End time must be after start time"):
            Turn(
                speaker=["SPEAKER_00"],
                text="Test",
                start_time=5.0,
                end_time=3.0,
                duration=2.0,
                turn_count=1
            )

        # Test empty speaker list
        with pytest.raises(ValueError, match="Speaker list cannot be empty"):
            Turn(
                speaker=[],
                text="Test",
                start_time=0.0,
                end_time=5.0,
                duration=5.0,
                turn_count=1
            )

        # Test empty text
        with pytest.raises(ValueError, match="Text cannot be empty"):
            Turn(
                speaker=["SPEAKER_00"],
                text="",
                start_time=0.0,
                end_time=5.0,
                duration=5.0,
                turn_count=1
            )

    def test_turn_methods(self):
        """Test Turn methods."""
        turn = Turn(
            speaker=["SPEAKER_00"],
            text="Test turn for methods.",
            start_time=10.0,
            end_time=15.0,
            duration=5.0,
            turn_count=1
        )

        # Test contains_time
        assert turn.contains_time(12.5)
        assert not turn.contains_time(5.0)
        assert not turn.contains_time(20.0)

        # Test overlaps_with
        other_turn = Turn(
            speaker=["SPEAKER_01"],
            text="Other turn",
            start_time=12.0,
            end_time=18.0,
            duration=6.0,
            turn_count=2
        )
        assert turn.overlaps_with(other_turn)

        non_overlapping = Turn(
            speaker=["SPEAKER_01"],
            text="Non-overlapping",
            start_time=20.0,
            end_time=25.0,
            duration=5.0,
            turn_count=3
        )
        assert not turn.overlaps_with(non_overlapping)

    def test_turn_to_dict(self):
        """Test converting Turn to dictionary."""
        turn = Turn(
            speaker=["SPEAKER_00"],
            text="Test turn",
            start_time=0.0,
            end_time=5.0,
            duration=5.0,
            turn_count=1,
            inferred_speaker_role="host",
            inferred_speaker_name="John Doe"
        )

        turn_dict = turn.to_dict()
        assert turn_dict['speaker'] == ["SPEAKER_00"]
        assert turn_dict['text'] == "Test turn"
        assert turn_dict['start_time'] == 0.0
        assert turn_dict['end_time'] == 5.0
        assert turn_dict['duration'] == 5.0
        assert turn_dict['is_overlapping'] is False
        assert turn_dict['primary_speaker'] == "SPEAKER_00"
        assert turn_dict['inferred_speaker_role'] == "host"
        assert turn_dict['inferred_speaker_name'] == "John Doe"
        assert turn_dict['word_count'] == 2
        assert turn_dict['words_per_second'] == 0.4


class TestEpisode:
    """Test the Episode class."""

    def test_episode_creation(self):
        """Test creating a basic Episode object."""
        episode = Episode(
            title="Test Episode",
            description="A test episode",
            mp3_url="https://example.com/test.mp3",
            duration_seconds=1800.0,
            transcript="This is the full transcript of the episode.",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="https://example.com/rss.xml",
            host_predicted_names=["John Doe"],
            guest_predicted_names=["Jane Smith"],
            main_ep_speakers=["SPEAKER_00", "SPEAKER_01"]
        )

        assert episode.title == "Test Episode"
        assert episode.duration_minutes == 30.0
        assert episode.duration_hours == 0.5
        assert episode.num_hosts == 1
        assert episode.num_guests == 1
        assert episode.num_main_speakers == 2
        assert episode.is_interview
        assert not episode.is_solo
        assert episode.has_guests

    def test_episode_properties(self):
        """Test Episode properties."""
        episode = Episode(
            title="Test Episode",
            description="A test episode",
            mp3_url="https://example.com/test.mp3",
            duration_seconds=3600.0,  # 1 hour
            transcript="Transcript",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="https://example.com/rss.xml",
            host_predicted_names=["John Doe"],
            guest_predicted_names=[],
            main_ep_speakers=["SPEAKER_00"]
        )

        assert episode.is_long_form
        assert not episode.is_short_form
        assert episode.is_solo
        assert not episode.is_interview
        assert not episode.has_guests

    def test_episode_validation(self):
        """Test Episode validation."""
        # Test empty title
        with pytest.raises(ValueError, match="Episode title cannot be empty"):
            Episode(
                title="",
                description="Test",
                mp3_url="https://example.com/test.mp3",
                duration_seconds=1800.0,
                transcript="Transcript",
                podcast_title="Test Podcast",
                podcast_description="A test podcast",
                rss_url="https://example.com/rss.xml"
            )

        # Test negative duration
        with pytest.raises(ValueError, match="Duration cannot be negative"):
            Episode(
                title="Test Episode",
                description="Test",
                mp3_url="https://example.com/test.mp3",
                duration_seconds=-100.0,
                transcript="Transcript",
                podcast_title="Test Podcast",
                podcast_description="A test podcast",
                rss_url="https://example.com/rss.xml"
            )

    def test_episode_turn_methods(self):
        """Test Episode turn-related methods."""
        episode = Episode(
            title="Test Episode",
            description="A test episode",
            mp3_url="https://example.com/test.mp3",
            duration_seconds=600.0,  # 10 minutes
            transcript="Transcript",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="https://example.com/rss.xml",
            host_predicted_names=["John Doe"],
            guest_predicted_names=[],
            main_ep_speakers=["SPEAKER_00"]
        )

        # Create some test turns
        turns_data = [
            {
                'mp3url': 'https://example.com/test.mp3',
                'speaker': ['SPEAKER_00'],
                'turnText': 'First turn',
                'startTime': 0.0,
                'endTime': 30.0,
                'duration': 30.0,
                'turnCount': 1,
                'inferredSpeakerRole': 'host',
                'inferredSpeakerName': 'John Doe'
            },
            {
                'mp3url': 'https://example.com/test.mp3',
                'speaker': ['SPEAKER_00'],
                'turnText': 'Second turn',
                'startTime': 30.0,
                'endTime': 60.0,
                'duration': 30.0,
                'turnCount': 2,
                'inferredSpeakerRole': 'host',
                'inferredSpeakerName': 'John Doe'
            }
        ]

        episode.load_turns(turns_data)

        # Test getting all turns
        all_turns = episode.get_all_turns()
        assert len(all_turns) == 2

        # Test getting turns by time range
        early_turns = episode.get_turns_by_time_range(0, 45, behavior=TimeRangeBehavior.STRICT)
        assert len(early_turns) == 1

        # Test getting turns by speaker
        speaker_turns = episode.get_turns_by_speaker("SPEAKER_00")
        assert len(speaker_turns) == 2

        # Test getting turns by minimum length
        long_turns = episode.get_turns_by_min_length(2)  # 2 words
        assert len(long_turns) == 2

        # Test getting host turns
        host_turns = episode.get_host_turns()
        assert len(host_turns) == 2

    def test_episode_statistics(self):
        """Test Episode statistics."""
        episode = Episode(
            title="Test Episode",
            description="A test episode",
            mp3_url="https://example.com/test.mp3",
            duration_seconds=600.0,
            transcript="Transcript",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="https://example.com/rss.xml",
            host_predicted_names=["John Doe"],
            guest_predicted_names=[],
            main_ep_speakers=["SPEAKER_00"]
        )

        # Load turns
        turns_data = [
            {
                'mp3url': 'https://example.com/test.mp3',
                'speaker': ['SPEAKER_00'],
                'turnText': 'First turn with more words',
                'startTime': 0.0,
                'endTime': 30.0,
                'duration': 30.0,
                'turnCount': 1
            },
            {
                'mp3url': 'https://example.com/test.mp3',
                'speaker': ['SPEAKER_00'],
                'turnText': 'Second turn',
                'startTime': 30.0,
                'endTime': 60.0,
                'duration': 30.0,
                'turnCount': 2
            }
        ]

        episode.load_turns(turns_data)

        stats = episode.get_turn_statistics()
        assert stats['total_turns'] == 2
        assert stats['total_words'] == 7  # "First turn with more words" + "Second turn"
        assert stats['avg_turn_duration'] == 30.0
        assert stats['avg_words_per_turn'] == 3.5

    def test_episode_indexing_and_iteration(self):
        """Test Episode indexing and iteration functionality."""
        episode = Episode(
            title="Test Episode",
            description="A test episode",
            mp3_url="https://example.com/test.mp3",
            duration_seconds=600.0,  # 10 minutes
            transcript="Transcript",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="https://example.com/rss.xml",
            host_predicted_names=["John Doe"],
            guest_predicted_names=[],
            main_ep_speakers=["SPEAKER_00"]
        )

        # Create some test turns
        turns_data = [
            {
                'mp3url': 'https://example.com/test.mp3',
                'speaker': ['SPEAKER_00'],
                'turnText': 'First turn',
                'startTime': 0.0,
                'endTime': 30.0,
                'duration': 30.0,
                'turnCount': 1,
                'inferredSpeakerRole': 'host',
                'inferredSpeakerName': 'John Doe'
            },
            {
                'mp3url': 'https://example.com/test.mp3',
                'speaker': ['SPEAKER_00'],
                'turnText': 'Second turn',
                'startTime': 30.0,
                'endTime': 60.0,
                'duration': 30.0,
                'turnCount': 2,
                'inferredSpeakerRole': 'host',
                'inferredSpeakerName': 'John Doe'
            },
            {
                'mp3url': 'https://example.com/test.mp3',
                'speaker': ['SPEAKER_00'],
                'turnText': 'Third turn',
                'startTime': 60.0,
                'endTime': 90.0,
                'duration': 30.0,
                'turnCount': 3,
                'inferredSpeakerRole': 'host',
                'inferredSpeakerName': 'John Doe'
            }
        ]

        episode.load_turns(turns_data)

        # Test length
        assert len(episode) == 3

        # Test indexing
        assert episode[0].text == "First turn"
        assert episode[1].text == "Second turn"
        assert episode[2].text == "Third turn"

        # Test negative indexing
        assert episode[-1].text == "Third turn"
        assert episode[-2].text == "Second turn"

        # Test iteration
        turn_texts = [turn.text for turn in episode]
        assert turn_texts == ["First turn", "Second turn", "Third turn"]

        # Test that indexing raises error when turns not loaded
        episode_no_turns = Episode(
            title="Test Episode",
            description="A test episode",
            mp3_url="https://example.com/test.mp3",
            duration_seconds=600.0,
            transcript="Transcript",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="https://example.com/rss.xml",
            host_predicted_names=["John Doe"],
            guest_predicted_names=[],
            main_ep_speakers=["SPEAKER_00"]
        )

        with pytest.raises(RuntimeError, match="Turns not loaded"):
            len(episode_no_turns)

        with pytest.raises(RuntimeError, match="Turns not loaded"):
            episode_no_turns[0]

        with pytest.raises(RuntimeError, match="Turns not loaded"):
            list(episode_no_turns)


class TestPodcast:
    """Test the Podcast class."""

    def test_podcast_creation(self):
        """Test creating a basic Podcast object."""
        podcast = Podcast(
            title="Test Podcast",
            description="A test podcast",
            rss_url="https://example.com/rss.xml"
        )

        assert podcast.title == "Test Podcast"
        assert podcast.description == "A test podcast"
        assert podcast.num_episodes == 0
        assert podcast.total_duration_seconds == 0.0
        assert podcast.total_duration_hours == 0.0

    def test_podcast_with_episodes(self):
        """Test Podcast with episodes."""
        podcast = Podcast(
            title="Test Podcast",
            description="A test podcast",
            rss_url="https://example.com/rss.xml"
        )

        # Create episodes
        episode1 = Episode(
            title="Episode 1",
            description="First episode",
            mp3_url="https://example.com/ep1.mp3",
            duration_seconds=1800.0,
            transcript="Transcript 1",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="https://example.com/rss.xml",
            host_predicted_names=["John Doe"],
            guest_predicted_names=[],
            main_ep_speakers=["SPEAKER_00"]
        )

        episode2 = Episode(
            title="Episode 2",
            description="Second episode",
            mp3_url="https://example.com/ep2.mp3",
            duration_seconds=3600.0,
            transcript="Transcript 2",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="https://example.com/rss.xml",
            host_predicted_names=["John Doe"],
            guest_predicted_names=["Jane Smith"],
            main_ep_speakers=["SPEAKER_00", "SPEAKER_01"]
        )

        podcast.add_episode(episode1)
        podcast.add_episode(episode2)

        assert podcast.num_episodes == 2
        assert podcast.total_duration_seconds == 5400.0
        assert podcast.total_duration_hours == 1.5
        assert podcast.avg_episode_duration_minutes == 45.0
        assert podcast.host_names == ["John Doe"]
        assert podcast.guest_names == ["Jane Smith"]
        assert len(podcast.solo_episodes) == 1
        assert len(podcast.interview_episodes) == 1

    def test_podcast_validation(self):
        """Test Podcast validation."""
        # Test empty title
        with pytest.raises(ValueError, match="Podcast title cannot be empty"):
            Podcast(
                title="",
                description="Test",
                rss_url="https://example.com/rss.xml"
            )

        # Test empty RSS URL
        with pytest.raises(ValueError, match="RSS URL cannot be empty"):
            Podcast(
                title="Test Podcast",
                description="Test",
                rss_url=""
            )

    def test_podcast_methods(self):
        """Test Podcast methods."""
        podcast = Podcast(
            title="Test Podcast",
            description="A test podcast",
            rss_url="https://example.com/rss.xml"
        )

        # Add episodes with different characteristics
        episode1 = Episode(
            title="Solo Episode",
            description="A solo episode",
            mp3_url="https://example.com/solo.mp3",
            duration_seconds=1800.0,
            transcript="Solo transcript",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="https://example.com/rss.xml",
            host_predicted_names=["John Doe"],
            guest_predicted_names=[],
            main_ep_speakers=["SPEAKER_00"]
        )

        episode2 = Episode(
            title="Interview Episode",
            description="An interview episode",
            mp3_url="https://example.com/interview.mp3",
            duration_seconds=3600.0,
            transcript="Interview transcript",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="https://example.com/rss.xml",
            host_predicted_names=["John Doe"],
            guest_predicted_names=["Jane Smith"],
            main_ep_speakers=["SPEAKER_00", "SPEAKER_01"]
        )

        podcast.add_episode(episode1)
        podcast.add_episode(episode2)

        # Test episode filtering methods
        solo_episodes = podcast.get_episodes_by_host("John Doe")
        assert len(solo_episodes) == 2

        interview_episodes = podcast.get_episodes_by_guest("Jane Smith")
        assert len(interview_episodes) == 1

        long_episodes = podcast.get_episodes_by_duration_range(min_minutes=30)
        assert len(long_episodes) == 2

        short_episodes = podcast.get_episodes_by_duration_range(max_minutes=30)
        assert len(short_episodes) == 1

        two_speaker_episodes = podcast.get_episodes_by_speaker_count(min_speakers=2, max_speakers=2)
        assert len(two_speaker_episodes) == 1

    def test_podcast_statistics(self):
        """Test Podcast statistics."""
        podcast = Podcast(
            title="Test Podcast",
            description="A test podcast",
            rss_url="https://example.com/rss.xml"
        )

        # Add episodes
        episode1 = Episode(
            title="Episode 1",
            description="First episode",
            mp3_url="https://example.com/ep1.mp3",
            duration_seconds=1800.0,
            transcript="Transcript 1",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="https://example.com/rss.xml",
            host_predicted_names=["John Doe"],
            guest_predicted_names=[],
            main_ep_speakers=["SPEAKER_00"]
        )

        episode2 = Episode(
            title="Episode 2",
            description="Second episode",
            mp3_url="https://example.com/ep2.mp3",
            duration_seconds=3600.0,
            transcript="Transcript 2",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="https://example.com/rss.xml",
            host_predicted_names=["John Doe"],
            guest_predicted_names=["Jane Smith"],
            main_ep_speakers=["SPEAKER_00", "SPEAKER_01"]
        )

        podcast.add_episode(episode1)
        podcast.add_episode(episode2)

        stats = podcast.get_episode_statistics()
        assert stats['num_episodes'] == 2
        assert stats['total_duration_hours'] == 1.5
        assert stats['avg_episode_duration_minutes'] == 45.0
        assert stats['episode_types']['solo'] == 1
        assert stats['episode_types']['interview'] == 1
        assert stats['host_names'] == ["John Doe"]
        assert stats['guest_names'] == ["Jane Smith"]

    def test_podcast_indexing(self):
        """Test Podcast indexing functionality."""
        podcast = Podcast(
            title="Test Podcast",
            description="A test podcast",
            rss_url="https://example.com/rss.xml"
        )

        # Create episodes
        episode1 = Episode(
            title="Episode 1",
            description="First episode",
            mp3_url="https://example.com/ep1.mp3",
            duration_seconds=1800.0,
            transcript="Transcript 1",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="https://example.com/rss.xml",
            host_predicted_names=["John Doe"],
            guest_predicted_names=[],
            main_ep_speakers=["SPEAKER_00"]
        )

        episode2 = Episode(
            title="Episode 2",
            description="Second episode",
            mp3_url="https://example.com/ep2.mp3",
            duration_seconds=3600.0,
            transcript="Transcript 2",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="https://example.com/rss.xml",
            host_predicted_names=["John Doe"],
            guest_predicted_names=["Jane Smith"],
            main_ep_speakers=["SPEAKER_00", "SPEAKER_01"]
        )

        episode3 = Episode(
            title="Episode 3",
            description="Third episode",
            mp3_url="https://example.com/ep3.mp3",
            duration_seconds=2700.0,
            transcript="Transcript 3",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="https://example.com/rss.xml",
            host_predicted_names=["John Doe"],
            guest_predicted_names=[],
            main_ep_speakers=["SPEAKER_00"]
        )

        podcast.add_episode(episode1)
        podcast.add_episode(episode2)
        podcast.add_episode(episode3)

        # Test indexing
        assert podcast[0].title == "Episode 1"
        assert podcast[1].title == "Episode 2"
        assert podcast[2].title == "Episode 3"

        # Test negative indexing
        assert podcast[-1].title == "Episode 3"
        assert podcast[-2].title == "Episode 2"

        # Test that indexing raises IndexError for out of bounds
        with pytest.raises(IndexError):
            podcast[3]

        with pytest.raises(IndexError):
            podcast[-4]

        # Test that indexing works with empty podcast
        empty_podcast = Podcast(
            title="Empty Podcast",
            description="An empty podcast",
            rss_url="https://example.com/empty.xml"
        )

        with pytest.raises(IndexError):
            empty_podcast[0]


class TestSPORCDataset:
    """Test the SPORCDataset class."""

    @patch('sporc.dataset.load_dataset')
    def test_safe_float_method(self, mock_load_dataset):
        """Test the _safe_float method handles None and edge cases correctly."""
        # Mock the dataset loading to avoid downloading real data
        mock_episode_data = MagicMock()
        mock_episode_data.__iter__ = lambda x: iter([])
        mock_speaker_turn_data = MagicMock()
        mock_speaker_turn_data.__iter__ = lambda x: iter([])
        mock_load_dataset.side_effect = [mock_episode_data, mock_speaker_turn_data]

        dataset = SPORCDataset()

        # Test None values
        assert dataset._safe_float(None) == 0.0
        assert dataset._safe_float(None, default=5.0) == 5.0

        # Test valid values
        assert dataset._safe_float(10) == 10.0
        assert dataset._safe_float(10.5) == 10.5
        assert dataset._safe_float("15.7") == 15.7
        assert dataset._safe_float("0") == 0.0

        # Test invalid values
        assert dataset._safe_float("invalid") == 0.0
        assert dataset._safe_float("invalid", default=3.0) == 3.0
        assert dataset._safe_float({}) == 0.0
        assert dataset._safe_float([]) == 0.0

        # Test edge cases
        assert dataset._safe_float("") == 0.0
        assert dataset._safe_float("   ") == 0.0
        assert dataset._safe_float(0) == 0.0
        assert dataset._safe_float(-5) == -5.0

    @patch('sporc.dataset.load_dataset')
    def test_dataset_initialization(self, mock_load_dataset):
        """Test SPORCDataset initialization."""
        # Mock the dataset loading
        mock_episode_data = MagicMock()
        mock_episode_data.__iter__ = lambda x: iter([
            {
                'podTitle': 'Test Podcast',
                'podDescription': 'A test podcast',
                'rssUrl': 'https://example.com/rss.xml',
                'epTitle': 'Test Episode',
                'epDescription': 'A test episode',
                'mp3url': 'https://example.com/test.mp3',
                'durationSeconds': 1800.0,
                'transcript': 'Test transcript',
                'hostPredictedNames': ['John Doe'],
                'guestPredictedNames': 'NO_GUEST_PREDICTED',
                'neitherPredictedNames': 'NO_NEITHER_IDENTIFIED',
                'mainEpSpeakers': ['SPEAKER_00'],
                'hostSpeakerLabels': 'SPEAKER_DATA_UNAVAILABLE',
                'guestSpeakerLabels': 'SPEAKER_DATA_UNAVAILABLE',
                'overlapPropDuration': 0.0,
                'overlapPropTurnCount': 0.0,
                'avgTurnDuration': 30.0,
                'totalSpLabels': 1.0,
                'language': 'en',
                'explicit': 0,
                'imageUrl': None,
                'episodeDateLocalized': None,
                'oldestEpisodeDate': None,
                'lastUpdate': None,
                'createdOn': None,
                'category1': 'education',
                'category2': None,
                'category3': None,
                'category4': None,
                'category5': None,
                'category6': None,
                'category7': None,
                'category8': None,
                'category9': None,
                'category10': None
            }
        ])

        mock_episode_data.__len__.return_value = 1

        mock_speaker_turn_data = MagicMock()
        mock_speaker_turn_data.__iter__ = lambda x: iter([
            {
                'mp3url': 'https://example.com/test.mp3',
                'speaker': ['SPEAKER_00'],
                'turnText': 'Test turn',
                'startTime': 0.0,
                'endTime': 30.0,
                'duration': 30.0,
                'turnCount': 1,
                'inferredSpeakerRole': 'host',
                'inferredSpeakerName': 'John Doe'
            }
        ])

        mock_speaker_turn_data.__len__.return_value = 1

        mock_load_dataset.side_effect = [mock_episode_data, mock_speaker_turn_data]

        # Initialize dataset
        dataset = SPORCDataset()

        assert len(dataset._podcasts) == 1
        assert len(dataset._episodes) == 1
        assert dataset._loaded is True

        # Test getting all podcasts
        podcasts = dataset.get_all_podcasts()
        assert len(podcasts) == 1
        assert podcasts[0].title == "Test Podcast"

        # Test getting all episodes
        episodes = dataset.get_all_episodes()
        assert len(episodes) == 1
        assert episodes[0].title == "Test Episode"

    @patch('sporc.dataset.load_dataset')
    def test_search_podcast(self, mock_load_dataset):
        """Test podcast search functionality."""
        # Mock dataset with multiple podcasts
        mock_episode_data = MagicMock()
        mock_episode_data.__iter__ = lambda x: iter([
            {
                'podTitle': 'Test Podcast 1',
                'podDescription': 'First test podcast',
                'rssUrl': 'https://example.com/rss1.xml',
                'epTitle': 'Episode 1',
                'epDescription': 'First episode',
                'mp3url': 'https://example.com/ep1.mp3',
                'durationSeconds': 1800.0,
                'transcript': 'Transcript 1',
                'hostPredictedNames': ['John Doe'],
                'guestPredictedNames': 'NO_GUEST_PREDICTED',
                'neitherPredictedNames': 'NO_NEITHER_IDENTIFIED',
                'mainEpSpeakers': ['SPEAKER_00'],
                'hostSpeakerLabels': 'SPEAKER_DATA_UNAVAILABLE',
                'guestSpeakerLabels': 'SPEAKER_DATA_UNAVAILABLE',
                'overlapPropDuration': 0.0,
                'overlapPropTurnCount': 0.0,
                'avgTurnDuration': 30.0,
                'totalSpLabels': 1.0,
                'language': 'en',
                'explicit': 0,
                'imageUrl': None,
                'episodeDateLocalized': None,
                'oldestEpisodeDate': None,
                'lastUpdate': None,
                'createdOn': None,
                'category1': 'education',
                'category2': None,
                'category3': None,
                'category4': None,
                'category5': None,
                'category6': None,
                'category7': None,
                'category8': None,
                'category9': None,
                'category10': None
            },
            {
                'podTitle': 'Test Podcast 2',
                'podDescription': 'Second test podcast',
                'rssUrl': 'https://example.com/rss2.xml',
                'epTitle': 'Episode 2',
                'epDescription': 'Second episode',
                'mp3url': 'https://example.com/ep2.mp3',
                'durationSeconds': 3600.0,
                'transcript': 'Transcript 2',
                'hostPredictedNames': ['Jane Smith'],
                'guestPredictedNames': 'NO_GUEST_PREDICTED',
                'neitherPredictedNames': 'NO_NEITHER_IDENTIFIED',
                'mainEpSpeakers': ['SPEAKER_00'],
                'hostSpeakerLabels': 'SPEAKER_DATA_UNAVAILABLE',
                'guestSpeakerLabels': 'SPEAKER_DATA_UNAVAILABLE',
                'overlapPropDuration': 0.0,
                'overlapPropTurnCount': 0.0,
                'avgTurnDuration': 30.0,
                'totalSpLabels': 1.0,
                'language': 'en',
                'explicit': 0,
                'imageUrl': None,
                'episodeDateLocalized': None,
                'oldestEpisodeDate': None,
                'lastUpdate': None,
                'createdOn': None,
                'category1': 'music',
                'category2': None,
                'category3': None,
                'category4': None,
                'category5': None,
                'category6': None,
                'category7': None,
                'category8': None,
                'category9': None,
                'category10': None
            }
        ])

        mock_episode_data.__len__.return_value = 2

        mock_speaker_turn_data = MagicMock()
        mock_speaker_turn_data.__iter__ = lambda x: iter([])

        mock_load_dataset.side_effect = [mock_episode_data, mock_speaker_turn_data]

        dataset = SPORCDataset()

        # Test exact match
        podcast = dataset.search_podcast("Test Podcast 1")
        assert podcast.title == "Test Podcast 1"

        # Test case-insensitive match
        podcast = dataset.search_podcast("test podcast 1")
        assert podcast.title == "Test Podcast 1"

        # Test partial match
        podcast = dataset.search_podcast("Test Podcast")
        assert podcast.title == "Test Podcast 1"  # Should return first match

        # Test not found
        with pytest.raises(NotFoundError):
            dataset.search_podcast("Nonexistent Podcast")

    @patch('sporc.dataset.load_dataset')
    def test_search_episodes(self, mock_load_dataset):
        """Test episode search functionality."""
        # Mock dataset with episodes
        mock_episode_data = MagicMock()
        mock_episode_data.__iter__ = lambda x: iter([
            {
                'podTitle': 'Test Podcast',
                'podDescription': 'A test podcast',
                'rssUrl': 'https://example.com/rss.xml',
                'epTitle': 'Short Episode',
                'epDescription': 'A short episode',
                'mp3url': 'https://example.com/short.mp3',
                'durationSeconds': 300.0,  # 5 minutes
                'transcript': 'Short transcript',
                'hostPredictedNames': ['John Doe'],
                'guestPredictedNames': 'NO_GUEST_PREDICTED',
                'neitherPredictedNames': 'NO_NEITHER_IDENTIFIED',
                'mainEpSpeakers': ['SPEAKER_00'],
                'hostSpeakerLabels': 'SPEAKER_DATA_UNAVAILABLE',
                'guestSpeakerLabels': 'SPEAKER_DATA_UNAVAILABLE',
                'overlapPropDuration': 0.0,
                'overlapPropTurnCount': 0.0,
                'avgTurnDuration': 30.0,
                'totalSpLabels': 1.0,
                'language': 'en',
                'explicit': 0,
                'imageUrl': None,
                'episodeDateLocalized': None,
                'oldestEpisodeDate': None,
                'lastUpdate': None,
                'createdOn': None,
                'category1': 'education',
                'category2': None,
                'category3': None,
                'category4': None,
                'category5': None,
                'category6': None,
                'category7': None,
                'category8': None,
                'category9': None,
                'category10': None
            },
            {
                'podTitle': 'Test Podcast',
                'podDescription': 'A test podcast',
                'rssUrl': 'https://example.com/rss.xml',
                'epTitle': 'Long Episode',
                'epDescription': 'A long episode',
                'mp3url': 'https://example.com/long.mp3',
                'durationSeconds': 3600.0,  # 1 hour
                'transcript': 'Long transcript',
                'hostPredictedNames': ['John Doe'],
                'guestPredictedNames': ['Jane Smith'],
                'neitherPredictedNames': 'NO_NEITHER_IDENTIFIED',
                'mainEpSpeakers': ['SPEAKER_00', 'SPEAKER_01'],
                'hostSpeakerLabels': 'SPEAKER_DATA_UNAVAILABLE',
                'guestSpeakerLabels': 'SPEAKER_DATA_UNAVAILABLE',
                'overlapPropDuration': 0.0,
                'overlapPropTurnCount': 0.0,
                'avgTurnDuration': 30.0,
                'totalSpLabels': 2.0,
                'language': 'en',
                'explicit': 0,
                'imageUrl': None,
                'episodeDateLocalized': None,
                'oldestEpisodeDate': None,
                'lastUpdate': None,
                'createdOn': None,
                'category1': 'music',
                'category2': None,
                'category3': None,
                'category4': None,
                'category5': None,
                'category6': None,
                'category7': None,
                'category8': None,
                'category9': None,
                'category10': None
            }
        ])

        mock_episode_data.__len__.return_value = 2

        mock_speaker_turn_data = MagicMock()
        mock_speaker_turn_data.__iter__ = lambda x: iter([])

        mock_load_dataset.side_effect = [mock_episode_data, mock_speaker_turn_data]

        dataset = SPORCDataset()

        # Test search by duration
        long_episodes = dataset.search_episodes(min_duration=1800)  # 30+ minutes
        assert len(long_episodes) == 1
        assert long_episodes[0].title == "Long Episode"

        short_episodes = dataset.search_episodes(max_duration=600)  # 10 minutes or less
        assert len(short_episodes) == 1
        assert short_episodes[0].title == "Short Episode"

        # Test search by speaker count
        solo_episodes = dataset.search_episodes(max_speakers=1)
        assert len(solo_episodes) == 1
        assert solo_episodes[0].title == "Short Episode"

        multi_speaker_episodes = dataset.search_episodes(min_speakers=2)
        assert len(multi_speaker_episodes) == 1
        assert multi_speaker_episodes[0].title == "Long Episode"

        # Test search by host
        john_episodes = dataset.search_episodes(host_name="John")
        assert len(john_episodes) == 2

        # Test search by category
        education_episodes = dataset.search_episodes(category="education")
        assert len(education_episodes) == 1
        assert education_episodes[0].title == "Short Episode"

        music_episodes = dataset.search_episodes(category="music")
        assert len(music_episodes) == 1
        assert music_episodes[0].title == "Long Episode"

    @patch('sporc.dataset.load_dataset')
    def test_dataset_statistics(self, mock_load_dataset):
        """Test dataset statistics."""
        # Mock dataset
        mock_episode_data = MagicMock()
        mock_episode_data.__iter__ = lambda x: iter([
            {
                'podTitle': 'Test Podcast',
                'podDescription': 'A test podcast',
                'rssUrl': 'https://example.com/rss.xml',
                'epTitle': 'Test Episode',
                'epDescription': 'A test episode',
                'mp3url': 'https://example.com/test.mp3',
                'durationSeconds': 1800.0,
                'transcript': 'Test transcript',
                'hostPredictedNames': ['John Doe'],
                'guestPredictedNames': 'NO_GUEST_PREDICTED',
                'neitherPredictedNames': 'NO_NEITHER_IDENTIFIED',
                'mainEpSpeakers': ['SPEAKER_00'],
                'hostSpeakerLabels': 'SPEAKER_DATA_UNAVAILABLE',
                'guestSpeakerLabels': 'SPEAKER_DATA_UNAVAILABLE',
                'overlapPropDuration': 0.0,
                'overlapPropTurnCount': 0.0,
                'avgTurnDuration': 30.0,
                'totalSpLabels': 1.0,
                'language': 'en',
                'explicit': 0,
                'imageUrl': None,
                'episodeDateLocalized': None,
                'oldestEpisodeDate': None,
                'lastUpdate': None,
                'createdOn': None,
                'category1': 'education',
                'category2': None,
                'category3': None,
                'category4': None,
                'category5': None,
                'category6': None,
                'category7': None,
                'category8': None,
                'category9': None,
                'category10': None
            }
        ])

        mock_episode_data.__len__.return_value = 1

        mock_speaker_turn_data = MagicMock()
        mock_speaker_turn_data.__iter__ = lambda x: iter([])

        mock_load_dataset.side_effect = [mock_episode_data, mock_speaker_turn_data]

        dataset = SPORCDataset()

        stats = dataset.get_dataset_statistics()
        assert stats['total_podcasts'] == 1
        assert stats['total_episodes'] == 1
        assert stats['total_duration_hours'] == 0.5
        assert stats['avg_episode_duration_minutes'] == 30.0
        assert 'education' in stats['category_distribution']
        assert stats['category_distribution']['education'] == 1
        assert 'en' in stats['language_distribution']
        assert stats['language_distribution']['en'] == 1
        assert 1 in stats['speaker_distribution']
        assert stats['speaker_distribution'][1] == 1


class TestExceptions:
    """Test custom exceptions."""

    def test_sporc_error(self):
        """Test SPORCError."""
        error = SPORCError("Test error")
        assert str(error) == "Test error"
        assert isinstance(error, Exception)

    def test_authentication_error(self):
        """Test AuthenticationError."""
        error = AuthenticationError("Authentication failed")
        assert str(error) == "Authentication failed"
        assert isinstance(error, SPORCError)

    def test_dataset_access_error(self):
        """Test DatasetAccessError."""
        error = DatasetAccessError("Dataset not found")
        assert str(error) == "Dataset not found"
        assert isinstance(error, SPORCError)

    def test_not_found_error(self):
        """Test NotFoundError."""
        error = NotFoundError("Resource not found")
        assert str(error) == "Resource not found"
        assert isinstance(error, SPORCError)


if __name__ == "__main__":
    pytest.main([__file__])