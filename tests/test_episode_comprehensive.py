"""
Comprehensive unit tests for missing functionality in the SPORC episode module.
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock

from sporc.episode import Episode, TimeRangeBehavior
from sporc.turn import Turn


class TestEpisodeProperties:
    """Test episode properties and computed attributes."""

    def setup_method(self):
        """Set up test fixtures."""
        self.episode = Episode(
            title="Test Episode",
            description="A test episode",
            mp3_url="http://example.com/episode.mp3",
            duration_seconds=3600.0,
            transcript="This is a test transcript.",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="http://example.com/rss.xml",
            category1="Education",
            category2="Technology",
            host_predicted_names=["John Doe", "Jane Smith"],
            guest_predicted_names=["Alice Johnson"],
            neither_predicted_names=["Bob Wilson"],
            main_ep_speakers=["SPEAKER_00", "SPEAKER_01", "SPEAKER_02"],
            host_speaker_labels={"SPEAKER_00": "host", "SPEAKER_01": "host"},
            guest_speaker_labels={"SPEAKER_02": "guest"},
            overlap_prop_duration=0.1,
            overlap_prop_turn_count=0.05,
            avg_turn_duration=15.0,
            total_speaker_labels=3,
            language="en",
            explicit=False,
            image_url="http://example.com/image.jpg",
            episode_date_localized=1640995200000,  # 2022-01-01 00:00:00 UTC
            oldest_episode_date="2020-01-01",
            last_update=1640995200000,
            created_on=1640995200000
        )

    def test_categories_property(self):
        """Test categories property returns all non-None categories."""
        categories = self.episode.categories
        assert len(categories) == 2
        assert "Education" in categories
        assert "Technology" in categories

    def test_categories_property_with_none_values(self):
        """Test categories property handles None values correctly."""
        episode = Episode(
            title="Test",
            description="Test",
            mp3_url="http://example.com/test.mp3",
            duration_seconds=1800.0,
            transcript="Test",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml",
            category1="Education",
            category2=None,
            category3="Technology",
            category4=None
        )
        categories = episode.categories
        assert len(categories) == 2
        assert "Education" in categories
        assert "Technology" in categories

    def test_primary_category_property(self):
        """Test primary_category property returns category1."""
        assert self.episode.primary_category == "Education"

    def test_host_names_property(self):
        """Test host_names property returns host_predicted_names."""
        assert self.episode.host_names == ["John Doe", "Jane Smith"]

    def test_guest_names_property(self):
        """Test guest_names property returns guest_predicted_names."""
        assert self.episode.guest_names == ["Alice Johnson"]

    def test_num_hosts_property(self):
        """Test num_hosts property returns correct count."""
        assert self.episode.num_hosts == 2

    def test_num_guests_property(self):
        """Test num_guests property returns correct count."""
        assert self.episode.num_guests == 1

    def test_num_main_speakers_property(self):
        """Test num_main_speakers property returns correct count."""
        assert self.episode.num_main_speakers == 3

    def test_duration_minutes_property(self):
        """Test duration_minutes property converts seconds to minutes."""
        assert self.episode.duration_minutes == 60.0

    def test_duration_hours_property(self):
        """Test duration_hours property converts seconds to hours."""
        assert self.episode.duration_hours == 1.0

    def test_episode_date_property_with_valid_timestamp(self):
        """Test episode_date property with valid timestamp."""
        date = self.episode.episode_date
        assert isinstance(date, datetime)
        assert date.year == 2021  # 1640995200000 corresponds to Dec 31, 2021

    def test_episode_date_property_with_none(self):
        """Test episode_date property with None timestamp."""
        episode = Episode(
            title="Test",
            description="Test",
            mp3_url="http://example.com/test.mp3",
            duration_seconds=1800.0,
            transcript="Test",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml",
            episode_date_localized=None
        )
        assert episode.episode_date is None

    def test_episode_date_property_with_string_timestamp(self):
        """Test episode_date property with string timestamp."""
        episode = Episode(
            title="Test",
            description="Test",
            mp3_url="http://example.com/test.mp3",
            duration_seconds=1800.0,
            transcript="Test",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml",
            episode_date_localized="1640995200000"
        )
        date = episode.episode_date
        assert isinstance(date, datetime)
        assert date.year == 2021  # 1640995200000 corresponds to Dec 31, 2021

    def test_episode_date_property_with_invalid_string(self):
        """Test episode_date property with invalid string timestamp."""
        episode = Episode(
            title="Test",
            description="Test",
            mp3_url="http://example.com/test.mp3",
            duration_seconds=1800.0,
            transcript="Test",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml",
            episode_date_localized="invalid"
        )
        assert episode.episode_date is None

    def test_is_long_form_property(self):
        """Test is_long_form property for episodes >30 minutes."""
        assert self.episode.is_long_form is True

        # Test with short episode
        short_episode = Episode(
            title="Short",
            description="Short",
            mp3_url="http://example.com/short.mp3",
            duration_seconds=1200.0,  # 20 minutes
            transcript="Short",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml"
        )
        assert short_episode.is_long_form is False

    def test_is_short_form_property(self):
        """Test is_short_form property for episodes <10 minutes."""
        assert self.episode.is_short_form is False

        # Test with short episode
        short_episode = Episode(
            title="Short",
            description="Short",
            mp3_url="http://example.com/short.mp3",
            duration_seconds=300.0,  # 5 minutes
            transcript="Short",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml"
        )
        assert short_episode.is_short_form is True

    def test_has_guests_property(self):
        """Test has_guests property."""
        assert self.episode.has_guests is True

        # Test without guests
        no_guests_episode = Episode(
            title="No Guests",
            description="No guests",
            mp3_url="http://example.com/noguests.mp3",
            duration_seconds=1800.0,
            transcript="No guests",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml",
            guest_predicted_names=[]
        )
        assert no_guests_episode.has_guests is False

    def test_is_solo_property(self):
        """Test is_solo property for single host, no guests."""
        # Test with multiple hosts (not solo)
        assert self.episode.is_solo is False

        # Test with single host, no guests
        solo_episode = Episode(
            title="Solo",
            description="Solo",
            mp3_url="http://example.com/solo.mp3",
            duration_seconds=1800.0,
            transcript="Solo",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml",
            host_predicted_names=["John Doe"],
            guest_predicted_names=[]
        )
        assert solo_episode.is_solo is True

    def test_is_interview_property(self):
        """Test is_interview property for host + guest."""
        assert self.episode.is_interview is True

        # Test without guests (not interview)
        no_guests_episode = Episode(
            title="No Guests",
            description="No guests",
            mp3_url="http://example.com/noguests.mp3",
            duration_seconds=1800.0,
            transcript="No guests",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml",
            host_predicted_names=["John Doe"],
            guest_predicted_names=[]
        )
        assert no_guests_episode.is_interview is False

    def test_is_panel_property(self):
        """Test is_panel property for multiple hosts/guests."""
        assert self.episode.is_panel is True  # 2 hosts + 1 guest = 3 total

        # Test with single host, single guest (not panel)
        interview_episode = Episode(
            title="Interview",
            description="Interview",
            mp3_url="http://example.com/interview.mp3",
            duration_seconds=1800.0,
            transcript="Interview",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml",
            host_predicted_names=["John Doe"],
            guest_predicted_names=["Alice Johnson"]
        )
        assert interview_episode.is_panel is False


class TestEpisodeTimeRangeMethods:
    """Test episode time range methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.episode = Episode(
            title="Test Episode",
            description="A test episode",
            mp3_url="http://example.com/episode.mp3",
            duration_seconds=3600.0,
            transcript="This is a test transcript.",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="http://example.com/rss.xml"
        )

        # Create test turns
        self.turn1 = Turn(
            speaker=["SPEAKER_00"],
            text="Hello world",
            start_time=0.0,
            end_time=5.0,
            duration=5.0,
            turn_count=1
        )
        self.turn2 = Turn(
            speaker=["SPEAKER_01"],
            text="Hi there",
            start_time=5.0,
            end_time=10.0,
            duration=5.0,
            turn_count=2
        )
        self.turn3 = Turn(
            speaker=["SPEAKER_00"],
            text="How are you?",
            start_time=10.0,
            end_time=15.0,
            duration=5.0,
            turn_count=3
        )

        # Load turns
        self.episode._turns = [self.turn1, self.turn2, self.turn3]
        self.episode._turns_loaded = True

    def test_get_turns_by_time_range_strict(self):
        """Test get_turns_by_time_range with STRICT behavior."""
        turns = self.episode.get_turns_by_time_range(5.0, 10.0, TimeRangeBehavior.STRICT)
        assert len(turns) == 1
        assert turns[0] == self.turn2

    def test_get_turns_by_time_range_include_partial(self):
        """Test get_turns_by_time_range with INCLUDE_PARTIAL behavior."""
        turns = self.episode.get_turns_by_time_range(3.0, 12.0, TimeRangeBehavior.INCLUDE_PARTIAL)
        assert len(turns) == 3  # All turns overlap with the range

    def test_get_turns_by_time_range_include_full_turns(self):
        """Test get_turns_by_time_range with INCLUDE_FULL_TURNS behavior."""
        turns = self.episode.get_turns_by_time_range(3.0, 12.0, TimeRangeBehavior.INCLUDE_FULL_TURNS)
        assert len(turns) == 3  # All turns overlap with the range

    def test_get_turns_by_time_range_invalid_behavior(self):
        """Test get_turns_by_time_range with invalid behavior."""
        with pytest.raises(ValueError, match="Unknown behavior"):
            self.episode.get_turns_by_time_range(0.0, 10.0, "invalid")

    def test_get_turns_by_time_range_turns_not_loaded(self):
        """Test get_turns_by_time_range when turns are not loaded."""
        episode = Episode(
            title="Test",
            description="Test",
            mp3_url="http://example.com/test.mp3",
            duration_seconds=1800.0,
            transcript="Test",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml"
        )
        with pytest.raises(RuntimeError, match="Turns not loaded"):
            episode.get_turns_by_time_range(0.0, 10.0)

    def test_get_turns_by_time_range_with_trimming(self):
        """Test get_turns_by_time_range_with_trimming."""
        result = self.episode.get_turns_by_time_range_with_trimming(3.0, 12.0)
        assert len(result) == 3

        for item in result:
            assert 'turn' in item
            assert 'trimmed_text' in item
            assert 'original_text' in item
            assert 'trimmed_start' in item
            assert 'trimmed_end' in item
            assert 'was_trimmed' in item

    def test_get_turns_by_time_range_with_trimming_turns_not_loaded(self):
        """Test get_turns_by_time_range_with_trimming when turns are not loaded."""
        episode = Episode(
            title="Test",
            description="Test",
            mp3_url="http://example.com/test.mp3",
            duration_seconds=1800.0,
            transcript="Test",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml"
        )
        with pytest.raises(RuntimeError, match="Turns not loaded"):
            episode.get_turns_by_time_range_with_trimming(0.0, 10.0)


class TestEpisodeSpeakerMethods:
    """Test episode speaker-related methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.episode = Episode(
            title="Test Episode",
            description="A test episode",
            mp3_url="http://example.com/episode.mp3",
            duration_seconds=3600.0,
            transcript="This is a test transcript.",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="http://example.com/rss.xml"
        )

        # Create test turns with different speakers
        self.turn1 = Turn(
            speaker=["SPEAKER_00"],
            text="Hello from host",
            start_time=0.0,
            end_time=5.0,
            duration=5.0,
            turn_count=1,
            inferred_speaker_role="host",
            inferred_speaker_name="John Doe"
        )
        self.turn2 = Turn(
            speaker=["SPEAKER_01"],
            text="Hello from guest",
            start_time=5.0,
            end_time=10.0,
            duration=5.0,
            turn_count=2,
            inferred_speaker_role="guest",
            inferred_speaker_name="Alice Johnson"
        )
        self.turn3 = Turn(
            speaker=["SPEAKER_00"],
            text="More from host",
            start_time=10.0,
            end_time=15.0,
            duration=5.0,
            turn_count=3,
            inferred_speaker_role="host",
            inferred_speaker_name="John Doe"
        )

        # Load turns
        self.episode._turns = [self.turn1, self.turn2, self.turn3]
        self.episode._turns_loaded = True

    def test_get_turns_by_speaker(self):
        """Test get_turns_by_speaker method."""
        turns = self.episode.get_turns_by_speaker("SPEAKER_00")
        assert len(turns) == 2
        assert all("SPEAKER_00" in turn.speaker for turn in turns)

    def test_get_turns_by_speaker_with_inferred_name(self):
        """Test get_turns_by_speaker with inferred speaker name."""
        turns = self.episode.get_turns_by_speaker("John Doe")
        assert len(turns) == 2
        assert all(turn.inferred_speaker_name == "John Doe" for turn in turns)

    def test_get_turns_by_min_length(self):
        """Test get_turns_by_min_length method."""
        # Create a turn with more words
        long_turn = Turn(
            speaker=["SPEAKER_02"],
            text="This is a much longer turn with many more words than the others",
            start_time=15.0,
            end_time=20.0,
            duration=5.0,
            turn_count=4
        )
        self.episode._turns.append(long_turn)

        turns = self.episode.get_turns_by_min_length(10)
        assert len(turns) == 1
        assert turns[0] == long_turn

    def test_get_turns_by_role(self):
        """Test get_turns_by_role method."""
        host_turns = self.episode.get_turns_by_role("host")
        assert len(host_turns) == 2
        assert all(turn.inferred_speaker_role == "host" for turn in host_turns)

        guest_turns = self.episode.get_turns_by_role("guest")
        assert len(guest_turns) == 1
        assert all(turn.inferred_speaker_role == "guest" for turn in guest_turns)

    def test_get_host_turns(self):
        """Test get_host_turns method."""
        host_turns = self.episode.get_host_turns()
        assert len(host_turns) == 2
        assert all(turn.inferred_speaker_role == "host" for turn in host_turns)

    def test_get_guest_turns(self):
        """Test get_guest_turns method."""
        guest_turns = self.episode.get_guest_turns()
        assert len(guest_turns) == 1
        assert all(turn.inferred_speaker_role == "guest" for turn in guest_turns)


class TestEpisodePropertiesAndMethods:
    """Test episode properties and methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.episode = Episode(
            title="Test Episode",
            description="A test episode",
            mp3_url="http://example.com/episode.mp3",
            duration_seconds=3600.0,
            transcript="This is a test transcript.",
            podcast_title="Test Podcast",
            podcast_description="A test podcast",
            rss_url="http://example.com/rss.xml"
        )

        # Create test turns
        self.turn1 = Turn(
            speaker=["SPEAKER_00"],
            text="Hello world",
            start_time=0.0,
            end_time=5.0,
            duration=5.0,
            turn_count=1
        )
        self.turn2 = Turn(
            speaker=["SPEAKER_01"],
            text="Hi there",
            start_time=5.0,
            end_time=10.0,
            duration=5.0,
            turn_count=2
        )

        # Load turns
        self.episode._turns = [self.turn1, self.turn2]
        self.episode._turns_loaded = True

    def test_turns_property(self):
        """Test turns property returns copy of turns."""
        turns = self.episode.turns
        assert len(turns) == 2
        assert turns is not self.episode._turns  # Should be a copy

    def test_turns_property_not_loaded(self):
        """Test turns property when turns are not loaded."""
        episode = Episode(
            title="Test",
            description="Test",
            mp3_url="http://example.com/test.mp3",
            duration_seconds=1800.0,
            transcript="Test",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml"
        )
        with pytest.raises(RuntimeError, match="Turns not loaded"):
            _ = episode.turns

    def test_turn_count_property(self):
        """Test turn_count property."""
        assert self.episode.turn_count == 2

    def test_turn_count_property_not_loaded(self):
        """Test turn_count property when turns are not loaded."""
        episode = Episode(
            title="Test",
            description="Test",
            mp3_url="http://example.com/test.mp3",
            duration_seconds=1800.0,
            transcript="Test",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml"
        )
        with pytest.raises(RuntimeError, match="Turns not loaded"):
            _ = episode.turn_count

    def test_has_turns_property(self):
        """Test has_turns property."""
        assert self.episode.has_turns is True

        # Test with no turns
        episode = Episode(
            title="Test",
            description="Test",
            mp3_url="http://example.com/test.mp3",
            duration_seconds=1800.0,
            transcript="Test",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml"
        )
        episode._turns_loaded = True
        episode._turns = []
        assert episode.has_turns is False

    def test_get_all_turns(self):
        """Test get_all_turns method."""
        turns = self.episode.get_all_turns()
        assert len(turns) == 2
        assert turns is not self.episode._turns  # Should be a copy

    def test_get_all_turns_not_loaded(self):
        """Test get_all_turns when turns are not loaded."""
        episode = Episode(
            title="Test",
            description="Test",
            mp3_url="http://example.com/test.mp3",
            duration_seconds=1800.0,
            transcript="Test",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml"
        )
        with pytest.raises(RuntimeError, match="Turns not loaded"):
            episode.get_all_turns()

    def test_get_turn_statistics(self):
        """Test get_turn_statistics method."""
        stats = self.episode.get_turn_statistics()
        assert isinstance(stats, dict)
        assert stats['total_turns'] == 2
        assert stats['total_words'] > 0
        assert stats['avg_turn_duration'] == 5.0
        assert 'speaker_distribution' in stats
        assert 'role_distribution' in stats

    def test_get_turn_statistics_no_turns(self):
        """Test get_turn_statistics with no turns."""
        episode = Episode(
            title="Test",
            description="Test",
            mp3_url="http://example.com/test.mp3",
            duration_seconds=1800.0,
            transcript="Test",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml"
        )
        episode._turns_loaded = True
        episode._turns = []

        stats = episode.get_turn_statistics()
        assert stats['total_turns'] == 0
        assert stats['total_words'] == 0
        assert stats['avg_turn_duration'] == 0.0

    def test_get_turn_statistics_not_loaded(self):
        """Test get_turn_statistics when turns are not loaded."""
        episode = Episode(
            title="Test",
            description="Test",
            mp3_url="http://example.com/test.mp3",
            duration_seconds=1800.0,
            transcript="Test",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml"
        )
        with pytest.raises(RuntimeError, match="Turns not loaded"):
            episode.get_turn_statistics()

    def test_load_turns(self):
        """Test load_turns method."""
        episode = Episode(
            title="Test",
            description="Test",
            mp3_url="http://example.com/test.mp3",
            duration_seconds=1800.0,
            transcript="Test",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml"
        )

        turns_data = [
            {
                'mp3url': 'http://example.com/test.mp3',
                'speaker': ['SPEAKER_00'],
                'turnText': 'Hello',
                'startTime': 0.0,
                'endTime': 5.0,
                'duration': 5.0,
                'turnCount': 1
            }
        ]

        episode.load_turns(turns_data)
        assert episode._turns_loaded is True
        assert len(episode._turns) == 1
        assert episode._turns[0].text == "Hello"

    def test_to_dict(self):
        """Test to_dict method."""
        episode_dict = self.episode.to_dict()
        assert isinstance(episode_dict, dict)
        assert episode_dict['title'] == "Test Episode"
        assert episode_dict['duration_minutes'] == 60.0
        assert episode_dict['turns_loaded'] is True
        assert episode_dict['num_turns'] == 2

    def test_str_repr(self):
        """Test string representation."""
        str_repr = str(self.episode)
        assert "Test Episode" in str_repr
        assert "60.0min" in str_repr

    def test_repr(self):
        """Test detailed string representation."""
        repr_str = repr(self.episode)
        assert "Test Episode" in repr_str
        assert "3600" in repr_str

    def test_len(self):
        """Test len method."""
        assert len(self.episode) == 2

    def test_len_not_loaded(self):
        """Test len when turns are not loaded."""
        episode = Episode(
            title="Test",
            description="Test",
            mp3_url="http://example.com/test.mp3",
            duration_seconds=1800.0,
            transcript="Test",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml"
        )
        with pytest.raises(RuntimeError, match="Turns not loaded"):
            len(episode)

    def test_getitem(self):
        """Test getitem method."""
        assert self.episode[0] == self.turn1
        assert self.episode[1] == self.turn2

    def test_getitem_not_loaded(self):
        """Test getitem when turns are not loaded."""
        episode = Episode(
            title="Test",
            description="Test",
            mp3_url="http://example.com/test.mp3",
            duration_seconds=1800.0,
            transcript="Test",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml"
        )
        with pytest.raises(RuntimeError, match="Turns not loaded"):
            _ = episode[0]

    def test_iter(self):
        """Test iter method."""
        turns = list(self.episode)
        assert len(turns) == 2
        assert turns[0] == self.turn1
        assert turns[1] == self.turn2

    def test_iter_not_loaded(self):
        """Test iter when turns are not loaded."""
        episode = Episode(
            title="Test",
            description="Test",
            mp3_url="http://example.com/test.mp3",
            duration_seconds=1800.0,
            transcript="Test",
            podcast_title="Test",
            podcast_description="Test",
            rss_url="http://example.com/rss.xml"
        )
        with pytest.raises(RuntimeError, match="Turns not loaded"):
            list(episode)


class TestEpisodeValidation:
    """Test episode validation."""

    def test_episode_validation_empty_title(self):
        """Test episode validation with empty title."""
        with pytest.raises(ValueError, match="Episode title cannot be empty"):
            Episode(
                title="",
                description="Test",
                mp3_url="http://example.com/test.mp3",
                duration_seconds=1800.0,
                transcript="Test",
                podcast_title="Test",
                podcast_description="Test",
                rss_url="http://example.com/rss.xml"
            )

    def test_episode_validation_negative_duration(self):
        """Test episode validation with negative duration."""
        with pytest.raises(ValueError, match="Duration cannot be negative"):
            Episode(
                title="Test",
                description="Test",
                mp3_url="http://example.com/test.mp3",
                duration_seconds=-100.0,
                transcript="Test",
                podcast_title="Test",
                podcast_description="Test",
                rss_url="http://example.com/rss.xml"
            )

    def test_episode_validation_empty_mp3_url(self):
        """Test episode validation with empty MP3 URL."""
        with pytest.raises(ValueError, match="MP3 URL cannot be empty"):
            Episode(
                title="Test",
                description="Test",
                mp3_url="",
                duration_seconds=1800.0,
                transcript="Test",
                podcast_title="Test",
                podcast_description="Test",
                rss_url="http://example.com/rss.xml"
            )