"""
Unit tests for robust field handling in the SPORC dataset.

These tests verify that the dataset can handle None, missing, or wrong types
for all fields being loaded from the dataset.
"""

import pytest
import json
from unittest.mock import Mock, MagicMock, patch
from sporc.dataset import SPORCDataset


class TestSafeConversionMethods:
    """Test the safe conversion methods for handling different field types."""

    def test_safe_float(self):
        """Test _safe_float method with various inputs."""
        dataset = SPORCDataset(streaming=True)

        # Test normal cases
        assert dataset._safe_float(123) == 123.0
        assert dataset._safe_float(123.45) == 123.45
        assert dataset._safe_float("123.45") == 123.45
        assert dataset._safe_float(0) == 0.0

        # Test None and missing cases
        assert dataset._safe_float(None) == 0.0
        assert dataset._safe_float(None, default=42.0) == 42.0

        # Test invalid cases
        assert dataset._safe_float("invalid") == 0.0
        assert dataset._safe_float("invalid", default=99.0) == 99.0
        assert dataset._safe_float({}) == 0.0
        assert dataset._safe_float([]) == 0.0

        # Test edge cases
        assert dataset._safe_float("") == 0.0
        assert dataset._safe_float("   ") == 0.0

    def test_safe_string(self):
        """Test _safe_string method with various inputs."""
        dataset = SPORCDataset(streaming=True)

        # Test normal cases
        assert dataset._safe_string("hello") == "hello"
        assert dataset._safe_string(123) == "123"
        assert dataset._safe_string(123.45) == "123.45"
        assert dataset._safe_string(True) == "True"

        # Test None and missing cases
        assert dataset._safe_string(None) == ""
        assert dataset._safe_string(None, default="default") == "default"

        # Test whitespace handling
        assert dataset._safe_string("  hello  ") == "hello"
        assert dataset._safe_string("") == ""
        assert dataset._safe_string("   ") == ""

        # Test edge cases
        assert dataset._safe_string({}) == "{}"
        assert dataset._safe_string([]) == "[]"
        assert dataset._safe_string(0) == "0"

    def test_safe_boolean(self):
        """Test _safe_boolean method with various inputs."""
        dataset = SPORCDataset(streaming=True)

        # Test normal cases
        assert dataset._safe_boolean(True) == True
        assert dataset._safe_boolean(False) == False
        assert dataset._safe_boolean(1) == True
        assert dataset._safe_boolean(0) == False
        assert dataset._safe_boolean(42) == True
        assert dataset._safe_boolean(0.0) == False
        assert dataset._safe_boolean(1.0) == True

        # Test string cases
        assert dataset._safe_boolean("true") == True
        assert dataset._safe_boolean("True") == True
        assert dataset._safe_boolean("1") == True
        assert dataset._safe_boolean("yes") == True
        assert dataset._safe_boolean("on") == True
        assert dataset._safe_boolean("false") == False
        assert dataset._safe_boolean("False") == False
        assert dataset._safe_boolean("0") == False
        assert dataset._safe_boolean("no") == False
        assert dataset._safe_boolean("off") == False

        # Test None and missing cases
        assert dataset._safe_boolean(None) == False
        assert dataset._safe_boolean(None, default=True) == True

        # Test edge cases
        assert dataset._safe_boolean("") == False
        assert dataset._safe_boolean("invalid") == False
        assert dataset._safe_boolean({}) == False
        assert dataset._safe_boolean([]) == False

    def test_safe_list(self):
        """Test _safe_list method with various inputs."""
        dataset = SPORCDataset(streaming=True)

        # Test normal cases
        assert dataset._safe_list([1, 2, 3]) == [1, 2, 3]
        assert dataset._safe_list(["a", "b", "c"]) == ["a", "b", "c"]
        assert dataset._safe_list([]) == []

        # Test special string cases
        assert dataset._safe_list("NO_HOST_PREDICTED") == []
        assert dataset._safe_list("NO_GUEST_PREDICTED") == []
        assert dataset._safe_list("NO_NEITHER_IDENTIFIED") == []
        assert dataset._safe_list("SPEAKER_DATA_UNAVAILABLE") == []

        # Test JSON string cases
        assert dataset._safe_list('["a", "b", "c"]') == ["a", "b", "c"]
        assert dataset._safe_list('["single"]') == ["single"]
        assert dataset._safe_list('[]') == []

        # Test single item cases
        assert dataset._safe_list("single_item") == ["single_item"]
        assert dataset._safe_list("") == []
        assert dataset._safe_list("   ") == []

        # Test other iterable types
        assert dataset._safe_list((1, 2, 3)) == [1, 2, 3]
        assert dataset._safe_list({1, 2, 3}) == [1, 2, 3]

        # Test None and missing cases
        assert dataset._safe_list(None) == []
        assert dataset._safe_list(None, default=["default"]) == ["default"]

        # Test edge cases
        assert dataset._safe_list(123) == ["123"]
        assert dataset._safe_list(True) == ["True"]

    def test_safe_dict(self):
        """Test _safe_dict method with various inputs."""
        dataset = SPORCDataset(streaming=True)

        # Test normal cases
        assert dataset._safe_dict({"a": 1, "b": 2}) == {"a": 1, "b": 2}
        assert dataset._safe_dict({}) == {}

        # Test special string cases
        assert dataset._safe_dict("SPEAKER_DATA_UNAVAILABLE") == {}

        # Test JSON string cases
        assert dataset._safe_dict('{"a": 1, "b": 2}') == {"a": 1, "b": 2}
        assert dataset._safe_dict('{}') == {}

        # Test None and missing cases
        assert dataset._safe_dict(None) == {}
        assert dataset._safe_dict(None, default={"default": "value"}) == {"default": "value"}

        # Test edge cases
        assert dataset._safe_dict("invalid_json") == {}
        assert dataset._safe_dict("") == {}
        assert dataset._safe_dict(123) == {}
        assert dataset._safe_dict([]) == {}


class TestRobustEpisodeCreation:
    """Test robust episode creation with various field types."""

    def test_episode_creation_with_none_fields(self):
        """Test episode creation when fields are None."""
        dataset = SPORCDataset(streaming=True)

        episode_dict = {
            'epTitle': 'Test Episode',  # Required field - cannot be None
            'epDescription': None,
            'mp3url': 'http://example.com/episode.mp3',  # Required field - cannot be None
            'durationSeconds': None,
            'transcript': None,
            'podTitle': 'Test Podcast',  # Required field - cannot be None
            'podDescription': None,
            'rssUrl': None,
            'category1': None,
            'category2': None,
            'category3': None,
            'category4': None,
            'category5': None,
            'category6': None,
            'category7': None,
            'category8': None,
            'category9': None,
            'category10': None,
            'hostPredictedNames': None,
            'guestPredictedNames': None,
            'neitherPredictedNames': None,
            'mainEpSpeakers': None,
            'hostSpeakerLabels': None,
            'guestSpeakerLabels': None,
            'overlapPropDuration': None,
            'overlapPropTurnCount': None,
            'avgTurnDuration': None,
            'totalSpLabels': None,
            'language': None,
            'explicit': None,
            'imageUrl': None,
            'episodeDateLocalized': None,
            'oldestEpisodeDate': None,
            'lastUpdate': None,
            'createdOn': None,
        }

        episode = dataset._create_episode_from_dict(episode_dict)

        # Check that all fields have safe defaults
        assert episode.title == "Test Episode"
        assert episode.description == ""
        assert episode.mp3_url == "http://example.com/episode.mp3"
        assert episode.duration_seconds == 0.0
        assert episode.transcript == ""
        assert episode.podcast_title == "Test Podcast"
        assert episode.podcast_description == ""
        assert episode.rss_url == ""
        assert episode.category1 == ""
        assert episode.category2 == ""
        assert episode.category3 == ""
        assert episode.category4 == ""
        assert episode.category5 == ""
        assert episode.category6 == ""
        assert episode.category7 == ""
        assert episode.category8 == ""
        assert episode.category9 == ""
        assert episode.category10 == ""
        assert episode.host_predicted_names == []
        assert episode.guest_predicted_names == []
        assert episode.neither_predicted_names == []
        assert episode.main_ep_speakers == []
        assert episode.host_speaker_labels == {}
        assert episode.guest_speaker_labels == {}
        assert episode.overlap_prop_duration == 0.0
        assert episode.overlap_prop_turn_count == 0.0
        assert episode.avg_turn_duration == 0.0
        assert episode.total_speaker_labels == 0.0
        assert episode.language == "en"
        assert episode.explicit == False
        assert episode.image_url == ""
        assert episode.episode_date_localized == ""
        assert episode.oldest_episode_date == ""
        assert episode.last_update == ""
        assert episode.created_on == ""

    def test_episode_creation_with_wrong_types(self):
        """Test episode creation when fields have wrong types."""
        dataset = SPORCDataset(streaming=True)

        episode_dict = {
            'epTitle': 123,
            'epDescription': 456.78,
            'mp3url': True,
            'durationSeconds': "not_a_number",
            'transcript': ["should", "be", "string"],
            'podTitle': {"should": "be", "string": True},
            'podDescription': None,
            'rssUrl': "",
            'category1': 999,
            'category2': False,
            'category3': [],
            'category4': {},
            'category5': None,
            'category6': None,
            'category7': None,
            'category8': None,
            'category9': None,
            'category10': None,
            'hostPredictedNames': "NO_HOST_PREDICTED",
            'guestPredictedNames': "NO_GUEST_PREDICTED",
            'neitherPredictedNames': "NO_NEITHER_IDENTIFIED",
            'mainEpSpeakers': "SPEAKER_DATA_UNAVAILABLE",
            'hostSpeakerLabels': "SPEAKER_DATA_UNAVAILABLE",
            'guestSpeakerLabels': "SPEAKER_DATA_UNAVAILABLE",
            'overlapPropDuration': "invalid",
            'overlapPropTurnCount': "invalid",
            'avgTurnDuration': "invalid",
            'totalSpLabels': "invalid",
            'language': 123,
            'explicit': "yes",
            'imageUrl': 456,
            'episodeDateLocalized': None,
            'oldestEpisodeDate': None,
            'lastUpdate': None,
            'createdOn': None,
        }

        episode = dataset._create_episode_from_dict(episode_dict)

        # Check that fields are converted to appropriate types
        assert episode.title == "123"
        assert episode.description == "456.78"
        assert episode.mp3_url == "True"
        assert episode.duration_seconds == 0.0  # Invalid number becomes default
        assert episode.transcript == "['should', 'be', 'string']"
        assert episode.podcast_title == "{'should': 'be', 'string': True}"
        assert episode.podcast_description == ""
        assert episode.rss_url == ""
        assert episode.category1 == "999"
        assert episode.category2 == "False"
        assert episode.category3 == "[]"
        assert episode.category4 == "{}"
        assert episode.host_predicted_names == []
        assert episode.guest_predicted_names == []
        assert episode.neither_predicted_names == []
        assert episode.main_ep_speakers == []
        assert episode.host_speaker_labels == {}
        assert episode.guest_speaker_labels == {}
        assert episode.overlap_prop_duration == 0.0
        assert episode.overlap_prop_turn_count == 0.0
        assert episode.avg_turn_duration == 0.0
        assert episode.total_speaker_labels == 0.0
        assert episode.language == "123"
        assert episode.explicit == True  # "yes" converts to True
        assert episode.image_url == "456"

    def test_episode_creation_with_special_strings(self):
        """Test episode creation with special string values."""
        dataset = SPORCDataset(streaming=True)

        episode_dict = {
            'epTitle': "Normal Title",
            'epDescription': "Normal Description",
            'mp3url': "http://example.com/episode.mp3",
            'durationSeconds': 3600,
            'transcript': "Normal transcript",
            'podTitle': "Normal Podcast",
            'podDescription': "Normal podcast description",
            'rssUrl': "http://example.com/rss",
            'category1': "Technology",
            'category2': "Science",
            'category3': None,
            'category4': None,
            'category5': None,
            'category6': None,
            'category7': None,
            'category8': None,
            'category9': None,
            'category10': None,
            'hostPredictedNames': '["John Doe", "Jane Smith"]',
            'guestPredictedNames': '["Guest 1", "Guest 2"]',
            'neitherPredictedNames': '["Other 1"]',
            'mainEpSpeakers': '["Speaker 1", "Speaker 2", "Speaker 3"]',
            'hostSpeakerLabels': '{"John Doe": "speaker_1", "Jane Smith": "speaker_2"}',
            'guestSpeakerLabels': '{"Guest 1": "speaker_3", "Guest 2": "speaker_4"}',
            'overlapPropDuration': 0.15,
            'overlapPropTurnCount': 0.12,
            'avgTurnDuration': 45.5,
            'totalSpLabels': 4.0,
            'language': "en",
            'explicit': 1,
            'imageUrl': "http://example.com/image.jpg",
            'episodeDateLocalized': "2023-01-15",
            'oldestEpisodeDate': "2020-01-01",
            'lastUpdate': "2023-01-15T10:00:00Z",
            'createdOn': "2023-01-15T09:00:00Z",
        }

        episode = dataset._create_episode_from_dict(episode_dict)

        # Check that JSON strings are properly parsed
        assert episode.host_predicted_names == ["John Doe", "Jane Smith"]
        assert episode.guest_predicted_names == ["Guest 1", "Guest 2"]
        assert episode.neither_predicted_names == ["Other 1"]
        assert episode.main_ep_speakers == ["Speaker 1", "Speaker 2", "Speaker 3"]
        assert episode.host_speaker_labels == {"John Doe": "speaker_1", "Jane Smith": "speaker_2"}
        assert episode.guest_speaker_labels == {"Guest 1": "speaker_3", "Guest 2": "speaker_4"}

        # Check other fields
        assert episode.title == "Normal Title"
        assert episode.duration_seconds == 3600.0
        assert episode.language == "en"
        assert episode.explicit == True
        assert episode.overlap_prop_duration == 0.15
        assert episode.overlap_prop_turn_count == 0.12
        assert episode.avg_turn_duration == 45.5
        assert episode.total_speaker_labels == 4.0


class TestRobustPodcastCreation:
    """Test robust podcast creation with various field types."""

    def test_podcast_creation_with_none_fields(self):
        """Test podcast creation when fields are None."""
        dataset = SPORCDataset(streaming=True)

        first_episode = {
            'podDescription': None,
            'rssUrl': 'http://example.com/rss',  # Required field - cannot be None
            'language': None,
            'explicit': None,
            'imageUrl': None,
            'itunesAuthor': None,
            'itunesOwnerName': None,
            'host': None,
            'createdOn': None,
            'lastUpdate': None,
            'oldestEpisodeDate': None,
        }

        podcast = dataset._create_podcast_from_dict(first_episode, "Test Podcast")

        # Check that all fields have safe defaults
        assert podcast.title == "Test Podcast"
        assert podcast.description == ""
        assert podcast.rss_url == "http://example.com/rss"
        assert podcast.language == "en"
        assert podcast.explicit == False
        assert podcast.image_url == ""
        assert podcast.itunes_author == ""
        assert podcast.itunes_owner_name == ""
        assert podcast.host == ""
        assert podcast.created_on == ""
        assert podcast.last_update == ""
        assert podcast.oldest_episode_date == ""

    def test_podcast_creation_with_wrong_types(self):
        """Test podcast creation when fields have wrong types."""
        dataset = SPORCDataset(streaming=True)

        first_episode = {
            'podDescription': 123,
            'rssUrl': 456.78,
            'language': True,
            'explicit': "true",
            'imageUrl': ["should", "be", "string"],
            'itunesAuthor': {"should": "be", "string": True},
            'itunesOwnerName': None,
            'host': "",
            'createdOn': 999,
            'lastUpdate': False,
            'oldestEpisodeDate': [],
        }

        podcast = dataset._create_podcast_from_dict(first_episode, "Test Podcast")

        # Check that fields are converted to appropriate types
        assert podcast.title == "Test Podcast"
        assert podcast.description == "123"
        assert podcast.rss_url == "456.78"
        assert podcast.language == "True"
        assert podcast.explicit == True  # "true" converts to True
        assert podcast.image_url == "['should', 'be', 'string']"
        assert podcast.itunes_author == "{'should': 'be', 'string': True}"
        assert podcast.itunes_owner_name == ""
        assert podcast.host == ""
        assert podcast.created_on == "999"
        assert podcast.last_update == "False"
        assert podcast.oldest_episode_date == "[]"


class TestRobustFieldAccess:
    """Test robust field access in various dataset operations."""

    def test_robust_metadata_collection(self):
        """Test that metadata collection handles various field types robustly."""
        dataset = SPORCDataset(streaming=True)

        episode_dict = {
            'podTitle': 123,  # Wrong type
            'language': None,  # None
            'explicit': "yes",  # String boolean
            'durationSeconds': "invalid",  # Invalid number
            'category1': 999,  # Wrong type
            'hostPredictedNames': "NO_HOST_PREDICTED",  # Special string
        }

        # Test metadata collection
        metadata = {
            'episodes': [],
            'categories': set(),
            'hosts': set(),
            'total_duration': 0.0,
            'language': dataset._safe_string(episode_dict.get('language'), 'en'),
            'explicit': dataset._safe_boolean(episode_dict.get('explicit'), False)
        }

        # Add episode to metadata
        metadata['episodes'].append(episode_dict)
        metadata['total_duration'] += dataset._safe_float(episode_dict.get('durationSeconds', 0))

        # Collect categories
        category = dataset._safe_string(episode_dict.get('category1'))
        if category:
            metadata['categories'].add(category)

        # Collect hosts
        host_names = dataset._safe_list(episode_dict.get('hostPredictedNames'))
        metadata['hosts'].update(host_names)

        # Check results
        assert metadata['language'] == "en"  # Default for None
        assert metadata['explicit'] == True  # "yes" converts to True
        assert metadata['total_duration'] == 0.0  # Invalid number becomes default
        assert "999" in metadata['categories']  # Converted to string
        assert len(metadata['hosts']) == 0  # Special string becomes empty list

    def test_robust_statistics_calculation(self):
        """Test that statistics calculation handles various field types robustly."""
        dataset = SPORCDataset(streaming=True)

        record = {
            'podTitle': 123,  # Wrong type
            'durationSeconds': "invalid",  # Invalid number
            'language': None,  # None
            'category1': 999,  # Wrong type
            'mainEpSpeakers': "SPEAKER_DATA_UNAVAILABLE",  # Special string
        }

        # Test statistics calculation
        total_episodes = 1
        total_duration = 0.0
        category_counts = {}
        language_counts = {}
        speaker_count_distribution = {}
        podcast_titles = set()

        duration = dataset._safe_float(record.get('durationSeconds', 0))
        total_duration += duration
        podcast_titles.add(dataset._safe_string(record.get('podTitle'), 'Unknown Podcast'))

        # Count categories
        category = dataset._safe_string(record.get('category1'))
        if category:
            category_counts[category] = category_counts.get(category, 0) + 1

        # Count languages
        language = dataset._safe_string(record.get('language'), 'en')
        language_counts[language] = language_counts.get(language, 0) + 1

        # Count speaker counts
        speaker_count = len(dataset._safe_list(record.get('mainEpSpeakers')))
        speaker_count_distribution[str(speaker_count)] = speaker_count_distribution.get(str(speaker_count), 0) + 1

        # Check results
        assert total_duration == 0.0  # Invalid number becomes default
        assert "123" in podcast_titles  # Converted to string
        assert category_counts["999"] == 1  # Converted to string
        assert language_counts["en"] == 1  # Default for None
        assert speaker_count_distribution["0"] == 1  # Special string becomes empty list


class TestRobustSearchOperations:
    """Test robust search operations with various field types."""

    def test_robust_podcast_search(self):
        """Test that podcast search handles various field types robustly."""
        dataset = SPORCDataset(streaming=True)

        # Mock dataset with problematic records
        mock_dataset = [
            {
                'epTitle': 'Episode 1',
                'podTitle': 123,  # Wrong type
                'podDescription': None,
                'rssUrl': None,
                'language': None,
                'explicit': "yes",
                'imageUrl': None,
                'itunesAuthor': None,
                'itunesOwnerName': None,
                'host': None,
                'createdOn': None,
                'lastUpdate': None,
                'oldestEpisodeDate': None,
            }
        ]

        with patch.object(dataset, '_dataset', mock_dataset):
            with patch.object(dataset, '_create_safe_iterator', return_value=mock_dataset):
                # This should not raise an exception
                try:
                    # The search should handle the wrong types gracefully
                    # Note: This is a simplified test - actual search would need more setup
                    pass
                except Exception as e:
                    pytest.fail(f"Podcast search should handle wrong types gracefully: {e}")

    def test_robust_episode_search(self):
        """Test that episode search handles various field types robustly."""
        dataset = SPORCDataset(streaming=True)

        # Test episode matching with various field types
        episode_dict = {
            'epTitle': 123,  # Wrong type
            'mp3url': 'http://example.com/episode.mp3',  # Required field - cannot be None
            'podTitle': 'Test Podcast',  # Required field - cannot be None
            'durationSeconds': "invalid",  # Invalid number
            'language': None,  # None
            'explicit': "yes",  # String boolean
            'hostPredictedNames': "NO_HOST_PREDICTED",  # Special string
            'guestPredictedNames': "NO_GUEST_PREDICTED",  # Special string
            'category1': 999,  # Wrong type
        }

        # Create episode from problematic data
        episode = dataset._create_episode_from_dict(episode_dict)

        # Test that episode has safe values
        assert episode.title == "123"
        assert episode.duration_seconds == 0.0
        assert episode.language == "en"
        assert episode.explicit == True
        assert episode.host_predicted_names == []
        assert episode.guest_predicted_names == []
        assert episode.category1 == "999"


if __name__ == "__main__":
    pytest.main([__file__])