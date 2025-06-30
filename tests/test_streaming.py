"""
Test suite for streaming mode functionality in the SPORC package.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from sporc import SPORCDataset, SPORCError


class TestStreamingMode:
    """Test streaming mode functionality."""

    @patch('sporc.dataset.load_dataset')
    def test_streaming_initialization(self, mock_load_dataset):
        """Test SPORCDataset initialization in streaming mode."""
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
        mock_speaker_turn_data.__iter__ = lambda x: iter([])
        mock_speaker_turn_data.__len__.return_value = 0

        mock_load_dataset.side_effect = [mock_episode_data, mock_speaker_turn_data]

        # Initialize dataset in streaming mode
        dataset = SPORCDataset(streaming=True)

        assert dataset.streaming is True
        assert dataset._loaded is True
        assert len(dataset._podcasts) == 0  # No data loaded initially
        assert len(dataset._episodes) == 0  # No data loaded initially

    @patch('sporc.dataset.load_dataset')
    def test_streaming_search_podcast(self, mock_load_dataset):
        """Test podcast search in streaming mode."""
        # Mock dataset with episodes
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
        mock_speaker_turn_data.__len__.return_value = 0

        mock_load_dataset.side_effect = [mock_episode_data, mock_speaker_turn_data]

        dataset = SPORCDataset(streaming=True)

        # Search for podcast
        podcast = dataset.search_podcast("Test Podcast")
        assert podcast.title == "Test Podcast"
        assert podcast.num_episodes == 1

    @patch('sporc.dataset.load_dataset')
    def test_streaming_search_episodes(self, mock_load_dataset):
        """Test episode search in streaming mode."""
        # Mock dataset with episodes
        mock_episode_data = MagicMock()
        mock_episode_data.__iter__ = lambda x: iter([
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
        mock_speaker_turn_data.__len__.return_value = 0

        mock_load_dataset.side_effect = [mock_episode_data, mock_speaker_turn_data]

        dataset = SPORCDataset(streaming=True)

        # Search for long episodes
        long_episodes = dataset.search_episodes(min_duration=1800)  # 30+ minutes
        assert len(long_episodes) == 1
        assert long_episodes[0].title == "Long Episode"

    @patch('sporc.dataset.load_dataset')
    def test_streaming_iterate_episodes(self, mock_load_dataset):
        """Test episode iteration in streaming mode."""
        # Mock dataset with episodes
        mock_episode_data = MagicMock()
        mock_episode_data.__iter__ = lambda x: iter([
            {
                'podTitle': 'Test Podcast',
                'podDescription': 'A test podcast',
                'rssUrl': 'https://example.com/rss.xml',
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
                'podTitle': 'Test Podcast',
                'podDescription': 'A test podcast',
                'rssUrl': 'https://example.com/rss.xml',
                'epTitle': 'Episode 2',
                'epDescription': 'Second episode',
                'mp3url': 'https://example.com/ep2.mp3',
                'durationSeconds': 3600.0,
                'transcript': 'Transcript 2',
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
        mock_episode_data.__len__.return_value = 2

        mock_speaker_turn_data = MagicMock()
        mock_speaker_turn_data.__iter__ = lambda x: iter([])
        mock_speaker_turn_data.__len__.return_value = 0

        mock_load_dataset.side_effect = [mock_episode_data, mock_speaker_turn_data]

        dataset = SPORCDataset(streaming=True)

        # Iterate over episodes
        episodes = list(dataset.iterate_episodes())
        assert len(episodes) == 2
        assert episodes[0].title == "Episode 1"
        assert episodes[1].title == "Episode 2"

    @patch('sporc.dataset.load_dataset')
    def test_streaming_iterate_podcasts(self, mock_load_dataset):
        """Test podcast iteration in streaming mode."""
        # Mock dataset with episodes from different podcasts
        mock_episode_data = MagicMock()
        mock_episode_data.__iter__ = lambda x: iter([
            {
                'podTitle': 'Podcast 1',
                'podDescription': 'First podcast',
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
                'podTitle': 'Podcast 2',
                'podDescription': 'Second podcast',
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
        mock_speaker_turn_data.__len__.return_value = 0

        mock_load_dataset.side_effect = [mock_episode_data, mock_speaker_turn_data]

        dataset = SPORCDataset(streaming=True)

        # Iterate over podcasts
        podcasts = list(dataset.iterate_podcasts())
        assert len(podcasts) == 2
        assert podcasts[0].title == "Podcast 1"
        assert podcasts[1].title == "Podcast 2"

    @patch('sporc.dataset.load_dataset')
    def test_streaming_length_error(self, mock_load_dataset):
        """Test that len() raises an error in streaming mode."""
        # Mock dataset
        mock_episode_data = MagicMock()
        mock_episode_data.__iter__ = lambda x: iter([])
        mock_speaker_turn_data = MagicMock()
        mock_speaker_turn_data.__iter__ = lambda x: iter([])
        mock_load_dataset.side_effect = [mock_episode_data, mock_speaker_turn_data]

        dataset = SPORCDataset(streaming=True)

        # Test that len() raises an error in streaming mode
        with pytest.raises(RuntimeError, match="len\\(\\) is not available in streaming mode"):
            len(dataset)

    @patch('sporc.dataset.load_dataset')
    def test_streaming_iterate_episodes_error(self, mock_load_dataset):
        """Test that iterate_episodes() works in both streaming and memory modes."""
        # Mock dataset
        mock_episode_data = MagicMock()
        mock_episode_data.__iter__ = lambda x: iter([])
        mock_speaker_turn_data = MagicMock()
        mock_speaker_turn_data.__iter__ = lambda x: iter([])
        mock_load_dataset.side_effect = [mock_episode_data, mock_speaker_turn_data]

        dataset = SPORCDataset(streaming=False)

        # Test that iterate_episodes() works in memory mode (should not raise error)
        episodes = list(dataset.iterate_episodes())
        assert len(episodes) == 0  # Empty dataset

    @patch('sporc.dataset.load_dataset')
    def test_streaming_iterate_podcasts_error(self, mock_load_dataset):
        """Test that iterate_podcasts() raises error in memory mode."""
        # Mock dataset
        mock_episode_data = MagicMock()
        mock_episode_data.__iter__ = lambda x: iter([])
        mock_speaker_turn_data = MagicMock()
        mock_speaker_turn_data.__iter__ = lambda x: iter([])
        mock_load_dataset.side_effect = [mock_episode_data, mock_speaker_turn_data]

        dataset = SPORCDataset(streaming=False)

        # Test that iterate_podcasts() raises RuntimeError
        with pytest.raises(RuntimeError, match="iterate_podcasts\\(\\) is only available in streaming mode"):
            list(dataset.iterate_podcasts())


if __name__ == "__main__":
    pytest.main([__file__])