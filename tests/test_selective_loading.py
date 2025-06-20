"""
Test suite for selective loading functionality in the SPORC package.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from sporc import SPORCDataset, SPORCError


class TestSelectiveLoading:
    """Test selective loading functionality."""

    @patch('sporc.dataset.load_dataset')
    def test_selective_loading_initialization(self, mock_load_dataset):
        """Test SPORCDataset initialization with selective loading."""
        # Mock the dataset loading
        mock_episode_data = MagicMock()
        mock_episode_data.__iter__ = lambda x: iter([
            {
                'podTitle': 'Education Podcast',
                'podDescription': 'An education podcast',
                'rssUrl': 'https://example.com/education.xml',
                'epTitle': 'Education Episode 1',
                'epDescription': 'First education episode',
                'mp3url': 'https://example.com/edu1.mp3',
                'durationSeconds': 1800.0,
                'transcript': 'Education transcript',
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
                'podTitle': 'Science Podcast',
                'podDescription': 'A science podcast',
                'rssUrl': 'https://example.com/science.xml',
                'epTitle': 'Science Episode 1',
                'epDescription': 'First science episode',
                'mp3url': 'https://example.com/sci1.mp3',
                'durationSeconds': 3600.0,
                'transcript': 'Science transcript',
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
                'category1': 'science',
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

        # Initialize dataset in streaming mode
        dataset = SPORCDataset(streaming=True)

        # Load education subset
        dataset.load_podcast_subset(categories=['education'])

        assert dataset.streaming is True
        assert dataset._selective_mode is True
        assert dataset._loaded is True
        assert len(dataset._podcasts) == 1  # Only education podcast
        assert len(dataset._episodes) == 1  # Only education episode
        assert 'Education Podcast' in dataset._podcasts

    @patch('sporc.dataset.load_dataset')
    def test_selective_loading_by_category(self, mock_load_dataset):
        """Test selective loading by category."""
        # Mock dataset with episodes from different categories
        mock_episode_data = MagicMock()
        mock_episode_data.__iter__ = lambda x: iter([
            {
                'podTitle': 'Education Podcast',
                'podDescription': 'An education podcast',
                'rssUrl': 'https://example.com/education.xml',
                'epTitle': 'Education Episode',
                'epDescription': 'Education episode',
                'mp3url': 'https://example.com/edu.mp3',
                'durationSeconds': 1800.0,
                'transcript': 'Education transcript',
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
                'podTitle': 'Science Podcast',
                'podDescription': 'A science podcast',
                'rssUrl': 'https://example.com/science.xml',
                'epTitle': 'Science Episode',
                'epDescription': 'Science episode',
                'mp3url': 'https://example.com/sci.mp3',
                'durationSeconds': 3600.0,
                'transcript': 'Science transcript',
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
                'category1': 'science',
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

        # Load education subset
        dataset.load_podcast_subset(categories=['education'])

        assert len(dataset._podcasts) == 1
        assert 'Education Podcast' in dataset._podcasts
        assert 'Science Podcast' not in dataset._podcasts

        # Load science subset
        dataset.load_podcast_subset(categories=['science'])

        assert len(dataset._podcasts) == 1
        assert 'Science Podcast' in dataset._podcasts
        assert 'Education Podcast' not in dataset._podcasts

    @patch('sporc.dataset.load_dataset')
    def test_selective_loading_by_host(self, mock_load_dataset):
        """Test selective loading by host."""
        # Mock dataset with episodes from different hosts
        mock_episode_data = MagicMock()
        mock_episode_data.__iter__ = lambda x: iter([
            {
                'podTitle': 'John Podcast',
                'podDescription': 'John\'s podcast',
                'rssUrl': 'https://example.com/john.xml',
                'epTitle': 'John Episode',
                'epDescription': 'John\'s episode',
                'mp3url': 'https://example.com/john.mp3',
                'durationSeconds': 1800.0,
                'transcript': 'John transcript',
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
                'podTitle': 'Jane Podcast',
                'podDescription': 'Jane\'s podcast',
                'rssUrl': 'https://example.com/jane.xml',
                'epTitle': 'Jane Episode',
                'epDescription': 'Jane\'s episode',
                'mp3url': 'https://example.com/jane.mp3',
                'durationSeconds': 3600.0,
                'transcript': 'Jane transcript',
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
                'category1': 'science',
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

        # Load John's podcasts
        dataset.load_podcast_subset(hosts=['John Doe'])

        assert len(dataset._podcasts) == 1
        assert 'John Podcast' in dataset._podcasts
        assert 'Jane Podcast' not in dataset._podcasts

        # Load Jane's podcasts
        dataset.load_podcast_subset(hosts=['Jane Smith'])

        assert len(dataset._podcasts) == 1
        assert 'Jane Podcast' in dataset._podcasts
        assert 'John Podcast' not in dataset._podcasts

    @patch('sporc.dataset.load_dataset')
    def test_selective_loading_by_episode_count(self, mock_load_dataset):
        """Test selective loading by episode count."""
        # Mock dataset with podcasts having different episode counts
        mock_episode_data = MagicMock()
        mock_episode_data.__iter__ = lambda x: iter([
            # Podcast with 1 episode
            {
                'podTitle': 'Small Podcast',
                'podDescription': 'A small podcast',
                'rssUrl': 'https://example.com/small.xml',
                'epTitle': 'Small Episode',
                'epDescription': 'Small episode',
                'mp3url': 'https://example.com/small.mp3',
                'durationSeconds': 1800.0,
                'transcript': 'Small transcript',
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
            # Podcast with 2 episodes (same podcast, different episode)
            {
                'podTitle': 'Large Podcast',
                'podDescription': 'A large podcast',
                'rssUrl': 'https://example.com/large.xml',
                'epTitle': 'Large Episode 1',
                'epDescription': 'First large episode',
                'mp3url': 'https://example.com/large1.mp3',
                'durationSeconds': 1800.0,
                'transcript': 'Large transcript 1',
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
                'category1': 'science',
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
                'podTitle': 'Large Podcast',
                'podDescription': 'A large podcast',
                'rssUrl': 'https://example.com/large.xml',
                'epTitle': 'Large Episode 2',
                'epDescription': 'Second large episode',
                'mp3url': 'https://example.com/large2.mp3',
                'durationSeconds': 3600.0,
                'transcript': 'Large transcript 2',
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
                'category1': 'science',
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
        mock_episode_data.__len__.return_value = 3

        mock_speaker_turn_data = MagicMock()
        mock_speaker_turn_data.__iter__ = lambda x: iter([])
        mock_speaker_turn_data.__len__.return_value = 0

        mock_load_dataset.side_effect = [mock_episode_data, mock_speaker_turn_data]

        dataset = SPORCDataset(streaming=True)

        # Load podcasts with at least 2 episodes
        dataset.load_podcast_subset(min_episodes=2)

        assert len(dataset._podcasts) == 1
        assert 'Large Podcast' in dataset._podcasts
        assert 'Small Podcast' not in dataset._podcasts
        assert len(dataset._episodes) == 2  # Both episodes from Large Podcast

    @patch('sporc.dataset.load_dataset')
    def test_selective_loading_by_duration(self, mock_load_dataset):
        """Test selective loading by total duration."""
        # Mock dataset with podcasts having different total durations
        mock_episode_data = MagicMock()
        mock_episode_data.__iter__ = lambda x: iter([
            # Podcast with 1 hour total duration
            {
                'podTitle': 'Short Podcast',
                'podDescription': 'A short podcast',
                'rssUrl': 'https://example.com/short.xml',
                'epTitle': 'Short Episode',
                'epDescription': 'Short episode',
                'mp3url': 'https://example.com/short.mp3',
                'durationSeconds': 3600.0,  # 1 hour
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
            # Podcast with 6 hours total duration (2 episodes of 3 hours each)
            {
                'podTitle': 'Long Podcast',
                'podDescription': 'A long podcast',
                'rssUrl': 'https://example.com/long.xml',
                'epTitle': 'Long Episode 1',
                'epDescription': 'First long episode',
                'mp3url': 'https://example.com/long1.mp3',
                'durationSeconds': 10800.0,  # 3 hours
                'transcript': 'Long transcript 1',
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
                'category1': 'science',
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
                'podTitle': 'Long Podcast',
                'podDescription': 'A long podcast',
                'rssUrl': 'https://example.com/long.xml',
                'epTitle': 'Long Episode 2',
                'epDescription': 'Second long episode',
                'mp3url': 'https://example.com/long2.mp3',
                'durationSeconds': 10800.0,  # 3 hours
                'transcript': 'Long transcript 2',
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
                'category1': 'science',
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
        mock_episode_data.__len__.return_value = 3

        mock_speaker_turn_data = MagicMock()
        mock_speaker_turn_data.__iter__ = lambda x: iter([])
        mock_speaker_turn_data.__len__.return_value = 0

        mock_load_dataset.side_effect = [mock_episode_data, mock_speaker_turn_data]

        dataset = SPORCDataset(streaming=True)

        # Load podcasts with at least 5 hours of content
        dataset.load_podcast_subset(min_total_duration=5.0)

        assert len(dataset._podcasts) == 1
        assert 'Long Podcast' in dataset._podcasts
        assert 'Short Podcast' not in dataset._podcasts
        assert len(dataset._episodes) == 2  # Both episodes from Long Podcast

    @patch('sporc.dataset.load_dataset')
    def test_selective_loading_complex_filtering(self, mock_load_dataset):
        """Test selective loading with complex filtering criteria."""
        # Mock dataset with various podcasts
        mock_episode_data = MagicMock()
        mock_episode_data.__iter__ = lambda x: iter([
            # English education podcast with 3 episodes
            {
                'podTitle': 'English Education Podcast',
                'podDescription': 'English education podcast',
                'rssUrl': 'https://example.com/eng_edu.xml',
                'epTitle': 'English Education Episode 1',
                'epDescription': 'First English education episode',
                'mp3url': 'https://example.com/eng_edu1.mp3',
                'durationSeconds': 3600.0,  # 1 hour
                'transcript': 'English education transcript',
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
                'podTitle': 'English Education Podcast',
                'podDescription': 'English education podcast',
                'rssUrl': 'https://example.com/eng_edu.xml',
                'epTitle': 'English Education Episode 2',
                'epDescription': 'Second English education episode',
                'mp3url': 'https://example.com/eng_edu2.mp3',
                'durationSeconds': 3600.0,  # 1 hour
                'transcript': 'English education transcript 2',
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
                'podTitle': 'English Education Podcast',
                'podDescription': 'English education podcast',
                'rssUrl': 'https://example.com/eng_edu.xml',
                'epTitle': 'English Education Episode 3',
                'epDescription': 'Third English education episode',
                'mp3url': 'https://example.com/eng_edu3.mp3',
                'durationSeconds': 3600.0,  # 1 hour
                'transcript': 'English education transcript 3',
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
            # Spanish science podcast (should be filtered out)
            {
                'podTitle': 'Spanish Science Podcast',
                'podDescription': 'Spanish science podcast',
                'rssUrl': 'https://example.com/spa_sci.xml',
                'epTitle': 'Spanish Science Episode',
                'epDescription': 'Spanish science episode',
                'mp3url': 'https://example.com/spa_sci.mp3',
                'durationSeconds': 1800.0,  # 30 minutes
                'transcript': 'Spanish science transcript',
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
                'language': 'es',
                'explicit': 0,
                'imageUrl': None,
                'episodeDateLocalized': None,
                'oldestEpisodeDate': None,
                'lastUpdate': None,
                'createdOn': None,
                'category1': 'science',
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
        mock_episode_data.__len__.return_value = 4

        mock_speaker_turn_data = MagicMock()
        mock_speaker_turn_data.__iter__ = lambda x: iter([])
        mock_speaker_turn_data.__len__.return_value = 0

        mock_load_dataset.side_effect = [mock_episode_data, mock_speaker_turn_data]

        dataset = SPORCDataset(streaming=True)

        # Load English education podcasts with at least 2 episodes and 2 hours of content
        dataset.load_podcast_subset(
            categories=['education'],
            min_episodes=2,
            min_total_duration=2.0,
            language='en'
        )

        assert len(dataset._podcasts) == 1
        assert 'English Education Podcast' in dataset._podcasts
        assert 'Spanish Science Podcast' not in dataset._podcasts
        assert len(dataset._episodes) == 3  # All 3 episodes from English Education Podcast

    @patch('sporc.dataset.load_dataset')
    def test_selective_loading_fast_access(self, mock_load_dataset):
        """Test that selective loading provides fast access to loaded subset."""
        # Mock dataset
        mock_episode_data = MagicMock()
        mock_episode_data.__iter__ = lambda x: iter([
            {
                'podTitle': 'Test Podcast',
                'podDescription': 'A test podcast',
                'rssUrl': 'https://example.com/test.xml',
                'epTitle': 'Test Episode',
                'epDescription': 'Test episode',
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

        # Load subset
        dataset.load_podcast_subset(categories=['education'])

        # Test fast access methods
        podcasts = dataset.get_all_podcasts()
        assert len(podcasts) == 1
        assert podcasts[0].title == 'Test Podcast'

        episodes = dataset.get_all_episodes()
        assert len(episodes) == 1
        assert episodes[0].title == 'Test Episode'

        # Test fast search
        search_results = dataset.search_episodes(category='education')
        assert len(search_results) == 1
        assert search_results[0].title == 'Test Episode'

        # Test podcast search
        podcast = dataset.search_podcast('Test Podcast')
        assert podcast.title == 'Test Podcast'

        # Test statistics
        stats = dataset.get_dataset_statistics()
        assert stats['total_podcasts'] == 1
        assert stats['total_episodes'] == 1

    @patch('sporc.dataset.load_dataset')
    def test_selective_loading_memory_mode_warning(self, mock_load_dataset):
        """Test that selective loading warns when used in memory mode."""
        # Mock dataset
        mock_episode_data = MagicMock()
        mock_episode_data.__iter__ = lambda x: iter([])
        mock_episode_data.__len__.return_value = 0
        mock_speaker_turn_data = MagicMock()
        mock_speaker_turn_data.__iter__ = lambda x: iter([])
        mock_speaker_turn_data.__len__.return_value = 0
        mock_load_dataset.side_effect = [mock_episode_data, mock_speaker_turn_data]

        dataset = SPORCDataset(streaming=False)  # Memory mode

        # This should not raise an error but should log a warning
        dataset.load_podcast_subset(categories=['education'])

        # The dataset should still be in memory mode
        assert dataset.streaming is False
        assert dataset._selective_mode is False


if __name__ == "__main__":
    pytest.main([__file__])