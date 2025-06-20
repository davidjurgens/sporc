"""
Unit tests for SPORC quick functionality testing.
"""

import pytest
import time
from unittest.mock import patch, MagicMock

from sporc import SPORCDataset


class TestQuickFunctionality:
    """Test core SPORC functionality quickly."""

    @patch('sporc.dataset.load_dataset')
    def test_import_functionality(self, mock_load_dataset):
        """Test that SPORC can be imported successfully."""
        # Mock the dataset loading to avoid actual downloads
        mock_dataset = MagicMock()
        mock_load_dataset.return_value = mock_dataset

        from sporc import SPORCDataset
        assert SPORCDataset is not None

    @patch('sporc.dataset.load_dataset')
    def test_streaming_mode_initialization(self, mock_load_dataset):
        """Test streaming mode initialization."""
        # Mock the dataset loading
        mock_dataset = MagicMock()
        mock_load_dataset.return_value = mock_dataset

        start_time = time.time()
        sporc = SPORCDataset(streaming=True)
        load_time = time.time() - start_time

        assert sporc is not None
        assert sporc.streaming is True
        assert load_time < 5.0  # Should be fast

    @patch('sporc.dataset.load_dataset')
    def test_memory_mode_initialization(self, mock_load_dataset):
        """Test memory mode initialization."""
        # Mock the dataset loading
        mock_dataset = MagicMock()
        mock_load_dataset.return_value = mock_dataset

        start_time = time.time()
        sporc = SPORCDataset(streaming=False)
        load_time = time.time() - start_time

        assert sporc is not None
        assert sporc.streaming is False
        assert load_time < 5.0  # Should be fast

    @patch('sporc.dataset.load_dataset')
    def test_episode_iteration(self, mock_load_dataset):
        """Test episode iteration functionality."""
        # Create mock episode data
        mock_episodes = [
            {
                'epTitle': 'Test Episode 1',
                'epDescription': 'First test episode',
                'mp3url': 'https://example.com/ep1.mp3',
                'durationSeconds': 1800.0,
                'transcript': 'Test transcript 1',
                'podTitle': 'Test Podcast',
                'podDescription': 'A test podcast',
                'rssUrl': 'https://example.com/feed.xml',
                'category1': 'Education',
                'language': 'en',
                'explicit': 0,
                'hostPredictedNames': '["Host 1"]',
                'guestPredictedNames': '["Guest 1"]',
                'mainEpSpeakers': '["SPEAKER_00", "SPEAKER_01"]',
                'overlapPropDuration': 0.1,
                'overlapPropTurnCount': 0.05,
                'avgTurnDuration': 15.0,
                'totalSpLabels': 120.0
            },
            {
                'epTitle': 'Test Episode 2',
                'epDescription': 'Second test episode',
                'mp3url': 'https://example.com/ep2.mp3',
                'durationSeconds': 2400.0,
                'transcript': 'Test transcript 2',
                'podTitle': 'Test Podcast',
                'podDescription': 'A test podcast',
                'rssUrl': 'https://example.com/feed.xml',
                'category1': 'Education',
                'language': 'en',
                'explicit': 0,
                'hostPredictedNames': '["Host 1"]',
                'guestPredictedNames': '["Guest 2"]',
                'mainEpSpeakers': '["SPEAKER_00", "SPEAKER_02"]',
                'overlapPropDuration': 0.15,
                'overlapPropTurnCount': 0.08,
                'avgTurnDuration': 18.0,
                'totalSpLabels': 160.0
            }
        ]

        mock_dataset = MagicMock()
        mock_dataset.__iter__ = MagicMock(return_value=iter(mock_episodes))
        mock_load_dataset.return_value = mock_dataset

        sporc = SPORCDataset(streaming=True)

        # Test episode iteration
        episode_count = 0
        for episode in sporc.iterate_episodes():
            episode_count += 1
            if episode_count >= 2:
                break

        assert episode_count == 2

    @patch('sporc.dataset.load_dataset')
    def test_podcast_iteration(self, mock_load_dataset):
        """Test podcast iteration functionality."""
        # Create mock episode data with different podcasts
        mock_episodes = [
            {
                'epTitle': 'Episode 1',
                'mp3url': 'https://example.com/ep1.mp3',
                'durationSeconds': 1800.0,
                'transcript': 'Test transcript 1',
                'podTitle': 'Test Podcast 1',
                'podDescription': 'First test podcast',
                'rssUrl': 'https://example.com/feed1.xml',
                'category1': 'Education',
                'language': 'en',
                'explicit': 0,
                'hostPredictedNames': '["Host 1"]',
                'guestPredictedNames': '["Guest 1"]',
                'mainEpSpeakers': '["SPEAKER_00", "SPEAKER_01"]',
                'overlapPropDuration': 0.1,
                'overlapPropTurnCount': 0.05,
                'avgTurnDuration': 15.0,
                'totalSpLabels': 120.0
            },
            {
                'epTitle': 'Episode 2',
                'mp3url': 'https://example.com/ep2.mp3',
                'durationSeconds': 2400.0,
                'transcript': 'Test transcript 2',
                'podTitle': 'Test Podcast 1',
                'podDescription': 'First test podcast',
                'rssUrl': 'https://example.com/feed1.xml',
                'category1': 'Education',
                'language': 'en',
                'explicit': 0,
                'hostPredictedNames': '["Host 1"]',
                'guestPredictedNames': '["Guest 2"]',
                'mainEpSpeakers': '["SPEAKER_00", "SPEAKER_02"]',
                'overlapPropDuration': 0.15,
                'overlapPropTurnCount': 0.08,
                'avgTurnDuration': 18.0,
                'totalSpLabels': 160.0
            },
            {
                'epTitle': 'Episode 3',
                'mp3url': 'https://example.com/ep3.mp3',
                'durationSeconds': 1200.0,
                'transcript': 'Test transcript 3',
                'podTitle': 'Test Podcast 2',
                'podDescription': 'Second test podcast',
                'rssUrl': 'https://example.com/feed2.xml',
                'category1': 'Technology',
                'language': 'en',
                'explicit': 0,
                'hostPredictedNames': '["Host 2"]',
                'guestPredictedNames': '[]',
                'mainEpSpeakers': '["SPEAKER_10"]',
                'overlapPropDuration': 0.0,
                'overlapPropTurnCount': 0.0,
                'avgTurnDuration': 20.0,
                'totalSpLabels': 60.0
            }
        ]

        mock_dataset = MagicMock()
        mock_dataset.__iter__ = MagicMock(return_value=iter(mock_episodes))
        mock_load_dataset.return_value = mock_dataset

        sporc = SPORCDataset(streaming=True)

        # Test podcast iteration
        podcasts = list(sporc.iterate_podcasts())
        assert len(podcasts) == 2

        # Check podcast titles
        podcast_titles = [p.title for p in podcasts]
        assert 'Test Podcast 1' in podcast_titles
        assert 'Test Podcast 2' in podcast_titles

    @patch('sporc.dataset.load_dataset')
    def test_search_functionality(self, mock_load_dataset):
        """Test search functionality."""
        # Create mock episode data
        mock_episodes = [
            {
                'epTitle': 'Short Episode',
                'mp3url': 'https://example.com/ep1.mp3',
                'durationSeconds': 900.0,  # 15 minutes
                'transcript': 'Test transcript 1',
                'podTitle': 'Test Podcast',
                'podDescription': 'A test podcast',
                'rssUrl': 'https://example.com/feed.xml',
                'category1': 'Education',
                'language': 'en',
                'explicit': 0,
                'hostPredictedNames': '["Host 1"]',
                'guestPredictedNames': '["Guest 1"]',
                'mainEpSpeakers': '["SPEAKER_00", "SPEAKER_01"]',
                'overlapPropDuration': 0.1,
                'overlapPropTurnCount': 0.05,
                'avgTurnDuration': 15.0,
                'totalSpLabels': 120.0
            },
            {
                'epTitle': 'Long Episode',
                'mp3url': 'https://example.com/ep2.mp3',
                'durationSeconds': 3600.0,  # 60 minutes
                'transcript': 'Test transcript 2',
                'podTitle': 'Test Podcast',
                'podDescription': 'A test podcast',
                'rssUrl': 'https://example.com/feed.xml',
                'category1': 'Education',
                'language': 'en',
                'explicit': 0,
                'hostPredictedNames': '["Host 1"]',
                'guestPredictedNames': '["Guest 2"]',
                'mainEpSpeakers': '["SPEAKER_00", "SPEAKER_02"]',
                'overlapPropDuration': 0.15,
                'overlapPropTurnCount': 0.08,
                'avgTurnDuration': 18.0,
                'totalSpLabels': 160.0
            }
        ]

        mock_dataset = MagicMock()
        mock_dataset.__iter__ = MagicMock(return_value=iter(mock_episodes))
        mock_load_dataset.return_value = mock_dataset

        sporc = SPORCDataset(streaming=False)

        # Test search by duration
        episodes = sporc.search_episodes(min_duration=1800)  # 30+ minutes
        assert len(episodes) == 1
        assert episodes[0].title == 'Long Episode'

    @patch('sporc.dataset.load_dataset')
    def test_selective_loading(self, mock_load_dataset):
        """Test selective loading functionality."""
        # Create mock episode data
        mock_episodes = [
            {
                'epTitle': 'Education Episode',
                'mp3url': 'https://example.com/ep1.mp3',
                'durationSeconds': 1800.0,
                'transcript': 'Test transcript 1',
                'podTitle': 'Education Podcast',
                'podDescription': 'An education podcast',
                'rssUrl': 'https://example.com/feed1.xml',
                'category1': 'Education',
                'language': 'en',
                'explicit': 0,
                'hostPredictedNames': '["Host 1"]',
                'guestPredictedNames': '["Guest 1"]',
                'mainEpSpeakers': '["SPEAKER_00", "SPEAKER_01"]',
                'overlapPropDuration': 0.1,
                'overlapPropTurnCount': 0.05,
                'avgTurnDuration': 15.0,
                'totalSpLabels': 120.0
            },
            {
                'epTitle': 'Technology Episode',
                'mp3url': 'https://example.com/ep2.mp3',
                'durationSeconds': 2400.0,
                'transcript': 'Test transcript 2',
                'podTitle': 'Technology Podcast',
                'podDescription': 'A technology podcast',
                'rssUrl': 'https://example.com/feed2.xml',
                'category1': 'Technology',
                'language': 'en',
                'explicit': 0,
                'hostPredictedNames': '["Host 2"]',
                'guestPredictedNames': '["Guest 2"]',
                'mainEpSpeakers': '["SPEAKER_10", "SPEAKER_11"]',
                'overlapPropDuration': 0.15,
                'overlapPropTurnCount': 0.08,
                'avgTurnDuration': 18.0,
                'totalSpLabels': 160.0
            }
        ]

        mock_dataset = MagicMock()
        mock_dataset.__iter__ = MagicMock(return_value=iter(mock_episodes))
        mock_load_dataset.return_value = mock_dataset

        sporc = SPORCDataset(streaming=True)
        sporc.load_podcast_subset(categories=['Education'])

        assert sporc._selective_mode is True
        assert len(sporc._podcasts) == 1
        assert 'Education Podcast' in sporc._podcasts

    def test_error_handling(self):
        """Test error handling for common issues."""
        # Test with invalid dataset ID
        with pytest.raises(Exception):
            # This should fail since we're not mocking the dataset loading
            SPORCDataset(dataset_id="invalid/dataset")

    @patch('sporc.dataset.load_dataset')
    def test_performance_benchmarks(self, mock_load_dataset):
        """Test performance benchmarks for core operations."""
        # Mock dataset
        mock_dataset = MagicMock()
        mock_dataset.__iter__ = MagicMock(return_value=iter([]))
        mock_load_dataset.return_value = mock_dataset

        # Test initialization performance
        start_time = time.time()
        sporc = SPORCDataset(streaming=True)
        init_time = time.time() - start_time

        assert init_time < 1.0  # Should be very fast with mocked data

        # Test iteration performance (with empty dataset)
        start_time = time.time()
        episodes = list(sporc.iterate_episodes())
        iteration_time = time.time() - start_time

        assert iteration_time < 1.0  # Should be very fast with empty dataset
        assert len(episodes) == 0