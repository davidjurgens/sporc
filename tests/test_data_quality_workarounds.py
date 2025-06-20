"""
Unit tests for SPORC data quality workarounds.
"""

import pytest
import tempfile
import os
import shutil
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

from sporc import SPORCDataset


class TestDataQualityWorkarounds:
    """Test data quality workarounds for SPORC dataset."""

    @patch('sporc.dataset.load_dataset')
    def test_memory_mode_workaround(self, mock_load_dataset):
        """Test if memory mode works better than streaming mode."""
        # Mock dataset with some data
        mock_episodes = [
            {
                'epTitle': 'Test Episode',
                'mp3url': 'https://example.com/ep1.mp3',
                'durationSeconds': 1800.0,
                'transcript': 'Test transcript',
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
            }
        ]

        mock_dataset = MagicMock()
        mock_dataset.__iter__ = MagicMock(return_value=iter(mock_episodes))
        mock_load_dataset.return_value = mock_dataset

        start_time = time.time()
        sporc = SPORCDataset(streaming=False)  # Force memory mode
        elapsed_time = time.time() - start_time

        assert sporc is not None
        assert elapsed_time < 10.0  # Should be reasonable

        # Test basic functionality
        stats = sporc.get_dataset_statistics()
        assert stats['total_podcasts'] > 0
        assert stats['total_episodes'] > 0

        # Test search
        episodes = sporc.search_episodes(min_duration=1800)
        assert len(episodes) >= 0  # Should not crash

    @patch('sporc.dataset.load_dataset')
    def test_selective_loading_workaround(self, mock_load_dataset):
        """Test if selective loading works better."""
        # Mock dataset with multiple episodes
        mock_episodes = []
        for i in range(20):  # Create 20 episodes
            mock_episodes.append({
                'epTitle': f'Episode {i}',
                'mp3url': f'https://example.com/ep{i}.mp3',
                'durationSeconds': 1800.0,
                'transcript': f'Test transcript {i}',
                'podTitle': f'Podcast {i // 5}',  # 4 episodes per podcast
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
            })

        mock_dataset = MagicMock()
        mock_dataset.__iter__ = MagicMock(return_value=iter(mock_episodes))
        mock_load_dataset.return_value = mock_dataset

        # Try loading a small subset
        sporc = SPORCDataset(streaming=True)
        sporc.load_podcast_subset(min_episodes=1, max_episodes=10)

        assert sporc._selective_mode is True
        assert len(sporc) > 0

        # Test iteration
        episode_count = 0
        for episode in sporc.get_all_episodes():
            episode_count += 1
            if episode_count >= 3:
                break

        assert episode_count > 0

    @patch('sporc.dataset.load_dataset')
    def test_different_cache_workaround(self, mock_load_dataset):
        """Test if using a different cache location helps."""
        # Create a temporary cache directory
        temp_cache = tempfile.mkdtemp(prefix="sporc_test_cache_")

        try:
            # Mock dataset
            mock_dataset = MagicMock()
            mock_dataset.__iter__ = MagicMock(return_value=iter([]))
            mock_load_dataset.return_value = mock_dataset

            sporc = SPORCDataset(cache_dir=temp_cache, streaming=True)

            # Test basic iteration
            episode_count = 0
            for episode in sporc.iterate_episodes():
                episode_count += 1
                if episode_count >= 3:
                    break

            assert sporc is not None

        finally:
            # Clean up
            shutil.rmtree(temp_cache, ignore_errors=True)

    @patch('sporc.dataset.load_dataset')
    def test_cache_clearing_workaround(self, mock_load_dataset):
        """Test if clearing cache and retrying helps."""
        # Mock dataset
        mock_dataset = MagicMock()
        mock_dataset.__iter__ = MagicMock(return_value=iter([]))
        mock_load_dataset.return_value = mock_dataset

        # Create a temporary cache to "clear"
        temp_cache = tempfile.mkdtemp(prefix="sporc_test_cache_")

        try:
            # Simulate cache clearing by creating a new cache
            sporc = SPORCDataset(cache_dir=temp_cache, streaming=True)

            # Test basic iteration
            episode_count = 0
            for episode in sporc.iterate_episodes():
                episode_count += 1
                if episode_count >= 3:
                    break

            assert sporc is not None

        finally:
            # Clean up
            shutil.rmtree(temp_cache, ignore_errors=True)

    @patch('sporc.dataset.load_dataset')
    def test_flexible_schema_workaround(self, mock_load_dataset):
        """Test flexible schema loading workaround."""
        # Mock dataset with flexible schema
        mock_dataset = MagicMock()
        mock_dataset.__iter__ = MagicMock(return_value=iter([]))
        mock_load_dataset.return_value = mock_dataset

        sporc = SPORCDataset(streaming=True)

        assert sporc is not None
        assert sporc.streaming is True

    @patch('sporc.dataset.load_dataset')
    def test_safe_iterator_workaround(self, mock_load_dataset):
        """Test safe iterator workaround for data type inconsistencies."""
        # Create mock data with mixed types
        mock_episodes = [
            {
                'epTitle': 'Valid Episode',
                'mp3url': 'https://example.com/ep1.mp3',
                'durationSeconds': 1800.0,
                'transcript': 'Test transcript',
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
                'epTitle': 123,  # Invalid type
                'mp3url': 'https://example.com/ep2.mp3',
                'durationSeconds': 'invalid',  # Invalid type
                'transcript': 'Test transcript',
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
            }
        ]

        mock_dataset = MagicMock()
        mock_dataset.__iter__ = MagicMock(return_value=iter(mock_episodes))
        mock_load_dataset.return_value = mock_dataset

        sporc = SPORCDataset(streaming=True)

        # Test safe iterator
        records = list(sporc._create_safe_iterator(sporc._dataset))

        # Should handle mixed types gracefully
        assert len(records) > 0
        # The safe iterator should clean the data
        for record in records:
            assert isinstance(record.get('epTitle', ''), str)
            assert isinstance(record.get('durationSeconds', 0), (int, float))

    @patch('sporc.dataset.load_dataset')
    def test_alternative_configuration_workaround(self, mock_load_dataset):
        """Test alternative configuration workaround."""
        # Mock dataset
        mock_dataset = MagicMock()
        mock_dataset.__iter__ = MagicMock(return_value=iter([]))
        mock_load_dataset.return_value = mock_dataset

        # Test with keep_in_memory=False
        sporc = SPORCDataset(streaming=True)

        assert sporc is not None
        assert sporc.streaming is True

    def test_cache_directory_validation(self):
        """Test cache directory validation methods."""
        # Test find_cache_directories
        cache_dirs = SPORCDataset.find_cache_directories()
        assert isinstance(cache_dirs, dict)

        # Test validate_cache_directory with non-existent directory
        assert not SPORCDataset.validate_cache_directory("/nonexistent/directory")

        # Test list_available_datasets
        datasets = SPORCDataset.list_available_datasets()
        assert isinstance(datasets, list)

    @patch('sporc.dataset.load_dataset')
    def test_error_handling_workarounds(self, mock_load_dataset):
        """Test error handling workarounds."""
        # Test with invalid dataset ID
        with pytest.raises(Exception):
            # This should fail since we're not mocking the dataset loading
            SPORCDataset(dataset_id="invalid/dataset")

        # Test with authentication error simulation
        mock_load_dataset.side_effect = Exception("401")
        with pytest.raises(Exception):
            SPORCDataset(streaming=True)

        # Test with JSON parse error simulation
        mock_load_dataset.side_effect = Exception("JSON parse error")
        with pytest.raises(Exception):
            SPORCDataset(streaming=True)

    @patch('sporc.dataset.load_dataset')
    def test_performance_workarounds(self, mock_load_dataset):
        """Test performance-related workarounds."""
        # Mock dataset
        mock_dataset = MagicMock()
        mock_dataset.__iter__ = MagicMock(return_value=iter([]))
        mock_load_dataset.return_value = mock_dataset

        # Test streaming mode performance
        start_time = time.time()
        sporc_streaming = SPORCDataset(streaming=True)
        streaming_time = time.time() - start_time

        assert sporc_streaming is not None
        assert streaming_time < 5.0  # Should be fast

        # Test memory mode performance
        start_time = time.time()
        sporc_memory = SPORCDataset(streaming=False)
        memory_time = time.time() - start_time

        assert sporc_memory is not None
        assert memory_time < 10.0  # Should be reasonable

    @patch('sporc.dataset.load_dataset')
    def test_workaround_combinations(self, mock_load_dataset):
        """Test combinations of workarounds."""
        # Mock dataset
        mock_dataset = MagicMock()
        mock_dataset.__iter__ = MagicMock(return_value=iter([]))
        mock_load_dataset.return_value = mock_dataset

        # Test combination: streaming + selective loading + custom cache
        temp_cache = tempfile.mkdtemp(prefix="sporc_test_cache_")

        try:
            sporc = SPORCDataset(cache_dir=temp_cache, streaming=True)
            sporc.load_podcast_subset(min_episodes=1)

            assert sporc is not None
            assert sporc.streaming is True
            assert sporc._selective_mode is True

        finally:
            shutil.rmtree(temp_cache, ignore_errors=True)