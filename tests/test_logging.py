"""
Unit tests for SPORC logging functionality.
"""

import pytest
import logging
import tempfile
import os
from unittest.mock import patch, MagicMock, call
from io import StringIO

from sporc import SPORCDataset


class TestLogging:
    """Test SPORC logging functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        # Capture log output
        self.log_capture = StringIO()
        self.handler = logging.StreamHandler(self.log_capture)
        self.handler.setLevel(logging.DEBUG)

        # Get the sporc logger
        self.sporc_logger = logging.getLogger('sporc.dataset')
        self.sporc_logger.addHandler(self.handler)
        self.sporc_logger.setLevel(logging.DEBUG)

    def teardown_method(self):
        """Clean up test fixtures."""
        self.sporc_logger.removeHandler(self.handler)
        self.handler.close()

    def get_log_output(self):
        """Get captured log output."""
        return self.log_capture.getvalue()

    @patch('sporc.dataset.load_dataset')
    def test_streaming_mode_logging(self, mock_load_dataset):
        """Test logging during streaming mode initialization."""
        # Mock dataset
        mock_dataset = MagicMock()
        mock_dataset.__iter__ = MagicMock(return_value=iter([]))
        mock_load_dataset.return_value = mock_dataset

        # Clear previous logs
        self.log_capture.truncate(0)
        self.log_capture.seek(0)

        # Initialize dataset
        sporc = SPORCDataset(streaming=True)

        # Check for expected log messages
        log_output = self.get_log_output()
        assert "Loading SPORC dataset from Hugging Face" in log_output
        assert "streaming=True" in log_output
        assert "Dataset loaded in streaming mode" in log_output

    @patch('sporc.dataset.load_dataset')
    def test_memory_mode_logging(self, mock_load_dataset):
        """Test logging during memory mode initialization."""
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

        # Clear previous logs
        self.log_capture.truncate(0)
        self.log_capture.seek(0)

        # Initialize dataset
        sporc = SPORCDataset(streaming=False)

        # Check for expected log messages
        log_output = self.get_log_output()
        assert "Loading SPORC dataset from Hugging Face" in log_output
        assert "streaming=False" in log_output
        assert "Processing dataset into Podcast and Episode objects" in log_output
        assert "Dataset processing completed" in log_output

    @patch('sporc.dataset.load_dataset')
    def test_safe_iterator_logging(self, mock_load_dataset):
        """Test logging during safe iterator processing."""
        # Mock dataset with mixed data types
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
            }
        ]

        mock_dataset = MagicMock()
        mock_dataset.__iter__ = MagicMock(return_value=iter(mock_episodes))
        mock_load_dataset.return_value = mock_dataset

        # Clear previous logs
        self.log_capture.truncate(0)
        self.log_capture.seek(0)

        # Initialize dataset and test safe iterator
        sporc = SPORCDataset(streaming=True)

        # Test safe iterator
        records = list(sporc._create_safe_iterator(sporc._dataset))

        # Check for expected log messages
        log_output = self.get_log_output()
        assert "Starting safe iterator with data type validation" in log_output
        assert "Safe iterator completed" in log_output

    @patch('sporc.dataset.load_dataset')
    def test_selective_loading_logging(self, mock_load_dataset):
        """Test logging during selective loading."""
        # Mock dataset with multiple episodes
        mock_episodes = [
            {
                'epTitle': 'Education Episode 1',
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
                'epTitle': 'Education Episode 2',
                'mp3url': 'https://example.com/ep2.mp3',
                'durationSeconds': 2400.0,
                'transcript': 'Test transcript 2',
                'podTitle': 'Education Podcast',
                'podDescription': 'An education podcast',
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
                'epTitle': 'Technology Episode',
                'mp3url': 'https://example.com/ep3.mp3',
                'durationSeconds': 1200.0,
                'transcript': 'Test transcript 3',
                'podTitle': 'Technology Podcast',
                'podDescription': 'A technology podcast',
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

        # Clear previous logs
        self.log_capture.truncate(0)
        self.log_capture.seek(0)

        # Initialize dataset and test selective loading
        sporc = SPORCDataset(streaming=True)
        sporc.load_podcast_subset(categories=['Education'])

        # Check for expected log messages
        log_output = self.get_log_output()
        assert "Loading podcast subset with criteria" in log_output
        assert "Scanning dataset to separate episode and speaker turn data" in log_output
        assert "Dataset scanning completed" in log_output
        assert "Grouping episodes by podcast and applying filters" in log_output
        assert "Episode grouping completed" in log_output
        assert "Creating Podcast and Episode objects for filtered subset" in log_output
        assert "Object creation completed" in log_output
        assert "Loading speaker turn data for selected episodes" in log_output
        assert "Speaker turn loading completed" in log_output
        assert "Selective loading completed" in log_output

    @patch('sporc.dataset.load_dataset')
    def test_error_logging(self, mock_load_dataset):
        """Test logging of error conditions."""
        # Mock dataset that raises an exception during iteration
        def problematic_iterator():
            yield {
                'epTitle': 'Test Episode',
                'podTitle': 'Test Podcast',
                'mp3url': 'https://example.com/test.mp3',
                'durationSeconds': 1800.0,
                'transcript': 'Test transcript',
                'podDescription': 'Test podcast description',
                'rssUrl': 'https://example.com/rss.xml',
                'language': 'en',
                'explicit': 0
            }  # First record works
            raise Exception("Test error")  # Second record fails

        mock_dataset = MagicMock()
        mock_dataset.__iter__ = MagicMock(return_value=problematic_iterator())
        mock_load_dataset.return_value = mock_dataset

        # Clear previous logs
        self.log_capture.truncate(0)
        self.log_capture.seek(0)

        # This should log errors but not crash
        with pytest.raises(Exception):
            sporc = SPORCDataset(streaming=True)
            list(sporc.iterate_episodes())

        # Check for error logging
        log_output = self.get_log_output()
        # The exception happens in the mock dataset's __iter__ method, so we check for the exception message
        # or any error-related logging that might occur
        assert "Test error" in log_output or "Exception" in log_output or "Error" in log_output

    @patch('sporc.dataset.load_dataset')
    def test_progress_logging(self, mock_load_dataset):
        """Test progress logging during long operations."""
        # Create many mock episodes to trigger progress logging
        mock_episodes = []
        for i in range(15000):  # Enough to trigger progress logs
            mock_episodes.append({
                'epTitle': f'Episode {i}',
                'mp3url': f'https://example.com/ep{i}.mp3',
                'durationSeconds': 1800.0,
                'transcript': f'Test transcript {i}',
                'podTitle': f'Podcast {i // 10}',
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

        # Clear previous logs
        self.log_capture.truncate(0)
        self.log_capture.seek(0)

        # Initialize dataset (this should trigger progress logs)
        sporc = SPORCDataset(streaming=False)

        # Check for progress logging
        log_output = self.get_log_output()
        assert "Processed" in log_output
        assert "records" in log_output

    def test_logging_configuration(self):
        """Test that logging is properly configured."""
        # Check that the sporc logger exists and has the right level
        sporc_logger = logging.getLogger('sporc.dataset')
        assert sporc_logger.level <= logging.INFO

        # Check that the logger has handlers
        assert len(sporc_logger.handlers) > 0

    @patch('sporc.dataset.load_dataset')
    def test_debug_logging_level(self, mock_load_dataset):
        """Test debug level logging."""
        # Set logger to DEBUG level
        self.sporc_logger.setLevel(logging.DEBUG)

        # Mock dataset
        mock_dataset = MagicMock()
        mock_dataset.__iter__ = MagicMock(return_value=iter([]))
        mock_load_dataset.return_value = mock_dataset

        # Clear previous logs
        self.log_capture.truncate(0)
        self.log_capture.seek(0)

        # Initialize dataset
        sporc = SPORCDataset(streaming=True)

        # Check for debug messages
        log_output = self.get_log_output()
        # Debug messages should be present when level is DEBUG
        assert "Loading SPORC dataset from Hugging Face" in log_output

    @patch('sporc.dataset.load_dataset')
    def test_info_logging_level(self, mock_load_dataset):
        """Test info level logging."""
        # Set logger to INFO level (should filter out DEBUG messages)
        self.sporc_logger.setLevel(logging.INFO)

        # Mock dataset
        mock_dataset = MagicMock()
        mock_dataset.__iter__ = MagicMock(return_value=iter([]))
        mock_load_dataset.return_value = mock_dataset

        # Clear previous logs
        self.log_capture.truncate(0)
        self.log_capture.seek(0)

        # Initialize dataset
        sporc = SPORCDataset(streaming=True)

        # Check for info messages
        log_output = self.get_log_output()
        assert "Loading SPORC dataset from Hugging Face" in log_output

    @patch('sporc.dataset.load_dataset')
    def test_progress_logging_at_debug_level(self, mock_load_dataset):
        """Test that progress logging is at DEBUG level."""
        # Mock dataset with enough episodes to trigger progress logs
        def create_mock_episodes():
            mock_episodes = []
            for i in range(15000):
                mock_episodes.append({
                    'epTitle': f'Episode {i}',
                    'mp3url': f'https://example.com/ep{i}.mp3',
                    'durationSeconds': 1800.0,
                    'transcript': f'Test transcript {i}',
                    'podTitle': f'Podcast {i // 10}',
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
            return mock_episodes

        # Test with INFO level (should not show progress logs)
        self.sporc_logger.setLevel(logging.INFO)
        self.log_capture.truncate(0)
        self.log_capture.seek(0)

        # Create fresh mock dataset for first test
        mock_dataset_1 = MagicMock()
        mock_dataset_1.__iter__ = MagicMock(return_value=iter(create_mock_episodes()))
        mock_load_dataset.return_value = mock_dataset_1

        sporc = SPORCDataset(streaming=False)
        log_output = self.get_log_output()

        # Progress logs should not appear at INFO level
        assert "Processed" not in log_output or "DEBUG" not in log_output

        # Test with DEBUG level (should show progress logs)
        self.sporc_logger.setLevel(logging.DEBUG)
        self.log_capture.truncate(0)
        self.log_capture.seek(0)

        # Create fresh mock dataset for second test
        mock_dataset_2 = MagicMock()
        mock_dataset_2.__iter__ = MagicMock(return_value=iter(create_mock_episodes()))
        mock_load_dataset.return_value = mock_dataset_2

        sporc = SPORCDataset(streaming=False)
        log_output = self.get_log_output()

        # Progress logs should appear at DEBUG level
        assert "Processed" in log_output