"""
Unit tests for SPORC dataset local files functionality.
"""

import pytest
import tempfile
import os
import gzip
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

from sporc import SPORCDataset, DatasetAccessError


class TestLocalFiles:
    """Test SPORC dataset loading from local JSONL.gz files."""

    def create_sample_jsonl_gz(self, filename, data):
        """Create a sample JSONL.gz file with test data."""
        filepath = os.path.join(self.temp_dir, filename)
        with gzip.open(filepath, 'wt', encoding='utf-8') as f:
            for item in data:
                f.write(json.dumps(item) + '\n')
        return filepath

    def setup_method(self):
        """Set up test fixtures."""
        import logging
        logging.getLogger("sporc.dataset").setLevel(logging.DEBUG)
        self.temp_dir = tempfile.mkdtemp()

        # Create sample episode data
        self.sample_episodes = [
            {
                'epTitle': 'Episode 1',
                'epDescription': 'First episode',
                'mp3url': 'https://example.com/ep1.mp3',
                'durationSeconds': 1800.0,
                'transcript': 'This is episode 1 transcript.',
                'podTitle': 'Test Podcast 1',
                'podDescription': 'A test podcast',
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
                'epDescription': 'Second episode',
                'mp3url': 'https://example.com/ep2.mp3',
                'durationSeconds': 2400.0,
                'transcript': 'This is episode 2 transcript.',
                'podTitle': 'Test Podcast 1',
                'podDescription': 'A test podcast',
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
                'epDescription': 'Third episode',
                'mp3url': 'https://example.com/ep3.mp3',
                'durationSeconds': 1200.0,
                'transcript': 'This is episode 3 transcript.',
                'podTitle': 'Test Podcast 2',
                'podDescription': 'Another test podcast',
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

        # Create sample speaker turn data
        self.sample_turns = [
            {
                'mp3url': 'https://example.com/ep1.mp3',
                'turnText': 'Hello, welcome to the show.',
                'speaker': 'SPEAKER_00',
                'startTime': 0.0,
                'endTime': 3.0,
                'duration': 3.0,
                'wordCount': 6
            },
            {
                'mp3url': 'https://example.com/ep1.mp3',
                'turnText': 'Thanks for having me.',
                'speaker': 'SPEAKER_01',
                'startTime': 3.5,
                'endTime': 5.0,
                'duration': 1.5,
                'wordCount': 4
            }
        ]

    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_required_files(self):
        """Create all required SPORC files for testing."""
        self.create_sample_jsonl_gz('episodeLevelData.jsonl.gz', self.sample_episodes)
        self.create_sample_jsonl_gz('episodeLevelDataSample.jsonl.gz', self.sample_episodes[:1])
        self.create_sample_jsonl_gz('speakerTurnData.jsonl.gz', self.sample_turns)
        self.create_sample_jsonl_gz('speakerTurnDataSample.jsonl.gz', self.sample_turns[:1])

    def test_local_files_validation_missing_directory(self):
        """Test validation when directory doesn't exist."""
        with pytest.raises(DatasetAccessError, match="Local data directory does not exist"):
            SPORCDataset(local_data_dir="/nonexistent/directory")

    def test_local_files_validation_not_directory(self):
        """Test validation when path is not a directory."""
        # Create a file instead of directory
        file_path = os.path.join(self.temp_dir, "not_a_directory")
        with open(file_path, 'w') as f:
            f.write("test")

        with pytest.raises(DatasetAccessError, match="Local data path is not a directory"):
            SPORCDataset(local_data_dir=file_path)

    def test_local_files_validation_missing_files(self):
        """Test validation when required files are missing."""
        with pytest.raises(DatasetAccessError, match="Missing required files"):
            SPORCDataset(local_data_dir=self.temp_dir)

    def test_local_files_validation_partial_files(self):
        """Test validation when only some required files are present."""
        # Create only some files
        self.create_sample_jsonl_gz('episodeLevelData.jsonl.gz', self.sample_episodes)

        with pytest.raises(DatasetAccessError, match="Missing required files"):
            SPORCDataset(local_data_dir=self.temp_dir)

    def test_local_files_memory_mode(self):
        """Test loading local files in memory mode."""
        self.create_required_files()

        sporc = SPORCDataset(local_data_dir=self.temp_dir, streaming=False)

        assert sporc is not None
        assert sporc._local_mode is True
        assert sporc._loaded is True

        # Check that data was loaded
        stats = sporc.get_dataset_statistics()
        assert stats['total_podcasts'] == 2
        assert stats['total_episodes'] == 3
        assert stats['total_duration_hours'] > 0

    def test_local_files_streaming_mode(self):
        """Test loading local files in streaming mode."""
        self.create_required_files()

        sporc = SPORCDataset(local_data_dir=self.temp_dir, streaming=True)

        assert sporc is not None
        assert sporc._local_mode is True
        assert sporc.streaming is True

        # Test episode iteration
        episode_count = 0
        for episode in sporc.iterate_episodes():
            episode_count += 1
            if episode_count >= 3:
                break

        assert episode_count == 3

    def test_local_files_podcast_iteration(self):
        """Test podcast iteration with local files."""
        self.create_required_files()

        sporc = SPORCDataset(local_data_dir=self.temp_dir, streaming=True)

        podcasts = list(sporc.iterate_podcasts())
        assert len(podcasts) == 2

        # Check podcast titles
        podcast_titles = [p.title for p in podcasts]
        assert 'Test Podcast 1' in podcast_titles
        assert 'Test Podcast 2' in podcast_titles

        # Check episode counts
        podcast1 = next(p for p in podcasts if p.title == 'Test Podcast 1')
        podcast2 = next(p for p in podcasts if p.title == 'Test Podcast 2')
        assert len(podcast1.episodes) == 2
        assert len(podcast2.episodes) == 1

    def test_local_files_selective_loading(self):
        """Test selective loading with local files."""
        self.create_required_files()

        sporc = SPORCDataset(local_data_dir=self.temp_dir, streaming=True)
        sporc.load_podcast_subset(categories=['Education'])

        assert sporc._selective_mode is True
        assert len(sporc._podcasts) == 1
        assert 'Test Podcast 1' in sporc._podcasts

        stats = sporc.get_dataset_statistics()
        assert stats['total_podcasts'] == 1
        assert stats['total_episodes'] == 2

    def test_local_files_search_functionality(self):
        """Test search functionality with local files."""
        self.create_required_files()

        sporc = SPORCDataset(local_data_dir=self.temp_dir, streaming=True)

        # Search for episodes by duration
        episodes = sporc.search_episodes(min_duration=2000)  # 33+ minutes
        assert len(episodes) == 1
        assert episodes[0].title == 'Episode 2'

        # Search for episodes by category
        episodes = sporc.search_episodes_by_subcategory('Technology')
        assert len(episodes) == 1
        assert episodes[0].title == 'Episode 3'

    def test_local_files_podcast_search(self):
        """Test podcast search with local files."""
        self.create_required_files()

        sporc = SPORCDataset(local_data_dir=self.temp_dir, streaming=True)

        # Search for specific podcast
        podcast = sporc.search_podcast("Test Podcast 1")
        assert podcast.title == "Test Podcast 1"
        assert len(podcast.episodes) == 2

        # Search for podcasts by subcategory
        podcasts = sporc.search_podcasts_by_subcategory('Education')
        assert len(podcasts) == 1
        assert podcasts[0].title == "Test Podcast 1"

    def test_local_files_with_invalid_json(self):
        """Test handling of invalid JSON in local files."""
        # Create file with invalid JSON
        filepath = os.path.join(self.temp_dir, 'episodeLevelData.jsonl.gz')
        with gzip.open(filepath, 'wt', encoding='utf-8') as f:
            f.write('{"valid": "json"}\n')
            f.write('invalid json line\n')
            f.write('{"another": "valid"}\n')

        # Create other required files
        self.create_sample_jsonl_gz('episodeLevelDataSample.jsonl.gz', self.sample_episodes[:1])
        self.create_sample_jsonl_gz('speakerTurnData.jsonl.gz', self.sample_turns)
        self.create_sample_jsonl_gz('speakerTurnDataSample.jsonl.gz', self.sample_turns[:1])

        # Should still load successfully, skipping invalid lines
        sporc = SPORCDataset(local_data_dir=self.temp_dir, streaming=True)
        assert sporc is not None

    def test_local_files_string_representation(self):
        """Test string representation of local dataset."""
        self.create_required_files()

        sporc = SPORCDataset(local_data_dir=self.temp_dir, streaming=True)

        str_repr = str(sporc)
        assert "local" in str_repr
        assert "streaming" in str_repr

        repr_repr = repr(sporc)
        assert "local" in repr_repr
        assert "streaming" in repr_repr