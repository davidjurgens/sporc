"""
Unit tests for SPORC streaming functionality.
"""

import pytest
import time
from unittest.mock import patch, MagicMock

from sporc import SPORCDataset


class TestStreamingFunctionality:
    """Test SPORC streaming mode functionality."""

    @patch('sporc.dataset.load_dataset')
    def test_streaming_initialization(self, mock_load_dataset):
        """Test streaming mode initialization."""
        # Mock dataset
        mock_dataset = MagicMock()
        mock_dataset.__iter__ = MagicMock(return_value=iter([]))
        mock_load_dataset.return_value = mock_dataset

        start_time = time.time()
        sporc = SPORCDataset(streaming=True)
        init_time = time.time() - start_time

        assert sporc is not None
        assert sporc.streaming is True
        assert init_time < 5.0  # Should be fast

    @patch('sporc.dataset.load_dataset')
    def test_episode_iteration(self, mock_load_dataset):
        """Test episode iteration in streaming mode."""
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
        start_time = time.time()

        for episode in sporc.iterate_episodes():
            episode_count += 1
            if episode_count >= 2:
                break

            # Verify episode data
            assert episode.title is not None, f"Episode {episode_count} has no title"
            assert episode.podcast_title is not None, f"Episode {episode_count} has no podcast title"

            if episode_count == 1:
                assert episode.title == 'Test Episode 1'
                assert episode.podcast_title == 'Test Podcast'
                assert episode.duration_seconds == 1800.0

        end_time = time.time()
        assert episode_count == 2
        assert end_time - start_time < 5.0  # Should be fast

    @patch('sporc.dataset.load_dataset')
    def test_podcast_iteration(self, mock_load_dataset):
        """Test podcast iteration in streaming mode."""
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

        # Check episode counts
        podcast1 = next(p for p in podcasts if p.title == 'Test Podcast 1')
        podcast2 = next(p for p in podcasts if p.title == 'Test Podcast 2')
        assert len(podcast1.episodes) == 2
        assert len(podcast2.episodes) == 1

    @patch('sporc.dataset.load_dataset')
    def test_podcast_search(self, mock_load_dataset):
        """Test podcast search in streaming mode."""
        # Create mock episode data
        mock_episodes = [
            {
                'epTitle': 'Episode 1',
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
                'epTitle': 'Episode 2',
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

        # Test podcast search
        podcast = sporc.search_podcast("Test Podcast")
        assert podcast.title == "Test Podcast"
        assert len(podcast.episodes) == 2

    @patch('sporc.dataset.load_dataset')
    def test_episode_search(self, mock_load_dataset):
        """Test episode search in streaming mode."""
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

        sporc = SPORCDataset(streaming=True)

        # Test search by duration
        episodes = sporc.search_episodes(min_duration=1800)  # 30+ minutes
        assert len(episodes) == 1
        assert episodes[0].title == 'Long Episode'

    @patch('sporc.dataset.load_dataset')
    def test_dataset_statistics(self, mock_load_dataset):
        """Test dataset statistics in streaming mode."""
        # Create mock episode data
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
                'podTitle': 'Test Podcast 2',
                'podDescription': 'Second test podcast',
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

        # Test statistics
        stats = sporc.get_dataset_statistics()
        assert stats['total_podcasts'] == 2
        assert stats['total_episodes'] == 2
        assert stats['total_duration_hours'] > 0
        assert 'Education' in stats['category_distribution']
        assert 'Technology' in stats['category_distribution']

    @patch('sporc.dataset.load_dataset')
    def test_streaming_vs_memory_comparison(self, mock_load_dataset):
        """Compare streaming mode with memory mode performance."""
        # Create mock episode data
        mock_episodes = []
        for i in range(100):  # Create 100 episodes for testing
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

        # Test streaming mode
        start_time = time.time()
        dataset_streaming = SPORCDataset(streaming=True)
        streaming_init_time = time.time() - start_time

        episode_count = 0
        start_time = time.time()
        for episode in dataset_streaming.iterate_episodes():
            episode_count += 1
            if episode_count >= 50:  # Test first 50 episodes
                break
        streaming_iter_time = time.time() - start_time

        # Test memory mode
        start_time = time.time()
        dataset_memory = SPORCDataset(streaming=False)
        memory_init_time = time.time() - start_time

        episode_count = 0
        start_time = time.time()
        for episode in dataset_memory._episodes[:50]:  # First 50 episodes
            episode_count += 1
        memory_iter_time = time.time() - start_time

        # Verify performance characteristics
        assert streaming_init_time < memory_init_time  # Streaming should initialize faster
        assert memory_iter_time < streaming_iter_time  # Memory should iterate faster
        assert episode_count == 50

    @patch('sporc.dataset.load_dataset')
    def test_safe_iterator_data_type_handling(self, mock_load_dataset):
        """Test safe iterator handling of data type inconsistencies."""
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
    def test_streaming_mode_limitations(self, mock_load_dataset):
        """Test streaming mode limitations."""
        # Mock dataset
        mock_dataset = MagicMock()
        mock_dataset.__iter__ = MagicMock(return_value=iter([]))
        mock_load_dataset.return_value = mock_dataset

        sporc = SPORCDataset(streaming=True)

        # Test that len() is not available in streaming mode
        with pytest.raises(RuntimeError, match="len\\(\\) is not available in streaming mode"):
            len(sporc)

        # Test that get_all_podcasts() is not available without selective loading
        with pytest.raises(RuntimeError, match="get_all_podcasts\\(\\) is not available in streaming mode"):
            sporc.get_all_podcasts()

        # Test that get_all_episodes() is not available without selective loading
        with pytest.raises(RuntimeError, match="get_all_episodes\\(\\) is not available in streaming mode"):
            sporc.get_all_episodes()

    @patch('sporc.dataset.load_dataset')
    def test_streaming_mode_with_selective_loading(self, mock_load_dataset):
        """Test streaming mode with selective loading."""
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

        # After selective loading, these methods should work
        assert sporc._selective_mode is True
        assert len(sporc._podcasts) == 1
        assert 'Education Podcast' in sporc._podcasts

        # Test that len() now works
        assert len(sporc) == 1134058

        # Test that get_all_podcasts() now works
        podcasts = sporc.get_all_podcasts()
        assert len(podcasts) == 1
        assert podcasts[0].title == 'Education Podcast'

        # Test that get_all_episodes() now works
        episodes = sporc.get_all_episodes()
        assert len(episodes) == 1
        assert episodes[0].title == 'Education Episode'

    @patch('sporc.dataset.load_dataset')
    def test_streaming_mode_with_selective_loading_correct_len(self, mock_load_dataset):
        """Test streaming mode with selective loading."""
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

        # After selective loading, these methods should work
        assert sporc._selective_mode is True
        assert len(sporc._podcasts) == 1
        assert 'Education Podcast' in sporc._podcasts

        # Test that len() returns the correct value in streaming mode
        assert len(sporc) == 1134058

        # Test that get_all_podcasts() now works
        podcasts = sporc.get_all_podcasts()
        assert len(podcasts) == 1
        assert podcasts[0].title == 'Education Podcast'

        # Test that get_all_episodes() now works
        episodes = sporc.get_all_episodes()
        assert len(episodes) == 1
        assert episodes[0].title == 'Education Episode'