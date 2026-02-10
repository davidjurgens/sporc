"""
Unit tests for the SPORC CLI module.
"""

import pytest
import sys
from unittest.mock import patch, MagicMock
from io import StringIO

from sporc.cli import main, handle_stats, handle_search_podcast, handle_search_episodes
from sporc import SPORCDataset, SPORCError


class TestCLI:
    """Test the CLI functionality."""

    @patch('sporc.cli.handle_stats')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_stats_command(self, mock_parse_args, mock_handle_stats):
        """Test main function with stats command."""
        mock_args = MagicMock()
        mock_args.command = 'stats'
        mock_parse_args.return_value = mock_args

        with patch.object(sys, 'argv', ['sporc', 'stats']):
            main()

        mock_handle_stats.assert_called_once_with(mock_args)

    @patch('sporc.cli.handle_search_podcast')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_search_podcast_command(self, mock_parse_args, mock_handle_search_podcast):
        """Test main function with search-podcast command."""
        mock_args = MagicMock()
        mock_args.command = 'search-podcast'
        mock_args.name = 'Test Podcast'
        mock_parse_args.return_value = mock_args

        with patch.object(sys, 'argv', ['sporc', 'search-podcast', 'Test Podcast']):
            main()

        mock_handle_search_podcast.assert_called_once_with(mock_args)

    @patch('sporc.cli.handle_search_episodes')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_search_episodes_command(self, mock_parse_args, mock_handle_search_episodes):
        """Test main function with search-episodes command."""
        mock_args = MagicMock()
        mock_args.command = 'search-episodes'
        mock_args.min_duration = 1800
        mock_args.max_duration = None
        mock_args.min_speakers = None
        mock_args.max_speakers = None
        mock_args.host_name = None
        mock_args.category = None
        mock_args.subcategory = None
        mock_args.limit = 10
        mock_parse_args.return_value = mock_args

        with patch.object(sys, 'argv', ['sporc', 'search-episodes', '--min-duration', '1800']):
            main()

        mock_handle_search_episodes.assert_called_once_with(mock_args)

    @patch('argparse.ArgumentParser.parse_args')
    def test_main_no_command(self, mock_parse_args):
        """Test main function with no command."""
        mock_args = MagicMock()
        mock_args.command = None
        mock_parse_args.return_value = mock_args

        with patch.object(sys, 'argv', ['sporc']), \
             patch.object(sys, 'exit') as mock_exit:
            main()

        assert mock_exit.call_count >= 1
        mock_exit.assert_any_call(1)

    @patch('argparse.ArgumentParser.parse_args')
    def test_main_unknown_command(self, mock_parse_args):
        """Test main function with unknown command."""
        mock_args = MagicMock()
        mock_args.command = 'unknown'
        mock_parse_args.return_value = mock_args

        with patch.object(sys, 'argv', ['sporc', 'unknown']), \
             patch.object(sys, 'exit') as mock_exit:
            main()

        mock_exit.assert_called_once_with(1)

    @patch('argparse.ArgumentParser.parse_args')
    def test_main_sporc_error(self, mock_parse_args):
        """Test main function with SPORC error."""
        mock_args = MagicMock()
        mock_args.command = 'stats'
        mock_parse_args.return_value = mock_args

        with patch('sporc.cli.handle_stats', side_effect=SPORCError("Test error")), \
             patch.object(sys, 'argv', ['sporc', 'stats']), \
             patch.object(sys, 'exit') as mock_exit, \
             patch('builtins.print') as mock_print:
            main()

        mock_exit.assert_called_once_with(1)
        mock_print.assert_any_call("SPORC Error: Test error")

    @patch('argparse.ArgumentParser.parse_args')
    def test_main_keyboard_interrupt(self, mock_parse_args):
        """Test main function with keyboard interrupt."""
        mock_args = MagicMock()
        mock_args.command = 'stats'
        mock_parse_args.return_value = mock_args

        with patch('sporc.cli.handle_stats', side_effect=KeyboardInterrupt()), \
             patch.object(sys, 'argv', ['sporc', 'stats']), \
             patch.object(sys, 'exit') as mock_exit, \
             patch('builtins.print') as mock_print:
            main()

        mock_exit.assert_called_once_with(1)
        mock_print.assert_any_call("\nOperation cancelled by user")

    @patch('argparse.ArgumentParser.parse_args')
    def test_main_unexpected_error(self, mock_parse_args):
        """Test main function with unexpected error."""
        mock_args = MagicMock()
        mock_args.command = 'stats'
        mock_parse_args.return_value = mock_args

        with patch('sporc.cli.handle_stats', side_effect=Exception("Unexpected error")), \
             patch.object(sys, 'argv', ['sporc', 'stats']), \
             patch.object(sys, 'exit') as mock_exit, \
             patch('builtins.print') as mock_print:
            main()

        mock_exit.assert_called_once_with(1)
        mock_print.assert_any_call("Unexpected error: Unexpected error")

    @patch('sporc.cli.SPORCDataset')
    def test_handle_stats_basic(self, mock_sporc_class):
        """Test handle_stats with basic arguments."""
        mock_sporc = MagicMock()
        mock_stats = {
            'total_podcasts': 1000,
            'total_episodes': 50000,
            'total_duration_hours': 25000.0,
            'avg_episode_duration_minutes': 30.0,
            'category_distribution': {'Education': 1000, 'Technology': 800},
            'language_distribution': {'en': 45000, 'es': 5000},
            'speaker_distribution': {'1': 1000, '2': 2000}
        }
        mock_sporc.get_dataset_statistics.return_value = mock_stats
        mock_sporc_class.return_value = mock_sporc

        args = MagicMock()
        args.parquet_dir = None

        with patch('builtins.print') as mock_print:
            handle_stats(args)

        mock_sporc_class.assert_called_once_with(parquet_dir=None)
        mock_sporc.get_dataset_statistics.assert_called_once()

    @patch('sporc.cli.SPORCDataset')
    def test_handle_stats_with_parquet_dir(self, mock_sporc_class):
        """Test handle_stats with parquet_dir argument."""
        mock_sporc = MagicMock()
        mock_stats = {
            'total_podcasts': 100,
            'total_episodes': 5000,
            'total_duration_hours': 2500.0,
            'avg_episode_duration_minutes': 30.0,
            'category_distribution': {'Education': 100},
            'language_distribution': {'en': 5000},
            'speaker_distribution': {'1': 100, '2': 200}
        }
        mock_sporc.get_dataset_statistics.return_value = mock_stats
        mock_sporc_class.return_value = mock_sporc

        args = MagicMock()
        args.parquet_dir = '/data/parquet'

        with patch('builtins.print') as mock_print:
            handle_stats(args)

        mock_sporc_class.assert_called_once_with(parquet_dir='/data/parquet')

    @patch('sporc.cli.SPORCDataset')
    def test_handle_stats_with_episode_types(self, mock_sporc_class):
        """Test handle_stats with episode types in statistics."""
        mock_sporc = MagicMock()
        mock_stats = {
            'total_podcasts': 1000,
            'total_episodes': 50000,
            'total_duration_hours': 25000.0,
            'avg_episode_duration_minutes': 30.0,
            'category_distribution': {'Education': 1000},
            'language_distribution': {'en': 45000},
            'speaker_distribution': {'1': 1000},
            'episode_types': {'full': 40000, 'trailer': 10000}
        }
        mock_sporc.get_dataset_statistics.return_value = mock_stats
        mock_sporc_class.return_value = mock_sporc

        args = MagicMock()
        args.parquet_dir = None

        with patch('builtins.print') as mock_print:
            handle_stats(args)

        mock_print.assert_any_call("\nEpisode types:")
        mock_print.assert_any_call("  full: 40000 episodes")
        mock_print.assert_any_call("  trailer: 10000 episodes")

    @patch('sporc.cli.SPORCDataset')
    def test_handle_search_podcast_found(self, mock_sporc_class):
        """Test handle_search_podcast when podcast is found."""
        mock_sporc = MagicMock()
        mock_podcast = MagicMock()
        mock_podcast.title = "Test Podcast"
        mock_podcast.description = "A test podcast"
        mock_podcast.num_episodes = 100
        mock_podcast.total_duration_hours = 50.0
        mock_podcast.host_names = ["John Doe", "Jane Smith"]
        mock_podcast.categories = ["Education", "Technology"]

        mock_episode1 = MagicMock()
        mock_episode1.title = "Episode 1"
        mock_episode1.duration_minutes = 45.0
        mock_episode1.num_main_speakers = 2

        mock_episode2 = MagicMock()
        mock_episode2.title = "Episode 2"
        mock_episode2.duration_minutes = 30.0
        mock_episode2.num_main_speakers = 1

        mock_episode3 = MagicMock()
        mock_episode3.title = "Episode 3"
        mock_episode3.duration_minutes = 60.0
        mock_episode3.num_main_speakers = 3

        mock_podcast.episodes = [mock_episode1, mock_episode2, mock_episode3]
        mock_sporc.search_podcast.return_value = mock_podcast
        mock_sporc_class.return_value = mock_sporc

        args = MagicMock()
        args.name = "Test Podcast"
        args.parquet_dir = None

        with patch('builtins.print') as mock_print:
            handle_search_podcast(args)

        mock_sporc_class.assert_called_once_with(parquet_dir=None)
        mock_sporc.search_podcast.assert_called_once_with("Test Podcast")

    @patch('sporc.cli.SPORCDataset')
    def test_handle_search_podcast_with_parquet_dir(self, mock_sporc_class):
        """Test handle_search_podcast with parquet_dir."""
        mock_sporc = MagicMock()
        mock_podcast = MagicMock()
        mock_podcast.title = "Test Podcast"
        mock_podcast.description = "A test podcast"
        mock_podcast.num_episodes = 100
        mock_podcast.total_duration_hours = 50.0
        mock_podcast.host_names = ["John Doe"]
        mock_podcast.categories = ["Education"]

        mock_episode = MagicMock()
        mock_episode.title = "Test Episode"
        mock_episode.duration_minutes = 45.0
        mock_episode.num_main_speakers = 2

        mock_podcast.episodes = [mock_episode]
        mock_sporc.search_podcast.return_value = mock_podcast
        mock_sporc_class.return_value = mock_sporc

        args = MagicMock()
        args.name = "Test Podcast"
        args.parquet_dir = "/data/parquet"

        with patch('builtins.print') as mock_print:
            handle_search_podcast(args)

        mock_sporc_class.assert_called_once_with(parquet_dir="/data/parquet")

    @patch('sporc.cli.SPORCDataset')
    def test_handle_search_episodes_basic(self, mock_sporc_class):
        """Test handle_search_episodes with basic criteria."""
        mock_sporc = MagicMock()
        mock_episode1 = MagicMock()
        mock_episode1.title = "Episode 1"
        mock_episode1.podcast_title = "Test Podcast"
        mock_episode1.duration_minutes = 45.0
        mock_episode1.host_names = ["John Doe"]
        mock_episode1.categories = ["Education"]
        mock_episode1.num_main_speakers = 2

        mock_episode2 = MagicMock()
        mock_episode2.title = "Episode 2"
        mock_episode2.podcast_title = "Test Podcast"
        mock_episode2.duration_minutes = 30.0
        mock_episode2.host_names = ["Jane Smith"]
        mock_episode2.categories = ["Technology"]
        mock_episode2.num_main_speakers = 1

        mock_sporc.search_episodes.return_value = [mock_episode1, mock_episode2]
        mock_sporc_class.return_value = mock_sporc

        args = MagicMock()
        args.min_duration = 1800
        args.max_duration = None
        args.min_speakers = None
        args.max_speakers = None
        args.host_name = None
        args.category = None
        args.subcategory = None
        args.limit = 10
        args.parquet_dir = None

        with patch('builtins.print') as mock_print:
            handle_search_episodes(args)

        mock_sporc_class.assert_called_once_with(parquet_dir=None)
        mock_sporc.search_episodes.assert_called_once_with(min_duration=1800)

    @patch('sporc.cli.SPORCDataset')
    def test_handle_search_episodes_complex_criteria(self, mock_sporc_class):
        """Test handle_search_episodes with complex criteria."""
        mock_sporc = MagicMock()
        mock_episode = MagicMock()
        mock_episode.title = "Test Episode"
        mock_episode.podcast_title = "Test Podcast"
        mock_episode.duration_minutes = 60.0
        mock_episode.host_names = ["John Doe"]
        mock_episode.categories = ["Education"]
        mock_episode.num_main_speakers = 2
        mock_sporc.search_episodes.return_value = [mock_episode]
        mock_sporc_class.return_value = mock_sporc

        args = MagicMock()
        args.min_duration = 1800
        args.max_duration = 7200
        args.min_speakers = 2
        args.max_speakers = 5
        args.host_name = "John Doe"
        args.category = "Education"
        args.subcategory = "Language Learning"
        args.limit = 5
        args.parquet_dir = "/data/parquet"

        with patch('builtins.print') as mock_print:
            handle_search_episodes(args)

        mock_sporc_class.assert_called_once_with(parquet_dir="/data/parquet")
        mock_sporc.search_episodes.assert_called_once_with(
            min_duration=1800,
            max_duration=7200,
            min_speakers=2,
            max_speakers=5,
            host_name="John Doe",
            category="Education",
            subcategory="Language Learning"
        )

    @patch('sporc.cli.SPORCDataset')
    def test_handle_search_episodes_no_results(self, mock_sporc_class):
        """Test handle_search_episodes when no results are found."""
        mock_sporc = MagicMock()
        mock_sporc.search_episodes.return_value = []
        mock_sporc_class.return_value = mock_sporc

        args = MagicMock()
        args.min_duration = 1800
        args.max_duration = None
        args.min_speakers = None
        args.max_speakers = None
        args.host_name = None
        args.category = None
        args.subcategory = None
        args.limit = 10
        args.parquet_dir = None

        with patch('builtins.print') as mock_print:
            handle_search_episodes(args)

        mock_print.assert_any_call("\nFound 0 episodes matching criteria: {'min_duration': 1800}")
