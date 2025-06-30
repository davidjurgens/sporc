"""
Unit tests for the LocalSPORCDataset class.
"""

import pytest
import tempfile
import os
import json
import gzip
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, mock_open

from sporc.dataset import LocalSPORCDataset


class TestLocalSPORCDataset:
    """Test the LocalSPORCDataset class."""

    def test_init_streaming_mode(self):
        """Test initialization in streaming mode."""
        file_paths = {'test': '/path/to/test.jsonl.gz'}
        dataset = LocalSPORCDataset(file_paths, streaming=True)
        assert dataset.file_paths == file_paths
        assert dataset.streaming is True
        assert dataset._all_records is None

    def test_init_memory_mode(self):
        """Test initialization in memory mode."""
        file_paths = {'test': '/path/to/test.jsonl.gz'}
        with patch.object(LocalSPORCDataset, '_load_all_records') as mock_load:
            dataset = LocalSPORCDataset(file_paths, streaming=False)
            assert dataset.file_paths == file_paths
            assert dataset.streaming is False
            mock_load.assert_called_once()

    def test_load_all_records_success(self):
        """Test successful loading of all records."""
        # Create temporary test data
        test_data = [
            {'id': 1, 'title': 'Test 1'},
            {'id': 2, 'title': 'Test 2'},
            {'id': 3, 'title': 'Test 3'}
        ]

        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl.gz', delete=False) as f:
            with gzip.open(f.name, 'wt') as gz:
                for record in test_data:
                    gz.write(json.dumps(record) + '\n')

            file_paths = {'test': f.name}

            try:
                with patch('sporc.dataset.logger') as mock_logger:
                    dataset = LocalSPORCDataset(file_paths, streaming=False)
                    assert len(dataset._all_records) == 3
                    assert dataset._all_records[0]['id'] == 1
                    assert dataset._all_records[1]['title'] == 'Test 2'
            finally:
                os.unlink(f.name)

    def test_load_all_records_with_invalid_json(self):
        """Test loading with invalid JSON lines."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl.gz', delete=False) as f:
            with gzip.open(f.name, 'wt') as gz:
                gz.write('{"id": 1, "title": "Valid"}\n')
                gz.write('invalid json line\n')
                gz.write('{"id": 2, "title": "Also Valid"}\n')

            file_paths = {'test': f.name}

            try:
                with patch('sporc.dataset.logger') as mock_logger:
                    dataset = LocalSPORCDataset(file_paths, streaming=False)
                    assert len(dataset._all_records) == 2
                    assert dataset._all_records[0]['id'] == 1
                    assert dataset._all_records[1]['id'] == 2
                    # Check that warning was logged
                    mock_logger.warning.assert_called()
            finally:
                os.unlink(f.name)

    def test_load_all_records_file_error(self):
        """Test handling of file reading errors."""
        file_paths = {'test': '/nonexistent/file.jsonl.gz'}

        with pytest.raises(Exception):
            LocalSPORCDataset(file_paths, streaming=False)

    def test_iter_streaming_mode(self):
        """Test iteration in streaming mode."""
        test_data = [
            {'id': 1, 'title': 'Test 1'},
            {'id': 2, 'title': 'Test 2'}
        ]

        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl.gz', delete=False) as f:
            with gzip.open(f.name, 'wt') as gz:
                for record in test_data:
                    gz.write(json.dumps(record) + '\n')

            file_paths = {'test': f.name}

            try:
                dataset = LocalSPORCDataset(file_paths, streaming=True)
                records = list(dataset)
                assert len(records) == 2
                assert records[0]['id'] == 1
                assert records[1]['title'] == 'Test 2'
            finally:
                os.unlink(f.name)

    def test_iter_memory_mode(self):
        """Test iteration in memory mode."""
        file_paths = {'test': '/path/to/test.jsonl.gz'}
        with patch.object(LocalSPORCDataset, '_load_all_records'):
            dataset = LocalSPORCDataset(file_paths, streaming=False)
            dataset._all_records = [{'id': 1}, {'id': 2}]

            records = list(dataset)
            assert len(records) == 2
            assert records[0]['id'] == 1
            assert records[1]['id'] == 2

    def test_len_streaming_mode(self):
        """Test length calculation in streaming mode."""
        test_data = [
            {'id': 1, 'title': 'Test 1'},
            {'id': 2, 'title': 'Test 2'},
            {'id': 3, 'title': 'Test 3'}
        ]

        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl.gz', delete=False) as f:
            with gzip.open(f.name, 'wt') as gz:
                for record in test_data:
                    gz.write(json.dumps(record) + '\n')

            file_paths = {'test': f.name}

            try:
                dataset = LocalSPORCDataset(file_paths, streaming=True)
                assert len(dataset) == 3
            finally:
                os.unlink(f.name)

    def test_len_memory_mode(self):
        """Test length calculation in memory mode."""
        file_paths = {'test': '/path/to/test.jsonl.gz'}
        with patch.object(LocalSPORCDataset, '_load_all_records'):
            dataset = LocalSPORCDataset(file_paths, streaming=False)
            dataset._all_records = [{'id': 1}, {'id': 2}, {'id': 3}]

            assert len(dataset) == 3

    def test_getitem_streaming_mode(self):
        """Test indexing in streaming mode raises error."""
        file_paths = {'test': '/path/to/test.jsonl.gz'}
        dataset = LocalSPORCDataset(file_paths, streaming=True)

        with pytest.raises(RuntimeError, match="Indexing is not supported in streaming mode"):
            dataset[0]

    def test_getitem_memory_mode_not_loaded(self):
        """Test indexing in memory mode when records not loaded."""
        file_paths = {'test': '/path/to/test.jsonl.gz'}
        with patch.object(LocalSPORCDataset, '_load_all_records'):
            dataset = LocalSPORCDataset(file_paths, streaming=False)
            dataset._all_records = None

            with pytest.raises(RuntimeError, match="Records not loaded into memory"):
                dataset[0]

    def test_getitem_memory_mode_success(self):
        """Test successful indexing in memory mode."""
        file_paths = {'test': '/path/to/test.jsonl.gz'}
        with patch.object(LocalSPORCDataset, '_load_all_records'):
            dataset = LocalSPORCDataset(file_paths, streaming=False)
            dataset._all_records = [{'id': 1}, {'id': 2}]

            assert dataset[0]['id'] == 1
            assert dataset[1]['id'] == 2