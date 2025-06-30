"""
Main dataset class for working with the SPORC dataset.
"""

import json
import logging
from typing import List, Dict, Any, Optional, Union, Iterator
from pathlib import Path
import warnings
import os
import time
import gzip
from tqdm import tqdm
import random
import threading
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from datasets import load_dataset, Dataset, IterableDataset
    from huggingface_hub import HfApi
except ImportError as e:
    raise ImportError(
        "The 'datasets' and 'huggingface_hub' packages are required. "
        "Please install them with: pip install datasets huggingface_hub"
    ) from e

from .podcast import Podcast
from .episode import Episode
from .exceptions import (
    SPORCError,
    DatasetAccessError,
    AuthenticationError,
    NotFoundError
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LocalSPORCDataset:
    """
    A dataset wrapper for local JSONL.gz files that mimics the Hugging Face dataset interface.

    This class provides an iterator interface that reads from local JSONL.gz files
    and yields records one at a time, supporting both streaming and memory modes.
    """

    def __init__(self, file_paths: Dict[str, str], streaming: bool = True):
        """
        Initialize the local dataset.

        Args:
            file_paths: Dictionary mapping file type to file path
            streaming: If True, reads files on-demand. If False, loads all data into memory.
        """
        self.file_paths = file_paths
        self.streaming = streaming
        self._all_records = None

        if not streaming:
            self._load_all_records()

    def _load_all_records(self):
        """Load all records from all files into memory."""
        logger.info("Loading all records from local files into memory...")

        all_records = []
        total_files = len(self.file_paths)

        for i, (file_type, file_path) in enumerate(self.file_paths.items()):
            logger.info(f"Loading {file_type}...")

            try:
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        try:
                            record = json.loads(line.strip())
                            all_records.append(record)

                            if line_num % 10000 == 0:
                                logger.debug(f"  Loaded {line_num:,} records from {file_type}")

                        except json.JSONDecodeError as e:
                            logger.warning(f"Skipping invalid JSON on line {line_num} in {file_type}: {e}")
                            continue

            except Exception as e:
                logger.error(f"Error reading {file_type}: {e}")
                raise

        self._all_records = all_records
        logger.info(f"✓ Loaded {len(all_records):,} total records from {total_files} files")

    def __iter__(self):
        """Iterate over all records from all files."""
        if self.streaming:
            return self._stream_records()
        else:
            return iter(self._all_records)

    def _stream_records(self):
        """Stream records from all files."""
        for file_type, file_path in self.file_paths.items():
            logger.debug(f"Streaming from {file_type}: {file_path}")

            try:
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        try:
                            record = json.loads(line.strip())
                            yield record

                        except json.JSONDecodeError as e:
                            logger.warning(f"Skipping invalid JSON on line {line_num} in {file_type}: {e}")
                            continue

            except Exception as e:
                logger.error(f"Error reading {file_type}: {e}")
                raise

    def __len__(self):
        """Get the total number of records."""
        if self.streaming:
            # Count records by reading through files
            total_count = 0
            for file_type, file_path in self.file_paths.items():
                try:
                    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                        for line in f:
                            if line.strip():  # Skip empty lines
                                total_count += 1
                except Exception as e:
                    logger.error(f"Error counting records in {file_type}: {e}")
                    raise
            return total_count
        else:
            return len(self._all_records)

    def __getitem__(self, index):
        """Get a record by index (only available in memory mode)."""
        if self.streaming:
            raise RuntimeError("Indexing is not supported in streaming mode")

        if self._all_records is None:
            raise RuntimeError("Records not loaded into memory")

        return self._all_records[index]


class SPORCDataset:
    """
    Main class for working with the SPORC (Structured Podcast Open Research Corpus) dataset.

    This class provides access to the SPORC dataset hosted on Hugging Face and offers
    various search and filtering capabilities for podcasts and episodes.

    The dataset can be loaded in three modes:
    - **Memory mode** (default): Loads all data into memory for fast access
    - **Streaming mode**: Loads data on-demand to reduce memory usage
    - **Selective mode**: Filters and loads specific podcasts into memory for O(1) operations

    The dataset can be loaded from:
    - **Hugging Face** (default): Downloads from Hugging Face Hub
    - **Local files**: Directly from JSONL.gz files in a specified directory
    """

    DATASET_ID = "blitt/SPoRC"
    EPISODE_SPLIT = "train"
    SPEAKER_TURN_SPLIT = "train"

    # Expected local file names
    LOCAL_FILES = {
        'episode_data': 'episodeLevelData.jsonl.gz',
        'episode_data_sample': 'episodeLevelDataSample.jsonl.gz',
        'speaker_turn_data': 'speakerTurnData.jsonl.gz',
        'speaker_turn_data_sample': 'speakerTurnDataSample.jsonl.gz'
    }

    def __init__(self, cache_dir: Optional[str] = None, use_auth_token: Optional[str] = None,
                 streaming: bool = False, custom_cache_dir: Optional[str] = None,
                 local_data_dir: Optional[str] = None, show_progress: bool = True,
                 load_turns_eagerly: bool = True):
        """
        Initialize the SPORC dataset.

        Args:
            cache_dir: Directory to cache the dataset. If None, uses Hugging Face default.
            use_auth_token: Hugging Face token for authentication. If None, uses cached credentials.
            streaming: If True, uses streaming mode for memory efficiency.
            custom_cache_dir: Specific directory where the dataset has already been downloaded.
                             This allows loading from a pre-existing cache location.
                             If provided, this takes precedence over cache_dir.
            local_data_dir: Directory containing local JSONL.gz files. If provided, loads from
                           local files instead of Hugging Face. Expected files:
                           - episodeLevelData.jsonl.gz
                           - episodeLevelDataSample.jsonl.gz
                           - speakerTurnData.jsonl.gz
                           - speakerTurnDataSample.jsonl.gz
            show_progress: Whether to show tqdm progress bars during loading (default: True)
            load_turns_eagerly: If True, loads all turn data during initialization (default: True).
                              If False, turn data is stored separately and loaded on-demand.
                              This reduces initial memory usage but requires explicit loading calls.
        """
        self.cache_dir = custom_cache_dir if custom_cache_dir else cache_dir
        self.use_auth_token = use_auth_token
        self.streaming = streaming
        self.custom_cache_dir = custom_cache_dir
        self.local_data_dir = local_data_dir
        self.show_progress = show_progress
        self.load_turns_eagerly = load_turns_eagerly

        # Data storage
        self._dataset = None
        self._podcasts: Dict[str, Podcast] = {}
        self._episodes: List[Episode] = []
        self._loaded = False
        self._selective_mode = False
        self._local_mode = local_data_dir is not None

        # Lazy loading storage (only used when load_turns_eagerly=False)
        self._turns_by_episode: Dict[str, List[Dict[str, Any]]] = {}
        self._turns_loaded = False

        # Indexing system for efficient turn loading
        self._turn_index: Dict[str, List[int]] = {}  # episode_url -> list of file offsets
        self._turn_file_path: Optional[str] = None
        self._index_built = False
        self._index_lock = threading.Lock()

        # Load the dataset
        self._load_dataset()

    def _validate_local_files(self) -> Dict[str, str]:
        """
        Validate that all required local files exist.

        Returns:
            Dictionary mapping file type to file path

        Raises:
            DatasetAccessError: If required files are missing
        """
        if not self.local_data_dir:
            return {}

        data_dir = Path(self.local_data_dir)
        if not data_dir.exists():
            raise DatasetAccessError(f"Local data directory does not exist: {self.local_data_dir}")

        if not data_dir.is_dir():
            raise DatasetAccessError(f"Local data path is not a directory: {self.local_data_dir}")

        file_paths = {}
        missing_files = []

        for file_type, filename in self.LOCAL_FILES.items():
            file_path = data_dir / filename
            if file_path.exists():
                file_paths[file_type] = str(file_path)
            else:
                missing_files.append(filename)

        if missing_files:
            raise DatasetAccessError(
                f"Missing required files in {self.local_data_dir}: {', '.join(missing_files)}\n"
                f"Expected files: {', '.join(self.LOCAL_FILES.values())}"
            )

        return file_paths

    def _load_local_dataset(self) -> None:
        """Load dataset from local JSONL.gz files."""
        start_time = time.time()

        try:
            logger.info(f"Loading SPORC dataset from local directory: {self.local_data_dir}")

            # Validate local files
            file_paths = self._validate_local_files()
            logger.info("✓ All required files found")

            # Create a custom dataset object that wraps the local files
            self._dataset = LocalSPORCDataset(
                file_paths=file_paths,
                streaming=self.streaming
            )

            total_loading_time = time.time() - start_time
            logger.info(f"✓ Local dataset loaded successfully in {total_loading_time:.2f} seconds")

            if self.streaming:
                logger.info("✓ Dataset loaded in streaming mode - data will be loaded on-demand")
                self._loaded = True
            else:
                logger.info(f"✓ Dataset loaded successfully with {len(self._dataset)} total records")

                # Process the data if not in streaming mode
                self._process_data()

        except Exception as e:
            total_time = time.time() - start_time
            logger.error(f"Failed to load local dataset after {total_time:.2f} seconds: {e}")
            raise DatasetAccessError(f"Failed to load local dataset: {e}") from e

    def _load_dataset_with_flexible_schema(self):
        """Load dataset with flexible schema to handle data type inconsistencies."""
        try:
            from datasets import Features, Value, Sequence

            # Define a flexible schema that can handle mixed data types
            flexible_features = Features({
                # Episode fields
                'epTitle': Value('string'),
                'epDescription': Value('string'),
                'mp3url': Value('string'),
                'durationSeconds': Value('float64'),
                'transcript': Value('string'),
                'podTitle': Value('string'),
                'podDescription': Value('string'),
                'rssUrl': Value('string'),
                'category1': Value('string'),
                'category2': Value('string'),
                'category3': Value('string'),
                'category4': Value('string'),
                'category5': Value('string'),
                'category6': Value('string'),
                'category7': Value('string'),
                'category8': Value('string'),
                'category9': Value('string'),
                'category10': Value('string'),
                # Handle mixed data types for these fields
                'hostPredictedNames': Value('string'),  # Will be converted to list later
                'guestPredictedNames': Value('string'),  # Will be converted to list later
                'neitherPredictedNames': Value('string'),  # Will be converted to list later
                'mainEpSpeakers': Value('string'),  # Will be converted to list later
                'hostSpeakerLabels': Value('string'),  # Will be converted to dict later
                'guestSpeakerLabels': Value('string'),  # Will be converted to dict later
                'overlapPropDuration': Value('float64'),
                'overlapPropTurnCount': Value('float64'),
                'avgTurnDuration': Value('float64'),
                'totalSpLabels': Value('float64'),
                'language': Value('string'),
                'explicit': Value('int64'),
                'imageUrl': Value('string'),
                'episodeDateLocalized': Value('string'),
                'oldestEpisodeDate': Value('string'),
                'lastUpdate': Value('string'),
                'createdOn': Value('string'),
                # Speaker turn fields (for when this is speaker turn data)
                'turnText': Value('string'),
                'speaker': Value('string'),
                'startTime': Value('float64'),
                'endTime': Value('float64'),
                'duration': Value('float64'),
                'wordCount': Value('int64'),
            })

            logger.info("Loading dataset with flexible schema to handle data type inconsistencies...")

            self._dataset = load_dataset(
                self.DATASET_ID,
                split=self.EPISODE_SPLIT,
                cache_dir=self.cache_dir,
                use_auth_token=self.use_auth_token,
                trust_remote_code=True,
                streaming=self.streaming,
                features=flexible_features
            )

            return True

        except Exception as e:
            logger.warning(f"Flexible schema loading failed: {e}")
            return False

    def _create_safe_iterator(self, dataset_iterator):
        """Create a safe iterator that handles data type inconsistencies gracefully."""
        import time
        start_time = time.time()
        processed_count = 0
        cleaned_count = 0
        skipped_count = 0
        last_log_time = time.time()

        logger.debug("Starting safe iterator with data type validation...")

        for record in dataset_iterator:
            processed_count += 1

            # Log progress every 1000 records or every 60 seconds
            current_time = time.time()
            if processed_count % 1000 == 0 or current_time - last_log_time > 60:
                elapsed = current_time - start_time
                rate = processed_count / elapsed if elapsed > 0 else 0
                logger.debug(f"Safe iterator: processed {processed_count:,} records (cleaned: {cleaned_count}, skipped: {skipped_count}, rate: {rate:.1f} rec/sec)")
                last_log_time = current_time

            try:
                # Validate and clean the record
                cleaned_record = self._clean_record(record)
                if cleaned_record is not None:
                    cleaned_count += 1
                    yield cleaned_record
                else:
                    skipped_count += 1
            except Exception as e:
                skipped_count += 1
                # Log the error but continue processing
                logger.debug(f"Skipping problematic record: {e}")
                continue

        total_time = time.time() - start_time
        logger.debug(f"Safe iterator completed: {processed_count:,} processed, {cleaned_count:,} cleaned, {skipped_count:,} skipped in {total_time:.2f}s")

    def _safe_float(self, value, default: float = 0.0) -> float:
        """
        Safely convert a value to float, handling None and other edge cases.

        Args:
            value: The value to convert
            default: Default value to return if conversion fails

        Returns:
            float: The converted value or default
        """
        if value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def _safe_string(self, value, default: str = '') -> str:
        """
        Safely convert a value to string, handling None and other edge cases.

        Args:
            value: The value to convert
            default: Default value to return if conversion fails

        Returns:
            str: The converted value or default
        """
        if value is None:
            return default
        try:
            return str(value).strip()
        except (ValueError, TypeError):
            return default

    def _safe_boolean(self, value, default: bool = False) -> bool:
        """
        Safely convert a value to boolean, handling None and other edge cases.

        Args:
            value: The value to convert
            default: Default value to return if conversion fails

        Returns:
            bool: The converted value or default
        """
        if value is None:
            return default
        try:
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                return bool(value)
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            return bool(value)
        except (ValueError, TypeError):
            return default

    def _safe_list(self, value, default: list = None) -> list:
        """
        Safely convert a value to list, handling None and other edge cases.

        Args:
            value: The value to convert
            default: Default value to return if conversion fails

        Returns:
            list: The converted value or default
        """
        if default is None:
            default = []
        if value is None:
            return default
        try:
            if isinstance(value, list):
                return value
            if isinstance(value, str):
                # Handle special string cases
                if value in ['NO_HOST_PREDICTED', 'NO_GUEST_PREDICTED', 'NO_NEITHER_IDENTIFIED', 'SPEAKER_DATA_UNAVAILABLE']:
                    return []
                # Try to parse as JSON if it looks like a list
                if value.startswith('[') and value.endswith(']'):
                    try:
                        import json
                        return json.loads(value)
                    except json.JSONDecodeError:
                        pass
                # If it's a single item, wrap it in a list
                return [value] if value.strip() else []
            if isinstance(value, (tuple, set)):
                return list(value)
            # For other types, try to convert to string and wrap in list
            return [str(value)]
        except (ValueError, TypeError):
            return default

    def _safe_dict(self, value, default: dict = None) -> dict:
        """
        Safely convert a value to dict, handling None and other edge cases.

        Args:
            value: The value to convert
            default: Default value to return if conversion fails

        Returns:
            dict: The converted value or default
        """
        if default is None:
            default = {}
        if value is None:
            return default
        try:
            if isinstance(value, dict):
                return value
            if isinstance(value, str):
                # Handle special string cases
                if value == 'SPEAKER_DATA_UNAVAILABLE':
                    return {}
                # Try to parse as JSON
                if value.startswith('{') and value.endswith('}'):
                    try:
                        import json
                        return json.loads(value)
                    except json.JSONDecodeError:
                        pass
                return {}
            return {}
        except (ValueError, TypeError):
            return default

    def _clean_record(self, record):
        """Clean and validate a record to handle data type inconsistencies."""
        try:
            # Track what fields need cleaning
            string_fields_cleaned = 0
            numeric_fields_cleaned = 0

            # Ensure all string fields are actually strings
            string_fields = [
                'epTitle', 'epDescription', 'mp3url', 'transcript', 'podTitle',
                'podDescription', 'rssUrl', 'category1', 'category2', 'category3',
                'category4', 'category5', 'category6', 'category7', 'category8',
                'category9', 'category10', 'hostPredictedNames', 'guestPredictedNames',
                'neitherPredictedNames', 'mainEpSpeakers', 'hostSpeakerLabels',
                'guestSpeakerLabels', 'language', 'imageUrl', 'episodeDateLocalized',
                'oldestEpisodeDate', 'lastUpdate', 'createdOn', 'turnText', 'speaker'
            ]

            for field in string_fields:
                if field in record and record[field] is not None:
                    if not isinstance(record[field], str):
                        record[field] = str(record[field])
                        string_fields_cleaned += 1

            # Ensure numeric fields are actually numeric
            numeric_fields = [
                'durationSeconds', 'overlapPropDuration', 'overlapPropTurnCount',
                'avgTurnDuration', 'totalSpLabels', 'explicit', 'startTime',
                'endTime', 'duration', 'wordCount'
            ]

            for field in numeric_fields:
                if field in record and record[field] is not None:
                    try:
                        record[field] = self._safe_float(record[field])
                    except (ValueError, TypeError):
                        record[field] = 0.0
                        numeric_fields_cleaned += 1

            # Log if significant cleaning was needed
            if string_fields_cleaned > 0 or numeric_fields_cleaned > 0:
                logger.debug(f"Cleaned record: {string_fields_cleaned} string fields, {numeric_fields_cleaned} numeric fields")

            return record

        except Exception as e:
            logger.debug(f"Failed to clean record: {e}")
            return None

    def _load_dataset(self) -> None:
        """Load the SPORC dataset from Hugging Face or local files."""
        if self._local_mode:
            self._load_local_dataset()
        else:
            self._load_huggingface_dataset()

    def _load_huggingface_dataset(self) -> None:
        """Load the SPORC dataset from Hugging Face."""
        start_time = time.time()

        try:
            logger.info(f"Loading SPORC dataset from Hugging Face (streaming={self.streaming})...")

            # Log cache directory information
            if self.custom_cache_dir:
                logger.info(f"Using custom cache directory: {self.custom_cache_dir}")
                if not os.path.exists(self.custom_cache_dir):
                    logger.warning(f"Custom cache directory does not exist: {self.custom_cache_dir}")
                else:
                    logger.info("✓ Custom cache directory found")
            elif self.cache_dir:
                logger.info(f"Using specified cache directory: {self.cache_dir}")
            else:
                logger.info("Using default Hugging Face cache directory")

            if not self.custom_cache_dir:
                logger.info("This may take several minutes on first run as the dataset needs to be downloaded.")

            # Try multiple loading strategies
            loading_successful = False
            strategy_start_time = time.time()

            # Strategy 1: Standard loading
            logger.info("Attempting standard loading...")

            try:
                strategy_1_start = time.time()
                self._dataset = load_dataset(
                    self.DATASET_ID,
                    split=self.EPISODE_SPLIT,
                    cache_dir=self.cache_dir,
                    use_auth_token=self.use_auth_token,
                    trust_remote_code=True,
                    streaming=self.streaming
                )
                strategy_1_time = time.time() - strategy_1_start
                loading_successful = True
                logger.info(f"✓ Dataset loaded successfully with standard method in {strategy_1_time:.2f} seconds")

            except Exception as e:
                strategy_1_time = time.time() - strategy_1_start
                logger.warning(f"Standard loading failed after {strategy_1_time:.2f} seconds")
                if "JSON parse error" in str(e) or "Column changed from" in str(e):
                    logger.info("Trying alternative loading methods...")
                else:
                    logger.error(f"Unexpected error in standard loading: {e}")
                    raise e

            # Strategy 2: Flexible schema loading
            if not loading_successful:
                logger.info("Attempting flexible schema loading...")
                strategy_2_start = time.time()
                if self._load_dataset_with_flexible_schema():
                    strategy_2_time = time.time() - strategy_2_start
                    loading_successful = True
                    logger.info(f"✓ Dataset loaded successfully with flexible schema in {strategy_2_time:.2f} seconds")
                else:
                    strategy_2_time = time.time() - strategy_2_start
                    logger.warning(f"Flexible schema loading failed after {strategy_2_time:.2f} seconds")

            # Strategy 3: Alternative configuration
            if not loading_successful:
                logger.info("Attempting alternative configuration...")
                try:
                    strategy_3_start = time.time()
                    self._dataset = load_dataset(
                        self.DATASET_ID,
                        split=self.EPISODE_SPLIT,
                        cache_dir=self.cache_dir,
                        use_auth_token=self.use_auth_token,
                        trust_remote_code=True,
                        streaming=self.streaming,
                        keep_in_memory=False
                    )
                    strategy_3_time = time.time() - strategy_3_start
                    loading_successful = True
                    logger.info(f"✓ Dataset loaded successfully with alternative configuration in {strategy_3_time:.2f} seconds")
                except Exception as e:
                    strategy_3_time = time.time() - strategy_3_start
                    logger.error(f"All loading strategies failed after {strategy_3_time:.2f} seconds")
                    logger.error(f"Final error: {e}")
                    raise e

            total_loading_time = time.time() - strategy_start_time
            logger.info(f"✓ Dataset loading completed in {total_loading_time:.2f} seconds")

            if self.streaming:
                logger.info("✓ Dataset loaded in streaming mode - data will be loaded on-demand")
                self._loaded = True
            else:
                logger.info(f"✓ Dataset loaded successfully with {len(self._dataset)} total records")

                # Process the data if not in streaming mode
                self._process_data()

        except Exception as e:
            total_time = time.time() - start_time
            logger.error(f"Failed to load dataset after {total_time:.2f} seconds: {e}")

            # Handle authentication and other errors
            if "401" in str(e) or "authentication" in str(e).lower():
                raise AuthenticationError(
                    "Authentication failed. Please ensure you have accepted the dataset terms "
                    "on Hugging Face and are properly authenticated. Visit "
                    "https://huggingface.co/datasets/blitt/SPoRC to accept the terms."
                ) from e
            elif "404" in str(e) or "not found" in str(e).lower():
                raise DatasetAccessError(
                    f"Dataset not found. Please check that the dataset ID '{self.DATASET_ID}' is correct."
                ) from e
            elif "JSON parse error" in str(e) or "Column changed from" in str(e):
                raise DatasetAccessError(
                    f"Failed to load dataset due to data quality issues. "
                    f"The Hugging Face dataset contains inconsistent data types that cannot be parsed. "
                    f"Error: {e}. "
                    f"This is a known issue with the dataset itself. "
                    f"Please try: "
                    f"1. Clearing your cache: rm -rf ~/.cache/huggingface/ "
                    f"2. Using memory mode instead of streaming mode "
                    f"3. Contacting the dataset maintainers about the data quality issues."
                ) from e
            else:
                raise DatasetAccessError(f"Failed to load dataset: {e}") from e

    def _process_data(self) -> None:
        """Process the loaded dataset into Podcast and Episode objects."""
        import time
        process_start_time = time.time()

        if self.streaming:
            logger.info("Dataset loaded in streaming mode - no data processed yet")
            logger.info("Data will be processed on-demand as you access it")
            self._loaded = True
            return

        logger.info("Processing dataset into Podcast and Episode objects...")

        # Separate episode data from speaker turn data
        logger.info("Separating episode data from speaker turn data...")
        separation_start = time.time()
        episode_data = []
        speaker_turns = []

        record_count = 0
        episode_count = 0
        turn_count = 0

        tqdm_bar = tqdm(self._dataset, total=1134058, disable=not self.show_progress, desc="Loading episodes")
        for record in tqdm_bar:
            record_count += 1
            if record_count % 10000 == 0:
                logger.debug(f"  Processed {record_count:,} records... (episodes: {episode_count}, turns: {turn_count})")

            # Check if this is episode data (has episode-specific fields)
            if 'epTitle' in record or 'podTitle' in record:
                episode_data.append(record)
                episode_count += 1
            # Check if this is speaker turn data (has turn-specific fields)
            elif 'turnText' in record or 'speaker' in record:
                speaker_turns.append(record)
                turn_count += 1

        separation_time = time.time() - separation_start
        logger.info(f"✓ Separation completed in {separation_time:.2f} seconds")
        logger.info(f"  Episode records: {len(episode_data):,}, Speaker turn records: {len(speaker_turns):,}")

        # Deduplicate episodes by mp3url
        seen_mp3urls = set()
        deduped_episode_data = []
        for episode in episode_data:
            mp3url = self._safe_string(episode.get('mp3url'))
            if mp3url and mp3url not in seen_mp3urls:
                deduped_episode_data.append(episode)
                seen_mp3urls.add(mp3url)
        episode_data = deduped_episode_data

        # Group episodes by podcast
        logger.info("Grouping episodes by podcast...")
        grouping_start = time.time()
        podcast_groups: Dict[str, List[Dict[str, Any]]] = {}
        current_podcast_title = None
        current_episodes = []

        # Process episodes in order (they're already grouped by podcast)
        for episode_dict in episode_data:
            podcast_title = self._safe_string(episode_dict.get('podTitle'), 'Unknown Podcast')
            ep_title = self._safe_string(episode_dict.get('epTitle'), 'NO_TITLE')
            logger.debug(f"Processing episode: {ep_title} (podcast: {podcast_title})")

            # Check if we've moved to a new podcast
            if current_podcast_title is None or podcast_title != current_podcast_title:
                # Store the previous podcast if it exists
                if current_podcast_title is not None and current_episodes:
                    logger.debug(f"Storing podcast: {current_podcast_title} with {len(current_episodes)} episodes")
                    podcast_groups[current_podcast_title] = current_episodes.copy()

                # Start new podcast
                current_podcast_title = podcast_title
                current_episodes = []

            # Add episode to current podcast
            current_episodes.append(episode_dict)

        # Store the last podcast
        if current_podcast_title is not None and current_episodes:
            logger.debug(f"Storing last podcast: {current_podcast_title} with {len(current_episodes)} episodes")
            podcast_groups[current_podcast_title] = current_episodes

        # After grouping, print the number of episodes per podcast
        for pt, eps in podcast_groups.items():
            logger.debug(f"Podcast '{pt}' has {len(eps)} episodes: {[self._safe_string(e.get('epTitle'), 'NO_TITLE') for e in eps]}")

        grouping_time = time.time() - grouping_start
        logger.info(f"✓ Grouping completed in {grouping_time:.2f} seconds")
        logger.info(f"  Episodes grouped into {len(podcast_groups)} podcasts")

        # Create Podcast and Episode objects
        logger.info("Creating Podcast and Episode objects...")
        creation_start = time.time()
        created_podcasts = 0
        created_episodes = 0

        for podcast_title, episode_dicts in podcast_groups.items():
            created_podcasts += 1
            if created_podcasts % 100 == 0:
                logger.debug(f"  Created {created_podcasts} podcasts... ({created_episodes} episodes)")

            # Create podcast object
            first_episode = episode_dicts[0]
            podcast = self._create_podcast_from_dict(first_episode, podcast_title)

            # Create episode objects
            for episode_dict in episode_dicts:
                episode = self._create_episode_from_dict(episode_dict, podcast_title)
                podcast.add_episode(episode)
                self._episodes.append(episode)
                created_episodes += 1

            self._podcasts[podcast_title] = podcast

        creation_time = time.time() - creation_start
        logger.info(f"✓ Object creation completed in {creation_time:.2f} seconds")
        logger.debug(f"  Created {created_podcasts} Podcast objects, {created_episodes} Episode objects")

        # Load turns for all episodes
        if self.load_turns_eagerly:
            logger.info("Loading speaker turn data for episodes...")
            turns_start = time.time()
            self._load_turns_for_episodes(speaker_turns)
            turns_time = time.time() - turns_start
            logger.info(f"✓ Speaker turn loading completed in {turns_time:.2f} seconds")
            self._turns_loaded = True
        else:
            logger.info("Storing speaker turn data for lazy loading...")
            turns_start = time.time()
            self._store_turns_for_lazy_loading(speaker_turns)
            turns_time = time.time() - turns_start
            logger.info(f"✓ Speaker turn data stored for lazy loading in {turns_time:.2f} seconds")
            # Note: _turns_loaded will be set to True in _store_turns_for_lazy_loading

        self._loaded = True
        total_process_time = time.time() - process_start_time

        logger.info(f"✓ Dataset processing completed in {total_process_time:.2f} seconds")
        logger.info(f"Final dataset: {len(self._podcasts):,} podcasts, {len(self._episodes):,} episodes")

    def load_podcast_subset(self, max_podcasts: Optional[int] = None, max_episodes: Optional[int] = None,
                           sampling_mode: str = "first", **criteria) -> None:
        """
        Load a subset of podcasts into memory based on filtering criteria.

        This method allows you to filter podcasts during the initial loading phase
        and then have O(1) access to the selected subset. This is useful when you
        want to work with a specific genre, host, or other criteria without loading
        the entire dataset.

        Args:
            max_podcasts: Maximum number of podcasts to return (None for all)
            max_episodes: Maximum number of episodes to return (None for all)
            sampling_mode: How to sample items ("first" or "random")
            **criteria: Filtering criteria including:
                - podcast_names: List of podcast names to include
                - categories: List of categories to include
                - hosts: List of host names to include
                - min_episodes: Minimum number of episodes per podcast
                - max_episodes: Maximum number of episodes per podcast
                - min_total_duration: Minimum total duration per podcast (in hours)
                - max_total_duration: Maximum total duration per podcast (in hours)
                - language: Language filter (e.g., 'en', 'es')
                - explicit: Filter by explicit content (True/False)

        Example:
            # Load only education podcasts
            sporc.load_podcast_subset(categories=['education'])

            # Load podcasts by specific hosts
            sporc.load_podcast_subset(hosts=['Simon Shapiro', 'John Doe'])

            # Load podcasts with at least 10 episodes
            sporc.load_podcast_subset(min_episodes=10)

            # Load English podcasts with at least 5 hours of content
            sporc.load_podcast_subset(language='en', min_total_duration=5.0)

            # Load first 100 education podcasts
            sporc.load_podcast_subset(categories=['education'], max_podcasts=100)

            # Load random 50 podcasts with at least 5 episodes
            sporc.load_podcast_subset(min_episodes=5, max_podcasts=50, sampling_mode="random")
        """
        if not self.streaming:
            logger.warning("load_podcast_subset() is designed for streaming mode. "
                          "In memory mode, all data is already loaded.")
            return

        import time
        start_time = time.time()

        logger.info(f"Loading podcast subset with criteria: {criteria}")
        if max_podcasts:
            logger.info(f"Limiting to {max_podcasts} podcasts (sampling mode: {sampling_mode})")
        if max_episodes:
            logger.info(f"Limiting to {max_episodes} episodes (sampling mode: {sampling_mode})")

        # Clear existing data
        self._podcasts.clear()
        self._episodes.clear()
        self._selective_mode = True

        # Load directly from files for efficiency
        if self._local_mode:
            self._load_podcast_subset_from_local_files(criteria, max_podcasts, max_episodes, sampling_mode)
        else:
            self._load_podcast_subset_from_streaming_dataset(criteria, max_podcasts, max_episodes, sampling_mode)

        self._loaded = True
        total_time = time.time() - start_time

        logger.info(f"✓ Selective loading completed in {total_time:.2f} seconds")
        logger.info(f"Final subset: {len(self._podcasts):,} podcasts, {len(self._episodes):,} episodes")

    def _load_podcast_subset_from_local_files(self, criteria: Dict[str, Any], max_podcasts: Optional[int] = None,
                                              max_episodes: Optional[int] = None, sampling_mode: str = "first") -> None:
        """Load podcast subset directly from local files for efficiency."""
        logger.info("Loading podcast subset directly from local files...")

        # Get episode data file path
        episode_file_path = None
        for file_type, file_path in self._dataset.file_paths.items():
            if 'episode' in file_type.lower():
                episode_file_path = file_path
                break

        if not episode_file_path:
            raise RuntimeError("No episode data file found in local dataset")

        logger.info(f"Reading episode data from: {episode_file_path}")

        # Group episodes by podcast and apply filters
        podcast_groups: Dict[str, List[Dict[str, Any]]] = {}
        podcast_metadata: Dict[str, Dict[str, Any]] = {}
        current_podcast_title = None
        current_episodes = []
        current_metadata = None
        record_count = 0

        import gzip
        import json
        import random

        # For random sampling, we need to collect all matching podcasts first
        all_matching_podcasts = []

        # Read episode data file
        with gzip.open(episode_file_path, 'rt', encoding='utf-8') as f:
            tqdm_bar = tqdm(desc="Loading episodes (subset)", disable=not self.show_progress)

            for line in f:
                try:
                    record = json.loads(line.strip())
                    record_count += 1

                    if record_count % 10000 == 0:
                        logger.debug(f"  Processed {record_count:,} records... (matching podcasts: {len(podcast_groups)})")

                    # Only process episode records
                    if 'epTitle' in record or 'podTitle' in record:
                        podcast_title = self._safe_string(record.get('podTitle'), 'Unknown Podcast')

                        # Check if we've moved to a new podcast
                        if current_podcast_title is None or podcast_title != current_podcast_title:
                            # Process the previous podcast if it exists
                            if current_podcast_title is not None and current_episodes:
                                if self._podcast_matches_criteria(current_podcast_title, current_metadata, criteria):
                                    if sampling_mode == "first":
                                        # For first n mode, add directly if we haven't hit the limit
                                        if max_podcasts is None or len(podcast_groups) < max_podcasts:
                                            podcast_groups[current_podcast_title] = current_episodes.copy()
                                            podcast_metadata[current_podcast_title] = current_metadata.copy()
                                    else:  # random mode
                                        # Collect all matching podcasts for later sampling
                                        all_matching_podcasts.append({
                                            'title': current_podcast_title,
                                            'episodes': current_episodes.copy(),
                                            'metadata': current_metadata.copy()
                                        })

                            # Start new podcast
                            current_podcast_title = podcast_title
                            current_episodes = []
                            current_metadata = {
                                'episodes': [],
                                'categories': set(),
                                'hosts': set(),
                                'total_duration': 0.0,
                                'language': self._safe_string(record.get('language'), 'en'),
                                'explicit': self._safe_boolean(record.get('explicit'), False)
                            }

                        # Add episode to current podcast
                        current_episodes.append(record)
                        current_metadata['episodes'].append(record)
                        current_metadata['total_duration'] += self._safe_float(record.get('durationSeconds', 0))

                        # Collect categories
                        for i in range(1, 11):
                            category = self._safe_string(record.get(f'category{i}'))
                            if category:
                                current_metadata['categories'].add(category)

                        # Collect hosts
                        host_names = self._safe_list(record.get('hostPredictedNames'))
                        current_metadata['hosts'].update(host_names)

                    tqdm_bar.update(1)

                except json.JSONDecodeError as e:
                    logger.debug(f"Skipping invalid JSON: {e}")
                    continue
                except Exception as e:
                    logger.debug(f"Skipping record due to error: {e}")
                    continue

        tqdm_bar.close()

        # Process the last podcast
        if current_podcast_title is not None and current_episodes:
            if self._podcast_matches_criteria(current_podcast_title, current_metadata, criteria):
                if sampling_mode == "first":
                    if max_podcasts is None or len(podcast_groups) < max_podcasts:
                        podcast_groups[current_podcast_title] = current_episodes
                        podcast_metadata[current_podcast_title] = current_metadata
                else:  # random mode
                    all_matching_podcasts.append({
                        'title': current_podcast_title,
                        'episodes': current_episodes,
                        'metadata': current_metadata
                    })

        # Handle random sampling
        if sampling_mode == "random" and all_matching_podcasts:
            if max_podcasts:
                # Sample podcasts randomly
                if max_podcasts < len(all_matching_podcasts):
                    selected_podcasts = random.sample(all_matching_podcasts, max_podcasts)
                else:
                    selected_podcasts = all_matching_podcasts

                for podcast_data in selected_podcasts:
                    podcast_groups[podcast_data['title']] = podcast_data['episodes']
                    podcast_metadata[podcast_data['title']] = podcast_data['metadata']
            else:
                # No limit, add all
                for podcast_data in all_matching_podcasts:
                    podcast_groups[podcast_data['title']] = podcast_data['episodes']
                    podcast_metadata[podcast_data['title']] = podcast_data['metadata']

        logger.info(f"✓ Found {len(podcast_groups)} podcasts matching criteria")

        # Apply episode-level sampling if requested
        if max_episodes and sampling_mode == "random":
            self._apply_episode_sampling(podcast_groups, max_episodes)

        # Create Podcast and Episode objects for filtered podcasts
        self._create_podcast_episode_objects(podcast_groups)

        # Load speaker turn data for selected episodes
        self._load_turns_for_subset()

    def _apply_episode_sampling(self, podcast_groups: Dict[str, List[Dict[str, Any]]], max_episodes: int) -> None:
        """Apply random sampling to episodes across all podcasts."""
        logger.info(f"Applying random episode sampling to limit to {max_episodes} episodes...")

        # Collect all episodes
        all_episodes = []
        for podcast_title, episodes in podcast_groups.items():
            for episode in episodes:
                all_episodes.append((podcast_title, episode))

        # Sample episodes randomly
        if max_episodes < len(all_episodes):
            selected_episodes = random.sample(all_episodes, max_episodes)
        else:
            selected_episodes = all_episodes

        # Rebuild podcast groups with sampled episodes
        new_podcast_groups = {}
        for podcast_title, episode in selected_episodes:
            if podcast_title not in new_podcast_groups:
                new_podcast_groups[podcast_title] = []
            new_podcast_groups[podcast_title].append(episode)

        # Replace original groups
        podcast_groups.clear()
        podcast_groups.update(new_podcast_groups)

        logger.info(f"✓ Sampled {len(selected_episodes)} episodes across {len(new_podcast_groups)} podcasts")

    def _load_podcast_subset_from_streaming_dataset(self, criteria: Dict[str, Any], max_podcasts: Optional[int] = None,
                                                   max_episodes: Optional[int] = None, sampling_mode: str = "first") -> None:
        """Load podcast subset from streaming dataset (original method for Hugging Face)."""
        logger.info("Loading podcast subset from streaming dataset...")

        # Separate episode data from speaker turn data
        logger.info("Scanning dataset to separate episode and speaker turn data...")
        scan_start = time.time()
        episode_data = []
        speaker_turns = []
        record_count = 0

        tqdm_bar = tqdm(self._dataset, total=1134058, disable=not self.show_progress, desc="Loading episodes (subset)")
        for record in tqdm_bar:
            record_count += 1
            if record_count % 10000 == 0:
                logger.debug(f"  Scanned {record_count:,} records... (episodes: {len(episode_data)}, turns: {len(speaker_turns)})")

            # Check if this is episode data (has episode-specific fields)
            if 'epTitle' in record or 'podTitle' in record:
                episode_data.append(record)
            # Check if this is speaker turn data (has turn-specific fields)
            elif 'turnText' in record or 'speaker' in record:
                speaker_turns.append(record)

        scan_time = time.time() - scan_start
        logger.info(f"✓ Dataset scanning completed in {scan_time:.2f} seconds")
        logger.info(f"  Episode records: {len(episode_data):,}, Speaker turn records: {len(speaker_turns):,}")

        # Deduplicate episodes by mp3url
        seen_mp3urls = set()
        deduped_episode_data = []
        for episode in episode_data:
            mp3url = self._safe_string(episode.get('mp3url'))
            if mp3url and mp3url not in seen_mp3urls:
                deduped_episode_data.append(episode)
                seen_mp3urls.add(mp3url)
        episode_data = deduped_episode_data

        # Group episodes by podcast and apply filters
        logger.info("Grouping episodes by podcast and applying filters...")
        grouping_start = time.time()
        podcast_groups: Dict[str, List[Dict[str, Any]]] = {}
        podcast_metadata: Dict[str, Dict[str, Any]] = {}
        current_podcast_title = None
        current_episodes = []
        current_metadata = None
        all_matching_podcasts = []

        # Process episodes in order (they're already grouped by podcast)
        for episode_dict in episode_data:
            podcast_title = self._safe_string(episode_dict.get('podTitle'), 'Unknown Podcast')

            # Check if we've moved to a new podcast
            if current_podcast_title is None or podcast_title != current_podcast_title:
                # Process the previous podcast if it exists
                if current_podcast_title is not None and current_episodes:
                    if self._podcast_matches_criteria(current_podcast_title, current_metadata, criteria):
                        if sampling_mode == "first":
                            # For first n mode, add directly if we haven't hit the limit
                            if max_podcasts is None or len(podcast_groups) < max_podcasts:
                                podcast_groups[current_podcast_title] = current_episodes.copy()
                                podcast_metadata[current_podcast_title] = current_metadata.copy()
                        else:  # random mode
                            # Collect all matching podcasts for later sampling
                            all_matching_podcasts.append({
                                'title': current_podcast_title,
                                'episodes': current_episodes.copy(),
                                'metadata': current_metadata.copy()
                            })

                # Start new podcast
                current_podcast_title = podcast_title
                current_episodes = []
                current_metadata = {
                    'episodes': [],
                    'categories': set(),
                    'hosts': set(),
                    'total_duration': 0.0,
                    'language': self._safe_string(episode_dict.get('language'), 'en'),
                    'explicit': self._safe_boolean(episode_dict.get('explicit'), False)
                }

            # Add episode to current podcast
            current_episodes.append(episode_dict)
            current_metadata['episodes'].append(episode_dict)
            current_metadata['total_duration'] += self._safe_float(episode_dict.get('durationSeconds', 0))

            # Collect categories
            for i in range(1, 11):
                category = self._safe_string(episode_dict.get(f'category{i}'))
                if category:
                    current_metadata['categories'].add(category)

            # Collect hosts
            host_names = self._safe_list(episode_dict.get('hostPredictedNames'))
            current_metadata['hosts'].update(host_names)

        # Process the last podcast
        if current_podcast_title is not None and current_episodes:
            if self._podcast_matches_criteria(current_podcast_title, current_metadata, criteria):
                if sampling_mode == "first":
                    if max_podcasts is None or len(podcast_groups) < max_podcasts:
                        podcast_groups[current_podcast_title] = current_episodes
                        podcast_metadata[current_podcast_title] = current_metadata
                else:  # random mode
                    all_matching_podcasts.append({
                        'title': current_podcast_title,
                        'episodes': current_episodes,
                        'metadata': current_metadata
                    })

        # Handle random sampling
        if sampling_mode == "random" and all_matching_podcasts:
            if max_podcasts:
                # Sample podcasts randomly
                if max_podcasts < len(all_matching_podcasts):
                    selected_podcasts = random.sample(all_matching_podcasts, max_podcasts)
                else:
                    selected_podcasts = all_matching_podcasts

                for podcast_data in selected_podcasts:
                    podcast_groups[podcast_data['title']] = podcast_data['episodes']
                    podcast_metadata[podcast_data['title']] = podcast_data['metadata']
            else:
                # No limit, add all
                for podcast_data in all_matching_podcasts:
                    podcast_groups[podcast_data['title']] = podcast_data['episodes']
                    podcast_metadata[podcast_data['title']] = podcast_data['metadata']

        grouping_time = time.time() - grouping_start
        logger.info(f"✓ Episode grouping completed in {grouping_time:.2f} seconds")
        logger.info(f"  Podcasts matching criteria: {len(podcast_groups)}")

        # Apply episode-level sampling if requested
        if max_episodes and sampling_mode == "random":
            self._apply_episode_sampling(podcast_groups, max_episodes)

        # Create Podcast and Episode objects for filtered podcasts
        self._create_podcast_episode_objects(podcast_groups)

        # Load speaker turn data for selected episodes
        self._load_turns_for_subset()

    def _create_podcast_episode_objects(self, podcast_groups: Dict[str, List[Dict[str, Any]]]) -> None:
        """Create Podcast and Episode objects from grouped episode data."""
        logger.info("Creating Podcast and Episode objects for filtered subset...")
        creation_start = time.time()
        created_podcasts = 0
        created_episodes = 0

        for podcast_title, episode_dicts in podcast_groups.items():
            created_podcasts += 1
            if created_podcasts % 10 == 0:
                logger.debug(f"  Created {created_podcasts} podcasts... ({created_episodes} episodes)")

            # Create podcast object
            first_episode = episode_dicts[0]
            podcast = self._create_podcast_from_dict(first_episode, podcast_title)

            # Create episode objects
            for episode_dict in episode_dicts:
                episode = self._create_episode_from_dict(episode_dict, podcast_title)
                podcast.add_episode(episode)
                self._episodes.append(episode)
                created_episodes += 1

            self._podcasts[podcast_title] = podcast

        creation_time = time.time() - creation_start
        logger.info(f"✓ Object creation completed in {creation_time:.2f} seconds")
        logger.debug(f"  Created {created_podcasts} Podcast objects, {created_episodes} Episode objects")

    def _load_turns_for_subset(self) -> None:
        """Load speaker turn data for the selected episodes."""
        logger.info("Loading speaker turn data for selected episodes...")
        turns_start = time.time()

        if self._local_mode:
            # For local files, read speaker turn data directly
            self._load_turns_for_subset_from_local_files()
        else:
            # For streaming dataset, use existing method
            # This would need to be implemented to work with the streaming dataset
            # For now, we'll skip turn loading in streaming mode for Hugging Face
            logger.info("Turn loading from streaming dataset not yet implemented for subset loading")

        turns_time = time.time() - turns_start
        logger.info(f"✓ Speaker turn loading completed in {turns_time:.2f} seconds")

    def _load_turns_for_subset_from_local_files(self) -> None:
        """Load speaker turn data for selected episodes from local files."""
        # Get speaker turn data file path
        turn_file_path = None
        for file_type, file_path in self._dataset.file_paths.items():
            if 'speaker' in file_type.lower() or 'turn' in file_type.lower():
                turn_file_path = file_path
                break

        if not turn_file_path:
            logger.warning("No speaker turn data file found, skipping turn loading")
            return

        logger.info(f"Loading turns from: {turn_file_path}")

        # Get all episode URLs for the selected episodes
        episode_urls = {episode.mp3_url for episode in self._episodes}

        # Group turns by episode URL
        turns_by_episode: Dict[str, List[Dict[str, Any]]] = {}
        record_count = 0

        import gzip
        import json

        with gzip.open(turn_file_path, 'rt', encoding='utf-8') as f:
            tqdm_bar = tqdm(desc="Loading speaker turns", disable=not self.show_progress)

            for line in f:
                try:
                    record = json.loads(line.strip())
                    record_count += 1

                    if record_count % 100000 == 0:
                        logger.debug(f"  Processed {record_count:,} turn records...")

                    # Only process speaker turn records
                    if 'turnText' in record or 'speaker' in record:
                        mp3_url = self._safe_string(record.get('mp3url'))
                        if mp3_url in episode_urls:
                            if mp3_url not in turns_by_episode:
                                turns_by_episode[mp3_url] = []
                            turns_by_episode[mp3_url].append(record)

                    tqdm_bar.update(1)

                except json.JSONDecodeError as e:
                    logger.debug(f"Skipping invalid JSON: {e}")
                    continue
                except Exception as e:
                    logger.debug(f"Skipping turn record due to error: {e}")
                    continue

        tqdm_bar.close()

        # Load turns for each episode
        logger.info("Loading turns for each episode...")
        loading_start = time.time()
        episodes_with_turns = 0
        total_turns_loaded = 0
        invalid_turns_skipped = 0

        for i, episode in enumerate(self._episodes):
            if i % 1000 == 0:
                logger.debug(f"  Processed {i:,} episodes... ({episodes_with_turns} with turns, {total_turns_loaded} total turns, {invalid_turns_skipped} invalid skipped)")

            episode_turns = turns_by_episode.get(episode.mp3_url, [])
            if episode_turns:
                episodes_with_turns += 1
                total_turns_loaded += len(episode_turns)

            try:
                episode.load_turns(episode_turns)
            except ValueError as e:
                if "End time must be after start time" in str(e) or "Text cannot be empty" in str(e):
                    invalid_turns_skipped += 1
                    logger.debug(f"Skipping episode {episode.title} due to invalid turn data: {e}")
                    # Try to load turns with filtering
                    try:
                        # Filter out turns with invalid time ranges or empty text
                        valid_turns = []
                        for turn in episode_turns:
                            start_time = self._safe_float(turn.get('startTime', 0))
                            end_time = self._safe_float(turn.get('endTime', 0))
                            text = self._safe_string(turn.get('turnText', ''))
                            if end_time > start_time and text.strip():
                                valid_turns.append(turn)
                        episode.load_turns(valid_turns)
                        logger.debug(f"Successfully loaded {len(valid_turns)} valid turns for episode {episode.title}")
                    except Exception as e2:
                        logger.debug(f"Failed to load even filtered turns for episode {episode.title}: {e2}")
                else:
                    raise e

        loading_time = time.time() - loading_start
        logger.info(f"✓ Turn loading completed in {loading_time:.2f} seconds")
        if len(self._episodes) > 0:
            logger.info(f"  Episodes with turns: {episodes_with_turns:,} / {len(self._episodes):,} ({episodes_with_turns/len(self._episodes)*100:.1f}%)")
        logger.info(f"  Total turns loaded: {total_turns_loaded:,}")
        if invalid_turns_skipped > 0:
            logger.info(f"  Invalid turns skipped: {invalid_turns_skipped:,}")

    def _podcast_matches_criteria(self, podcast_title: str, metadata: Dict[str, Any], criteria: Dict[str, Any]) -> bool:
        """Check if a podcast matches the given criteria."""
        # Filter by podcast names
        if 'podcast_names' in criteria:
            podcast_names = criteria['podcast_names']
            if not any(name.lower() in podcast_title.lower() for name in podcast_names):
                return False

        # Filter by categories
        if 'categories' in criteria:
            categories = criteria['categories']
            podcast_categories = {cat.lower() for cat in metadata['categories']}
            if not any(cat.lower() in podcast_categories for cat in categories):
                return False

        # Filter by hosts
        if 'hosts' in criteria:
            hosts = criteria['hosts']
            podcast_hosts = {host.lower() for host in metadata['hosts']}
            if not any(host.lower() in podcast_hosts for host in hosts):
                return False

        # Filter by episode count
        episode_count = len(metadata['episodes'])
        if 'min_episodes' in criteria and episode_count < criteria['min_episodes']:
            return False
        if 'max_episodes' in criteria and episode_count > criteria['max_episodes']:
            return False

        # Filter by total duration (convert to hours)
        total_duration_hours = metadata['total_duration'] / 3600.0
        if 'min_total_duration' in criteria and total_duration_hours < criteria['min_total_duration']:
            return False
        if 'max_total_duration' in criteria and total_duration_hours > criteria['max_total_duration']:
            return False

        # Filter by language
        if 'language' in criteria and metadata['language'] != criteria['language']:
            return False

        # Filter by explicit content
        if 'explicit' in criteria and metadata['explicit'] != criteria['explicit']:
            return False

        return True

    def _create_episode_from_dict(self, episode_dict: Dict[str, Any], podcast_title: Optional[str] = None) -> Episode:
        """Create an Episode object from a dictionary."""
        # Handle host names
        host_names = self._safe_list(episode_dict.get('hostPredictedNames'))

        # Handle guest names
        guest_names = self._safe_list(episode_dict.get('guestPredictedNames'))

        # Handle neither names
        neither_names = self._safe_list(episode_dict.get('neitherPredictedNames'))

        # Handle speaker labels
        main_speakers = self._safe_list(episode_dict.get('mainEpSpeakers'))

        # Handle host speaker labels
        host_speaker_labels = self._safe_dict(episode_dict.get('hostSpeakerLabels'))

        # Handle guest speaker labels
        guest_speaker_labels = self._safe_dict(episode_dict.get('guestSpeakerLabels'))

        # Use provided podcast_title if available, otherwise extract from episode_dict
        episode_podcast_title = podcast_title if podcast_title is not None else self._safe_string(episode_dict.get('podTitle'))

        # Handle empty episode title by providing a default
        episode_title = self._safe_string(episode_dict.get('epTitle'))
        if not episode_title.strip():
            episode_title = f"Untitled Episode ({self._safe_string(episode_dict.get('mp3url', 'unknown'))})"

        episode = Episode(
            title=episode_title,
            description=self._safe_string(episode_dict.get('epDescription')),
            mp3_url=self._safe_string(episode_dict.get('mp3url')),
            duration_seconds=self._safe_float(episode_dict.get('durationSeconds', 0)),
            transcript=self._safe_string(episode_dict.get('transcript')),
            podcast_title=episode_podcast_title,
            podcast_description=self._safe_string(episode_dict.get('podDescription')),
            rss_url=self._safe_string(episode_dict.get('rssUrl')),
            category1=self._safe_string(episode_dict.get('category1')),
            category2=self._safe_string(episode_dict.get('category2')),
            category3=self._safe_string(episode_dict.get('category3')),
            category4=self._safe_string(episode_dict.get('category4')),
            category5=self._safe_string(episode_dict.get('category5')),
            category6=self._safe_string(episode_dict.get('category6')),
            category7=self._safe_string(episode_dict.get('category7')),
            category8=self._safe_string(episode_dict.get('category8')),
            category9=self._safe_string(episode_dict.get('category9')),
            category10=self._safe_string(episode_dict.get('category10')),
            host_predicted_names=host_names,
            guest_predicted_names=guest_names,
            neither_predicted_names=neither_names,
            main_ep_speakers=main_speakers,
            host_speaker_labels=host_speaker_labels,
            guest_speaker_labels=guest_speaker_labels,
            overlap_prop_duration=self._safe_float(episode_dict.get('overlapPropDuration', 0)),
            overlap_prop_turn_count=self._safe_float(episode_dict.get('overlapPropTurnCount', 0)),
            avg_turn_duration=self._safe_float(episode_dict.get('avgTurnDuration', 0)),
            total_speaker_labels=self._safe_float(episode_dict.get('totalSpLabels', 0)),
            language=self._safe_string(episode_dict.get('language'), 'en'),
            explicit=self._safe_boolean(episode_dict.get('explicit'), False),
            image_url=self._safe_string(episode_dict.get('imageUrl')),
            episode_date_localized=self._safe_string(episode_dict.get('episodeDateLocalized')),
            oldest_episode_date=self._safe_string(episode_dict.get('oldestEpisodeDate')),
            last_update=self._safe_string(episode_dict.get('lastUpdate')),
            created_on=self._safe_string(episode_dict.get('createdOn')),
        )

        return episode

    def _load_turns_for_episodes(self, speaker_turns: List[Dict[str, Any]]) -> None:
        """Load turn data for all episodes."""
        import time
        turns_start = time.time()

        logger.info(f"Loading turn data for {len(speaker_turns):,} speaker turn records...")

        # Group turns by episode URL
        logger.info("Grouping turns by episode URL...")
        grouping_start = time.time()
        turns_by_episode: Dict[str, List[Dict[str, Any]]] = {}

        for turn in speaker_turns:
            mp3_url = self._safe_string(turn.get('mp3url'))
            if mp3_url:
                if mp3_url not in turns_by_episode:
                    turns_by_episode[mp3_url] = []
                turns_by_episode[mp3_url].append(turn)

        grouping_time = time.time() - grouping_start
        logger.info(f"✓ Turn grouping completed in {grouping_time:.2f} seconds")
        logger.info(f"  Turns grouped into {len(turns_by_episode):,} episodes")

        # Load turns for each episode
        logger.info("Loading turns for each episode...")
        loading_start = time.time()
        episodes_with_turns = 0
        total_turns_loaded = 0
        invalid_turns_skipped = 0

        for i, episode in enumerate(self._episodes):
            if i % 1000 == 0:
                logger.debug(f"  Processed {i:,} episodes... ({episodes_with_turns} with turns, {total_turns_loaded} total turns, {invalid_turns_skipped} invalid skipped)")

            episode_turns = turns_by_episode.get(episode.mp3_url, [])
            if episode_turns:
                episodes_with_turns += 1
                total_turns_loaded += len(episode_turns)

            try:
                episode.load_turns(episode_turns)
            except ValueError as e:
                if "End time must be after start time" in str(e) or "Text cannot be empty" in str(e):
                    invalid_turns_skipped += 1
                    logger.debug(f"Skipping episode {episode.title} due to invalid turn data: {e}")
                    # Try to load turns with filtering
                    try:
                        # Filter out turns with invalid time ranges or empty text
                        valid_turns = []
                        for turn in episode_turns:
                            start_time = self._safe_float(turn.get('startTime', 0))
                            end_time = self._safe_float(turn.get('endTime', 0))
                            text = self._safe_string(turn.get('turnText', ''))
                            if end_time > start_time and text.strip():
                                valid_turns.append(turn)
                        episode.load_turns(valid_turns)
                        logger.debug(f"Successfully loaded {len(valid_turns)} valid turns for episode {episode.title}")
                    except Exception as e2:
                        logger.debug(f"Failed to load even filtered turns for episode {episode.title}: {e2}")
                else:
                    raise e

        loading_time = time.time() - loading_start
        total_turns_time = time.time() - turns_start

        logger.info(f"✓ Turn loading completed in {loading_time:.2f} seconds")
        if len(self._episodes) > 0:
            logger.info(f"  Episodes with turns: {episodes_with_turns:,} / {len(self._episodes):,} ({episodes_with_turns/len(self._episodes)*100:.1f}%)")
        else:
            logger.info(f"  Episodes with turns: {episodes_with_turns:,} / {len(self._episodes):,} (no episodes to process)")
        logger.info(f"  Total turns loaded: {total_turns_loaded:,}")
        logger.info(f"  Total turn processing time: {total_turns_time:.2f} seconds")
        if invalid_turns_skipped > 0:
            logger.info(f"  Invalid turns skipped: {invalid_turns_skipped:,}")

    def search_podcast(self, name: str) -> Podcast:
        """
        Search for a podcast by name.

        Args:
            name: Name of the podcast to search for

        Returns:
            Podcast object if found

        Raises:
            NotFoundError: If the podcast is not found
        """
        if self.streaming and not self._selective_mode:
            return self._search_podcast_streaming(name)

        if not self._loaded:
            raise RuntimeError("Dataset not loaded. Call _load_dataset() first.")

        # Try exact match first
        if name in self._podcasts:
            return self._podcasts[name]

        # Try case-insensitive match
        for podcast_name, podcast in self._podcasts.items():
            if podcast_name.lower() == name.lower():
                return podcast

        # Try partial match
        for podcast_name, podcast in self._podcasts.items():
            if name.lower() in podcast_name.lower():
                return podcast

        raise NotFoundError(f"Podcast '{name}' not found")

    def _search_podcast_streaming(self, name: str) -> Podcast:
        """Search for a podcast in streaming mode."""
        logger.info(f"Searching for podcast '{name}' in streaming mode...")

        # Search through episode data to find the podcast
        found_episodes = []
        podcast_info = None
        current_podcast_title = None

        # Use safe iterator to handle data type inconsistencies
        for record in self._create_safe_iterator(self._dataset):
            # Only process episode records
            if 'epTitle' in record or 'podTitle' in record:
                try:
                    podcast_title = self._safe_string(record.get('podTitle'), '')

                    # Check if we've moved to a new podcast
                    if current_podcast_title is None or podcast_title != current_podcast_title:
                        # If we found the target podcast in the previous batch, we can stop
                        if current_podcast_title and current_podcast_title.lower() == name.lower():
                            break

                        current_podcast_title = podcast_title

                    # Check for exact match (case-insensitive)
                    if podcast_title.lower() == name.lower():
                        found_episodes.append(record)
                        if podcast_info is None:
                            podcast_info = {
                                'title': podcast_title,
                                'description': record.get('podDescription', ''),
                                'rss_url': record.get('rssUrl', ''),
                                'language': record.get('language', 'en'),
                                'explicit': bool(record.get('explicit', 0)),
                                'image_url': record.get('imageUrl'),
                                'itunes_author': record.get('itunesAuthor'),
                                'itunes_owner_name': record.get('itunesOwnerName'),
                                'host': record.get('host'),
                                'created_on': record.get('createdOn'),
                                'last_update': record.get('lastUpdate'),
                                'oldest_episode_date': record.get('oldestEpisodeDate'),
                            }
                except Exception as e:
                    logger.debug(f"Skipping record during podcast search: {e}")
                    continue

        if not found_episodes:
            raise NotFoundError(f"Podcast '{name}' not found")

        if len(found_episodes) == 0:
            raise NotFoundError(f"Podcast '{name}' not found")

        # Create podcast object
        podcast = Podcast(
            title=self._safe_string(podcast_info['title']),
            description=self._safe_string(podcast_info['description']),
            rss_url=self._safe_string(podcast_info['rss_url']),
            language=self._safe_string(podcast_info['language'], 'en'),
            explicit=self._safe_boolean(podcast_info['explicit'], False),
            image_url=self._safe_string(podcast_info['image_url']),
            itunes_author=self._safe_string(podcast_info['itunes_author']),
            itunes_owner_name=self._safe_string(podcast_info['itunes_owner_name']),
            host=self._safe_string(podcast_info['host']),
            created_on=self._safe_string(podcast_info['created_on']),
            last_update=self._safe_string(podcast_info['last_update']),
            oldest_episode_date=self._safe_string(podcast_info['oldest_episode_date']),
        )

        # Create episode objects
        for episode_dict in found_episodes:
            try:
                episode = self._create_episode_from_dict(episode_dict)
                podcast.add_episode(episode)
            except Exception as e:
                logger.debug(f"Skipping episode during podcast creation: {e}")
                continue

        logger.info(f"Found podcast '{name}' with {len(found_episodes)} episodes")
        return podcast

    def _load_turns_for_episode_streaming(self, episode: Episode) -> None:
        """Load turn data for a single episode in streaming mode."""
        if episode._turns_loaded:
            return

        turns_data = []

        # Search through speaker turn data to find turns for this episode
        for record in self._dataset:
            # Only process speaker turn records
            if 'turnText' in record or 'speaker' in record:
                if self._safe_string(record.get('mp3url')) == episode.mp3_url:
                    turns_data.append(record)

        episode.load_turns(turns_data)

    def search_episodes(self, max_episodes: Optional[int] = None, sampling_mode: str = "first", **criteria) -> List[Episode]:
        """
        Search for episodes based on various criteria.

        Args:
            max_episodes: Maximum number of episodes to return (None for all)
            sampling_mode: How to sample episodes ("first" or "random")
            **criteria: Search criteria including:
                - min_duration: Minimum duration in seconds
                - max_duration: Maximum duration in seconds
                - min_speakers: Minimum number of speakers
                - max_speakers: Maximum number of speakers
                - host_name: Host name to search for
                - guest_name: Guest name to search for
                - category: Category to search for
                - subcategory: Subcategory to search for
                - min_overlap_prop_duration: Minimum overlap proportion (duration)
                - max_overlap_prop_duration: Maximum overlap proportion (duration)
                - min_overlap_prop_turn_count: Minimum overlap proportion (turn count)
                - max_overlap_prop_turn_count: Maximum overlap proportion (turn count)

        Returns:
            List of episodes matching the criteria
        """
        if self.streaming and not self._selective_mode:
            return self._search_episodes_streaming(max_episodes, sampling_mode, **criteria)

        if not self._loaded:
            raise RuntimeError("Dataset not loaded. Call _load_dataset() first.")

        episodes = self._episodes.copy()

        # Filter by duration
        if 'min_duration' in criteria:
            min_duration = criteria['min_duration']
            episodes = [ep for ep in episodes if ep.duration_seconds >= min_duration]

        if 'max_duration' in criteria:
            max_duration = criteria['max_duration']
            episodes = [ep for ep in episodes if ep.duration_seconds <= max_duration]

        # Filter by speaker count
        if 'min_speakers' in criteria:
            min_speakers = criteria['min_speakers']
            episodes = [ep for ep in episodes if ep.num_main_speakers >= min_speakers]

        if 'max_speakers' in criteria:
            max_speakers = criteria['max_speakers']
            episodes = [ep for ep in episodes if ep.num_main_speakers <= max_speakers]

        # Filter by host name
        if 'host_name' in criteria:
            host_name = criteria['host_name'].lower()
            episodes = [
                ep for ep in episodes
                if any(host_name in host.lower() for host in ep.host_names)
            ]

        # Filter by guest name
        if 'guest_name' in criteria:
            guest_name = criteria['guest_name'].lower()
            episodes = [
                ep for ep in episodes
                if any(guest_name in guest.lower() for guest in ep.guest_names)
            ]

        # Filter by category
        if 'category' in criteria:
            category = criteria['category'].lower()
            episodes = [
                ep for ep in episodes
                if any(category in cat.lower() for cat in ep.categories)
            ]

        # Filter by subcategory
        if 'subcategory' in criteria:
            subcategory = criteria['subcategory'].lower()
            episodes = [
                ep for ep in episodes
                if any(subcategory in cat.lower() for cat in ep.categories)
            ]

        # Filter by overlap proportions
        if 'min_overlap_prop_duration' in criteria:
            min_overlap = criteria['min_overlap_prop_duration']
            episodes = [ep for ep in episodes if ep.overlap_prop_duration >= min_overlap]

        if 'max_overlap_prop_duration' in criteria:
            max_overlap = criteria['max_overlap_prop_duration']
            episodes = [ep for ep in episodes if ep.overlap_prop_duration <= max_overlap]

        if 'min_overlap_prop_turn_count' in criteria:
            min_overlap = criteria['min_overlap_prop_turn_count']
            episodes = [ep for ep in episodes if ep.overlap_prop_turn_count >= min_overlap]

        if 'max_overlap_prop_turn_count' in criteria:
            max_overlap = criteria['max_overlap_prop_turn_count']
            episodes = [ep for ep in episodes if ep.overlap_prop_turn_count <= max_overlap]

        # Apply sampling if requested
        if max_episodes and len(episodes) > max_episodes:
            if sampling_mode == "first":
                episodes = episodes[:max_episodes]
            else:  # random mode
                episodes = random.sample(episodes, max_episodes)

        return episodes

    def search_episodes_by_subcategory(self, subcategory: str, **additional_criteria) -> List[Episode]:
        """
        Search for episodes in a specific subcategory.

        Args:
            subcategory: Subcategory to search for
            **additional_criteria: Additional search criteria (same as search_episodes)

        Returns:
            List of episodes in the specified subcategory
        """
        criteria = {'subcategory': subcategory}
        criteria.update(additional_criteria)
        return self.search_episodes(**criteria)

    def search_podcasts_by_subcategory(self, subcategory: str) -> List[Podcast]:
        """
        Search for podcasts that have episodes in a specific subcategory.

        Args:
            subcategory: Subcategory to search for

        Returns:
            List of podcasts with episodes in the specified subcategory
        """
        if self.streaming and not self._selective_mode:
            return self._search_podcasts_by_subcategory_streaming(subcategory)

        if not self._loaded:
            raise RuntimeError("Dataset not loaded. Call _load_dataset() first.")

        matching_podcasts = []
        subcategory_lower = subcategory.lower()

        for podcast in self._podcasts.values():
            # Check if any episode in this podcast has the subcategory
            for episode in podcast.episodes:
                if any(subcategory_lower in cat.lower() for cat in episode.categories):
                    matching_podcasts.append(podcast)
                    break  # Found one episode, no need to check others

        return matching_podcasts

    def _search_episodes_streaming(self, max_episodes: Optional[int], sampling_mode: str, **criteria) -> List[Episode]:
        """Search for episodes in streaming mode."""
        logger.info(f"Searching for episodes with criteria: {criteria}")
        if max_episodes:
            logger.info(f"Limiting to {max_episodes} episodes (sampling mode: {sampling_mode})")

        matching_episodes = []
        episode_count = 0

        # Use safe iterator to handle data type inconsistencies
        for record in self._create_safe_iterator(self._dataset):
            # Only process episode records
            if 'epTitle' in record or 'podTitle' in record:
                try:
                    episode = self._create_episode_from_dict(record)

                    if self._episode_matches_criteria(episode, criteria):
                        episode_count += 1

                        if max_episodes:
                            if sampling_mode == "first":
                                # First n mode: just add until we hit the limit
                                if len(matching_episodes) < max_episodes:
                                    matching_episodes.append(episode)
                            else:  # random mode - use reservoir sampling
                                if len(matching_episodes) < max_episodes:
                                    # Fill the reservoir
                                    matching_episodes.append(episode)
                                else:
                                    # Reservoir sampling: replace with probability k/n
                                    j = random.randint(0, episode_count - 1)
                                    if j < max_episodes:
                                        matching_episodes[j] = episode
                        else:
                            # No limit, add all
                            matching_episodes.append(episode)

                except Exception as e:
                    logger.debug(f"Skipping episode during search: {e}")
                    continue

        logger.info(f"Found {len(matching_episodes)} matching episodes")
        return matching_episodes

    def _episode_matches_criteria(self, episode: Episode, criteria: Dict[str, Any]) -> bool:
        """Check if an episode matches the given criteria."""
        # Filter by duration
        if 'min_duration' in criteria:
            if episode.duration_seconds < criteria['min_duration']:
                return False

        if 'max_duration' in criteria:
            if episode.duration_seconds > criteria['max_duration']:
                return False

        # Filter by speaker count
        if 'min_speakers' in criteria:
            if episode.num_main_speakers < criteria['min_speakers']:
                return False

        if 'max_speakers' in criteria:
            if episode.num_main_speakers > criteria['max_speakers']:
                return False

        # Filter by host name
        if 'host_name' in criteria:
            host_name = criteria['host_name'].lower()
            if not any(host_name in host.lower() for host in episode.host_names):
                return False

        # Filter by guest name
        if 'guest_name' in criteria:
            guest_name = criteria['guest_name'].lower()
            if not any(guest_name in guest.lower() for guest in episode.guest_names):
                return False

        # Filter by category
        if 'category' in criteria:
            category = criteria['category'].lower()
            if not any(category in cat.lower() for cat in episode.categories):
                return False

        # Filter by subcategory
        if 'subcategory' in criteria:
            subcategory = criteria['subcategory'].lower()
            if not any(subcategory in cat.lower() for cat in episode.categories):
                return False

        # Filter by overlap proportions
        if 'min_overlap_prop_duration' in criteria:
            if episode.overlap_prop_duration < criteria['min_overlap_prop_duration']:
                return False

        if 'max_overlap_prop_duration' in criteria:
            if episode.overlap_prop_duration > criteria['max_overlap_prop_duration']:
                return False

        if 'min_overlap_prop_turn_count' in criteria:
            if episode.overlap_prop_turn_count < criteria['min_overlap_prop_turn_count']:
                return False

        if 'max_overlap_prop_turn_count' in criteria:
            if episode.overlap_prop_turn_count > criteria['max_overlap_prop_turn_count']:
                return False

        return True

    def get_all_podcasts(self) -> List[Podcast]:
        """
        Get all podcasts in the dataset.

        Returns:
            List of all Podcast objects
        """
        if self.streaming and not self._selective_mode:
            raise RuntimeError("get_all_podcasts() is not available in streaming mode")

        if self.streaming and self._selective_mode:
            return self._get_all_podcasts_streaming()

        if not self._loaded:
            raise RuntimeError("Dataset not loaded. Call _load_dataset() first.")

        return list(self._podcasts.values())

    def _get_all_podcasts_streaming(self) -> List[Podcast]:
        """Get all podcasts in streaming mode."""
        if not self._selective_mode:
            raise RuntimeError(
                "get_all_podcasts() is not available in streaming mode unless "
                "a subset has been loaded with load_podcast_subset(). "
                "Use iterate_podcasts() instead."
            )

        return list(self._podcasts.values())

    def get_all_episodes(self) -> List[Episode]:
        """
        Get all episodes in the dataset.

        Returns:
            List of all Episode objects
        """
        if self.streaming and not self._selective_mode:
            raise RuntimeError("get_all_episodes() is not available in streaming mode")

        if self.streaming and self._selective_mode:
            return self._get_all_episodes_streaming()

        if not self._loaded:
            raise RuntimeError("Dataset not loaded. Call _load_dataset() first.")

        return self._episodes.copy()

    def _get_all_episodes_streaming(self) -> List[Episode]:
        """Get all episodes in streaming mode."""
        if not self._selective_mode:
            raise RuntimeError(
                "get_all_episodes() is not available in streaming mode unless "
                "a subset has been loaded with load_podcast_subset(). "
                "Use iterate_episodes() instead."
            )

        return self._episodes.copy()

    def iterate_episodes(self, max_episodes: Optional[int] = None, sampling_mode: str = "first") -> Iterator[Episode]:
        """
        Iterate over all episodes in the dataset (streaming or memory mode).

        Args:
            max_episodes: Maximum number of episodes to yield (None for all)
            sampling_mode: How to sample episodes ("first" or "random")
        """
        if self.streaming:
            logger.info("Iterating over episodes in streaming mode...")
            if max_episodes:
                logger.info(f"Limiting to {max_episodes} episodes (sampling mode: {sampling_mode})")

            episode_count = 0
            yielded_count = 0

            try:
                for record in self._create_safe_iterator(self._dataset):
                    try:
                        episode = self._create_episode_from_dict(record)
                        episode_count += 1

                        if max_episodes:
                            if sampling_mode == "first":
                                # First n mode: just yield until we hit the limit
                                if yielded_count < max_episodes:
                                    yield episode
                                    yielded_count += 1
                                else:
                                    break
                            else:  # random mode - use reservoir sampling
                                if yielded_count < max_episodes:
                                    # Fill the reservoir
                                    yield episode
                                    yielded_count += 1
                                else:
                                    # Reservoir sampling: replace with probability k/n
                                    j = random.randint(0, episode_count - 1)
                                    if j < max_episodes:
                                        # We can't easily replace in an iterator, so we'll skip this one
                                        # and continue with the next
                                        continue
                        else:
                            # No limit, yield all
                            yield episode

                    except Exception as e:
                        logger.debug(f"Skipping episode due to processing error: {e}")
            except Exception as e:
                logger.error(f"Exception during episode iteration: {e}")
                raise
        else:
            logger.info("Iterating over episodes in memory mode...")
            episodes = self._episodes.copy()

            if max_episodes and len(episodes) > max_episodes:
                if sampling_mode == "first":
                    episodes = episodes[:max_episodes]
                else:  # random mode
                    episodes = random.sample(episodes, max_episodes)

            for episode in episodes:
                yield episode

    def iterate_podcasts(self, max_podcasts: Optional[int] = None, sampling_mode: str = "first") -> Iterator[Podcast]:
        """Iterate over podcasts without loading them all into memory."""
        if not self.streaming:
            raise RuntimeError("iterate_podcasts() is only available in streaming mode")

        import time
        start_time = time.time()
        logger.info("Iterating over podcasts in streaming mode...")
        if max_podcasts:
            logger.info(f"Limiting to {max_podcasts} podcasts (sampling mode: {sampling_mode})")

        current_podcast = None
        current_podcast_title = None
        episode_count = 0
        podcast_count = 0
        skipped_count = 0
        last_progress_time = time.time()

        yielded_titles = set()
        all_podcasts = []  # For random sampling

        # Use safe iterator to handle data type inconsistencies
        for record in self._create_safe_iterator(self._dataset):
            # Only process episode records
            if 'epTitle' in record or 'podTitle' in record:
                try:
                    podcast_title = self._safe_string(record.get('podTitle'), 'Unknown Podcast')

                    # Check if we've moved to a new podcast
                    if current_podcast_title is None or podcast_title != current_podcast_title:
                        # Yield the previous podcast if it exists and hasn't been yielded yet
                        if current_podcast is not None and current_podcast_title not in yielded_titles:
                            podcast_count += 1

                            if max_podcasts:
                                if sampling_mode == "first":
                                    # First n mode: yield directly
                                    if len(yielded_titles) < max_podcasts:
                                        yield current_podcast
                                        yielded_titles.add(current_podcast_title)
                                else:  # random mode - collect for later sampling
                                    all_podcasts.append(current_podcast)
                            else:
                                # No limit, yield directly
                                yield current_podcast
                                yielded_titles.add(current_podcast_title)

                            # Log progress every 10 podcasts or every 30 seconds
                            current_time = time.time()
                            if podcast_count % 10 == 0 or current_time - last_progress_time > 30:
                                elapsed = current_time - start_time
                                rate = episode_count / elapsed if elapsed > 0 else 0
                                logger.debug(f"  Processed {podcast_count} podcasts... ({episode_count} episodes, skipped: {skipped_count}, rate: {rate:.1f} eps/sec)")
                                last_progress_time = current_time

                        # Create new podcast
                        current_podcast = Podcast(
                            title=podcast_title,
                            description=self._safe_string(record.get('podDescription')),
                            rss_url=self._safe_string(record.get('rssUrl')),
                            language=self._safe_string(record.get('language'), 'en'),
                            explicit=self._safe_boolean(record.get('explicit'), False),
                            image_url=self._safe_string(record.get('imageUrl')),
                            itunes_author=self._safe_string(record.get('itunesAuthor')),
                            itunes_owner_name=self._safe_string(record.get('itunesOwnerName')),
                            host=self._safe_string(record.get('host')),
                            created_on=self._safe_string(record.get('createdOn')),
                            last_update=self._safe_string(record.get('lastUpdate')),
                            oldest_episode_date=self._safe_string(record.get('oldestEpisodeDate')),
                        )
                        current_podcast_title = podcast_title

                    # Add episode to current podcast
                    episode = self._create_episode_from_dict(record)
                    current_podcast.add_episode(episode)
                    episode_count += 1

                except Exception as e:
                    skipped_count += 1
                    logger.debug(f"Skipping podcast episode due to processing error: {e}")
                    continue

        # Yield the last podcast if it hasn't been yielded yet
        if current_podcast is not None and current_podcast_title not in yielded_titles:
            podcast_count += 1

            if max_podcasts:
                if sampling_mode == "first":
                    if len(yielded_titles) < max_podcasts:
                        yield current_podcast
                        yielded_titles.add(current_podcast_title)
                else:  # random mode
                    all_podcasts.append(current_podcast)
            else:
                yield current_podcast
                yielded_titles.add(current_podcast_title)

        # Handle random sampling
        if max_podcasts and sampling_mode == "random" and all_podcasts:
            logger.info(f"Applying random sampling to {len(all_podcasts)} podcasts...")
            if max_podcasts < len(all_podcasts):
                selected_podcasts = random.sample(all_podcasts, max_podcasts)
            else:
                selected_podcasts = all_podcasts

            for podcast in selected_podcasts:
                yield podcast

        total_time = time.time() - start_time
        logger.info(f"✓ Streaming podcast iteration completed in {total_time:.2f} seconds")
        logger.info(f"Podcasts processed: {podcast_count:,}, episodes: {episode_count:,}, skipped: {skipped_count:,}")

    def get_dataset_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics about the dataset.

        Returns:
            Dictionary with dataset statistics
        """
        if self.streaming and not self._selective_mode:
            # Calculate statistics from the entire streaming dataset
            return self._get_dataset_statistics_streaming()

        if self.streaming and self._selective_mode:
            # Return statistics based on the loaded subset
            return self._get_dataset_statistics_selective()

        if not self._loaded:
            raise RuntimeError("Dataset not loaded. Call _load_dataset() first.")

        total_episodes = len(self._episodes)
        total_podcasts = len(self._podcasts)

        if total_episodes == 0:
            return {
                'total_podcasts': 0,
                'total_episodes': 0,
                'total_duration_hours': 0.0,
                'avg_episode_duration_minutes': 0.0,
                'category_distribution': {},
                'language_distribution': {},
                'speaker_distribution': {},
            }

        # Calculate statistics
        total_duration_seconds = sum(ep.duration_seconds for ep in self._episodes)
        total_duration_hours = total_duration_seconds / 3600.0
        avg_duration_minutes = sum(ep.duration_minutes for ep in self._episodes) / total_episodes

        # Category distribution
        category_counts = {}
        for episode in self._episodes:
            for category in episode.categories:
                category_counts[category] = category_counts.get(category, 0) + 1

        # Language distribution
        language_counts = {}
        for episode in self._episodes:
            language = episode.language
            language_counts[language] = language_counts.get(language, 0) + 1

        # Speaker count distribution
        speaker_counts = {}
        for episode in self._episodes:
            speaker_count = episode.num_main_speakers
            speaker_counts[speaker_count] = speaker_counts.get(speaker_count, 0) + 1

        return {
            'total_podcasts': total_podcasts,
            'total_episodes': total_episodes,
            'total_duration_hours': total_duration_hours,
            'avg_episode_duration_minutes': avg_duration_minutes,
            'category_distribution': category_counts,
            'language_distribution': language_counts,
            'speaker_distribution': speaker_counts,
            'episode_types': {
                'solo': len([ep for ep in self._episodes if ep.is_solo]),
                'interview': len([ep for ep in self._episodes if ep.is_interview]),
                'panel': len([ep for ep in self._episodes if ep.is_panel]),
                'long_form': len([ep for ep in self._episodes if ep.is_long_form]),
                'short_form': len([ep for ep in self._episodes if ep.is_short_form]),
            },
        }

    def _get_dataset_statistics_streaming(self) -> Dict[str, Any]:
        """Get dataset statistics in streaming mode."""
        logger.info("Calculating dataset statistics in streaming mode...")

        total_episodes = 0
        total_duration = 0.0
        category_counts = {}
        language_counts = {}
        speaker_count_distribution = {}
        duration_distribution = {}
        podcast_titles = set()

        # Use safe iterator to handle data type inconsistencies
        for record in self._create_safe_iterator(self._dataset):
            # Only process episode records
            if 'epTitle' in record or 'podTitle' in record:
                try:
                    total_episodes += 1
                    duration = self._safe_float(record.get('durationSeconds', 0))
                    total_duration += duration
                    podcast_titles.add(self._safe_string(record.get('podTitle'), 'Unknown Podcast'))

                    # Count categories
                    for i in range(1, 11):
                        category = self._safe_string(record.get(f'category{i}'))
                        if category:
                            category_counts[category] = category_counts.get(category, 0) + 1

                    # Count languages
                    language = self._safe_string(record.get('language'), 'en')
                    language_counts[language] = language_counts.get(language, 0) + 1

                    # Count speaker counts
                    speaker_count = len(self._safe_list(record.get('mainEpSpeakers')))
                    speaker_count_distribution[str(speaker_count)] = speaker_count_distribution.get(str(speaker_count), 0) + 1

                    # Count duration ranges
                    duration_minutes = duration / 60
                    if duration_minutes < 10:
                        duration_range = "0-10 minutes"
                    elif duration_minutes < 30:
                        duration_range = "10-30 minutes"
                    elif duration_minutes < 60:
                        duration_range = "30-60 minutes"
                    else:
                        duration_range = "60+ minutes"
                    duration_distribution[duration_range] = duration_distribution.get(duration_range, 0) + 1

                except Exception as e:
                    logger.debug(f"Skipping record during statistics calculation: {e}")
                    continue

        logger.info(f"✓ Statistics calculated: {len(podcast_titles):,} podcasts, {total_episodes:,} episodes")

        return {
            'total_podcasts': len(podcast_titles),
            'total_episodes': total_episodes,
            'total_duration_hours': total_duration / 3600,
            'avg_episode_duration_minutes': (total_duration / 60) / total_episodes if total_episodes > 0 else 0,
            'category_distribution': category_counts,
            'language_distribution': language_counts,
            'speaker_distribution': speaker_count_distribution,
            'duration_distribution': duration_distribution,
        }

    def _get_dataset_statistics_selective(self) -> Dict[str, Any]:
        """Get dataset statistics for selective loading mode."""
        total_episodes = len(self._episodes)
        total_podcasts = len(self._podcasts)

        if total_episodes == 0:
            return {
                'total_podcasts': 0,
                'total_episodes': 0,
                'total_duration_hours': 0.0,
                'avg_episode_duration_minutes': 0.0,
                'category_distribution': {},
                'language_distribution': {},
                'speaker_distribution': {},
            }

        # Calculate statistics
        total_duration_seconds = sum(ep.duration_seconds for ep in self._episodes)
        total_duration_hours = total_duration_seconds / 3600.0
        avg_duration_minutes = sum(ep.duration_minutes for ep in self._episodes) / total_episodes

        # Category distribution
        category_counts = {}
        for episode in self._episodes:
            for category in episode.categories:
                category_counts[category] = category_counts.get(category, 0) + 1

        # Language distribution
        language_counts = {}
        for episode in self._episodes:
            language = episode.language
            language_counts[language] = language_counts.get(language, 0) + 1

        # Speaker count distribution
        speaker_counts = {}
        for episode in self._episodes:
            speaker_count = episode.num_main_speakers
            speaker_counts[speaker_count] = speaker_counts.get(speaker_count, 0) + 1

        return {
            'total_podcasts': total_podcasts,
            'total_episodes': total_episodes,
            'total_duration_hours': total_duration_hours,
            'avg_episode_duration_minutes': avg_duration_minutes,
            'category_distribution': category_counts,
            'language_distribution': language_counts,
            'speaker_distribution': speaker_counts,
        }

    def __len__(self) -> int:
        """Get the number of episodes in the dataset."""
        if self.streaming and not self._selective_mode:
            raise RuntimeError("len() is not available in streaming mode")
        if self.streaming and self._selective_mode:
            # Return the total number of episodes in the dataset
            return 1134058
        return len(self._episodes)

    def __str__(self) -> str:
        """String representation of the dataset."""
        mode_info = []
        if self._local_mode:
            mode_info.append("local")
        if self.streaming:
            mode_info.append("streaming")
        if self._selective_mode:
            mode_info.append("selective")

        mode_str = f"({', '.join(mode_info)})" if mode_info else ""

        if self.streaming:
            if self._selective_mode:
                return f"SPORCDataset{mode_str}({len(self._podcasts)} podcasts, {len(self._episodes)} episodes)"
            return f"SPORCDataset{mode_str}"
        return f"SPORCDataset{mode_str}({len(self._podcasts)} podcasts, {len(self._episodes)} episodes)"

    def __repr__(self) -> str:
        """Detailed string representation of the dataset."""
        mode_info = []
        if self._local_mode:
            mode_info.append("local")
        if self.streaming:
            mode_info.append("streaming")
        if self._selective_mode:
            mode_info.append("selective")

        mode_str = f"({', '.join(mode_info)})" if mode_info else ""

        if self.streaming:
            if self._selective_mode:
                return f"SPORCDataset{mode_str}(podcasts={len(self._podcasts)}, episodes={len(self._episodes)}, loaded={self._loaded})"
            return f"SPORCDataset{mode_str}(loaded={self._loaded})"
        return (f"SPORCDataset{mode_str}(podcasts={len(self._podcasts)}, episodes={len(self._episodes)}, "
                f"loaded={self._loaded})")

    def _search_podcasts_by_subcategory_streaming(self, subcategory: str) -> List[Podcast]:
        """Search for podcasts by subcategory in streaming mode."""
        logger.info(f"Searching for podcasts with subcategory '{subcategory}' in streaming mode...")

        podcast_dict: Dict[str, Podcast] = {}
        current_podcast = None
        current_podcast_title = None
        current_has_subcategory = False

        # Use safe iterator to handle data type inconsistencies
        for record in self._create_safe_iterator(self._dataset):
            # Only process episode records
            if 'epTitle' in record or 'podTitle' in record:
                try:
                    podcast_title = self._safe_string(record.get('podTitle'), 'Unknown Podcast')

                    # Check if we've moved to a new podcast
                    if current_podcast_title is None or podcast_title != current_podcast_title:
                        # If the previous podcast had the subcategory, keep it
                        if current_podcast is not None and current_has_subcategory:
                            podcast_dict[current_podcast_title] = current_podcast

                        # Start new podcast
                        current_podcast_title = podcast_title
                        current_has_subcategory = False

                        # Create new podcast
                        current_podcast = Podcast(
                            title=podcast_title,
                            description=self._safe_string(record.get('podDescription')),
                            rss_url=self._safe_string(record.get('rssUrl')),
                            language=self._safe_string(record.get('language'), 'en'),
                            explicit=self._safe_boolean(record.get('explicit'), False),
                            image_url=self._safe_string(record.get('imageUrl')),
                            itunes_author=self._safe_string(record.get('itunesAuthor')),
                            itunes_owner_name=self._safe_string(record.get('itunesOwnerName')),
                            host=self._safe_string(record.get('host')),
                            created_on=self._safe_string(record.get('createdOn')),
                            last_update=self._safe_string(record.get('lastUpdate')),
                            oldest_episode_date=self._safe_string(record.get('oldestEpisodeDate')),
                        )

                    # Check if this episode has the subcategory
                    has_subcategory = False
                    for i in range(1, 11):
                        category = self._safe_string(record.get(f'category{i}'))
                        if category == subcategory:
                            has_subcategory = True
                            current_has_subcategory = True
                            break

                    # Add episode to current podcast
                    episode = self._create_episode_from_dict(record)
                    current_podcast.add_episode(episode)

                except Exception as e:
                    logger.debug(f"Skipping record during subcategory search: {e}")
                    continue

        # Check the last podcast
        if current_podcast is not None and current_has_subcategory:
            podcast_dict[current_podcast_title] = current_podcast

        logger.info(f"Found {len(podcast_dict)} podcasts with subcategory '{subcategory}'")
        return list(podcast_dict.values())

    @staticmethod
    def find_cache_directories() -> Dict[str, str]:
        """
        Find existing Hugging Face cache directories on the system.

        Returns:
            Dictionary mapping cache type to directory path
        """
        cache_dirs = {}

        # Common cache locations
        possible_paths = [
            ("default", Path.home() / ".cache" / "huggingface"),
            ("macos", Path.home() / "Library" / "Caches" / "huggingface"),
            ("windows", Path.home() / "AppData" / "Local" / "huggingface"),
            ("user_cache", Path.home() / ".cache" / "huggingface_hub"),
        ]

        for cache_type, path in possible_paths:
            if path.exists():
                cache_dirs[cache_type] = str(path)

        return cache_dirs

    @staticmethod
    def validate_cache_directory(cache_dir: str) -> bool:
        """
        Validate if a cache directory contains the SPORC dataset.

        Args:
            cache_dir: Path to the cache directory to validate

        Returns:
            True if the directory contains SPORC dataset files, False otherwise
        """
        import os
        from pathlib import Path

        cache_path = Path(cache_dir)
        if not cache_path.exists():
            return False

        # Look for SPORC dataset files
        sporc_indicators = [
            "datasets/blitt/SPoRC",
            "datasets--blitt--SPoRC",
            "SPoRC",
        ]

        for indicator in sporc_indicators:
            if (cache_path / indicator).exists():
                return True

        return False

    @staticmethod
    def list_available_datasets(cache_dir: Optional[str] = None) -> List[str]:
        """
        List available datasets in a cache directory.

        Args:
            cache_dir: Path to cache directory. If None, searches common locations.

        Returns:
            List of available dataset names
        """
        import os
        from pathlib import Path

        datasets = []

        if cache_dir:
            search_paths = [Path(cache_dir)]
        else:
            search_paths = [
                Path.home() / ".cache" / "huggingface",
                Path.home() / "Library" / "Caches" / "huggingface",
                Path.home() / "AppData" / "Local" / "huggingface",
            ]

        for search_path in search_paths:
            if not search_path.exists():
                continue

            # Look for dataset directories
            for item in search_path.iterdir():
                if item.is_dir():
                    if "datasets" in item.name or "SPoRC" in item.name:
                        datasets.append(str(item))

        return datasets

    def _create_podcast_from_dict(self, first_episode: Dict[str, Any], podcast_title: str) -> Podcast:
        """Create a Podcast object from a dictionary with safe field handling."""
        return Podcast(
            title=podcast_title,
            description=self._safe_string(first_episode.get('podDescription')),
            rss_url=self._safe_string(first_episode.get('rssUrl')),
            language=self._safe_string(first_episode.get('language'), 'en'),
            explicit=self._safe_boolean(first_episode.get('explicit'), False),
            image_url=self._safe_string(first_episode.get('imageUrl')),
            itunes_author=self._safe_string(first_episode.get('itunesAuthor')),
            itunes_owner_name=self._safe_string(first_episode.get('itunesOwnerName')),
            host=self._safe_string(first_episode.get('host')),
            created_on=self._safe_string(first_episode.get('createdOn')),
            last_update=self._safe_string(first_episode.get('lastUpdate')),
            oldest_episode_date=self._safe_string(first_episode.get('oldestEpisodeDate')),
        )

    def _store_turns_for_lazy_loading(self, speaker_turns: List[Dict[str, Any]]) -> None:
        """Store turn data for lazy loading without loading it into episodes."""
        logger.info(f"Setting up lazy loading for {len(speaker_turns):,} speaker turn records...")

        if self._local_mode:
            # For local files, build an index for efficient access
            logger.info("Building turn index for efficient lazy loading...")
            self._build_turn_index()
        else:
            # For Hugging Face datasets, store in memory (fallback)
            logger.info("Storing turn data in memory for lazy loading...")

            # Group turns by episode URL
            logger.info("Grouping turns by episode URL...")
            grouping_start = time.time()

            for turn in speaker_turns:
                mp3_url = self._safe_string(turn.get('mp3url'))
                if mp3_url:
                    if mp3_url not in self._turns_by_episode:
                        self._turns_by_episode[mp3_url] = []
                    self._turns_by_episode[mp3_url].append(turn)

            grouping_time = time.time() - grouping_start
            logger.info(f"✓ Turn grouping completed in {grouping_time:.2f} seconds")
            logger.info(f"  Turns grouped into {len(self._turns_by_episode):,} episodes")

        # Set _turns_loaded = True for lazy loading since turns are stored and available
        # even though they're not loaded into individual episodes yet
        self._turns_loaded = True

    def load_turns_for_episode(self, episode: Episode) -> None:
        """
        Load turn data for a specific episode on-demand.

        Args:
            episode: Episode to load turns for
        """
        if episode._turns_loaded:
            return

        if not self._turns_loaded:
            raise RuntimeError("Turn data not available. Dataset may not be loaded or turn data was not stored.")

        # Use efficient indexed loading if available
        if self._local_mode and self._index_built:
            self._load_turns_for_episode_efficient(episode)
            return

        # Fall back to in-memory loading
        episode_turns = self._turns_by_episode.get(episode.mp3_url, [])
        if episode_turns:
            try:
                episode.load_turns(episode_turns)
                logger.debug(f"Loaded {len(episode_turns)} turns for episode: {episode.title}")
            except ValueError as e:
                if "End time must be after start time" in str(e) or "Text cannot be empty" in str(e):
                    logger.debug(f"Skipping episode {episode.title} due to invalid turn data: {e}")
                    # Try to load turns with filtering
                    try:
                        # Filter out turns with invalid time ranges or empty text
                        valid_turns = []
                        for turn in episode_turns:
                            start_time = self._safe_float(turn.get('startTime', 0))
                            end_time = self._safe_float(turn.get('endTime', 0))
                            text = self._safe_string(turn.get('turnText', ''))
                            if end_time > start_time and text.strip():
                                valid_turns.append(turn)
                        episode.load_turns(valid_turns)
                        logger.debug(f"Successfully loaded {len(valid_turns)} valid turns for episode {episode.title}")
                    except Exception as e2:
                        logger.debug(f"Failed to load even filtered turns for episode {episode.title}: {e2}")
                else:
                    raise e
        else:
            logger.debug(f"No turns found for episode: {episode.title}")

    def load_turns_for_podcast(self, podcast: Podcast) -> None:
        """
        Load turn data for all episodes in a podcast on-demand.

        Args:
            podcast: Podcast to load turns for
        """
        logger.info(f"Loading turns for podcast: {podcast.title} ({len(podcast.episodes)} episodes)")

        for episode in podcast.episodes:
            self.load_turns_for_episode(episode)

        logger.info(f"✓ Loaded turns for all episodes in podcast: {podcast.title}")

    def preload_turns_for_episodes(self, episodes: List[Episode]) -> None:
        """
        Preload turn data for a list of episodes.

        Args:
            episodes: List of episodes to load turns for
        """
        logger.info(f"Preloading turns for {len(episodes)} episodes...")

        # Use efficient indexed loading if available
        if self._local_mode and self._index_built:
            episode_urls = [ep.mp3_url for ep in episodes]
            turns_data = self._load_turns_from_index(episode_urls)

            # Load turns for each episode
            for episode in episodes:
                episode_turns = turns_data.get(episode.mp3_url, [])
                if episode_turns:
                    try:
                        episode.load_turns(episode_turns)
                    except ValueError as e:
                        if "End time must be after start time" in str(e) or "Text cannot be empty" in str(e):
                            logger.debug(f"Skipping episode {episode.title} due to invalid turn data: {e}")
                            # Try to load turns with filtering
                            try:
                                valid_turns = []
                                for turn in episode_turns:
                                    start_time = self._safe_float(turn.get('startTime', 0))
                                    end_time = self._safe_float(turn.get('endTime', 0))
                                    text = self._safe_string(turn.get('turnText', ''))
                                    if end_time > start_time and text.strip():
                                        valid_turns.append(turn)
                                episode.load_turns(valid_turns)
                                logger.debug(f"Successfully loaded {len(valid_turns)} valid turns for episode {episode.title}")
                            except Exception as e2:
                                logger.debug(f"Failed to load even filtered turns for episode {episode.title}: {e2}")
                        else:
                            raise e
        else:
            # Fall back to individual loading
            for episode in episodes:
                self.load_turns_for_episode(episode)

        logger.info(f"✓ Preloaded turns for {len(episodes)} episodes")

    def _build_turn_index(self) -> None:
        """
        Build an index mapping episode URLs to file offsets in the turn data file.
        This allows efficient random access to turns for specific episodes.
        """
        if self._index_built:
            return

        if not self._local_mode:
            logger.warning("Turn indexing is only available for local files")
            return

        # Find the turn data file
        turn_file_path = None
        if hasattr(self, '_dataset') and hasattr(self._dataset, 'file_paths'):
            for file_type, file_path in self._dataset.file_paths.items():
                if 'speaker' in file_type.lower() or 'turn' in file_type.lower():
                    turn_file_path = file_path
                    break

        if not turn_file_path:
            logger.warning("No speaker turn data file found for indexing")
            return

        self._turn_file_path = turn_file_path

        # Check if index file already exists
        index_file_path = turn_file_path + '.index'
        if os.path.exists(index_file_path):
            logger.info(f"Loading existing turn index from {index_file_path}")
            try:
                with open(index_file_path, 'rb') as f:
                    self._turn_index = pickle.load(f)
                self._index_built = True
                logger.info(f"✓ Loaded index for {len(self._turn_index)} episodes")
                return
            except Exception as e:
                logger.warning(f"Failed to load existing index: {e}")

        # Build the index
        logger.info(f"Building turn index for {turn_file_path}...")
        start_time = time.time()

        self._turn_index = {}
        record_count = 0

        try:
            with gzip.open(turn_file_path, 'rt', encoding='utf-8') as f:
                tqdm_bar = tqdm(desc="Building turn index", disable=not self.show_progress)

                for line in f:
                    try:
                        # Get current file position
                        offset = f.tell()

                        # Parse the record
                        record = json.loads(line.strip())
                        record_count += 1

                        # Only process speaker turn records
                        if 'turnText' in record or 'speaker' in record:
                            mp3_url = self._safe_string(record.get('mp3url'))
                            if mp3_url:
                                if mp3_url not in self._turn_index:
                                    self._turn_index[mp3_url] = []
                                self._turn_index[mp3_url].append(offset)

                        tqdm_bar.update(1)

                        # Log progress every 100,000 records
                        if record_count % 100000 == 0:
                            logger.debug(f"  Indexed {record_count:,} records, {len(self._turn_index)} episodes")

                    except json.JSONDecodeError as e:
                        logger.debug(f"Skipping invalid JSON: {e}")
                        continue
                    except Exception as e:
                        logger.debug(f"Skipping record due to error: {e}")
                        continue

                tqdm_bar.close()

            # Save the index
            try:
                with open(index_file_path, 'wb') as f:
                    pickle.dump(self._turn_index, f)
                logger.info(f"✓ Saved turn index to {index_file_path}")
            except Exception as e:
                logger.warning(f"Failed to save turn index: {e}")

            index_time = time.time() - start_time
            logger.info(f"✓ Turn index built in {index_time:.2f} seconds")
            logger.info(f"  Indexed {record_count:,} records for {len(self._turn_index)} episodes")
            self._index_built = True

        except Exception as e:
            logger.error(f"Error building turn index: {e}")
            self._index_built = False

    def _load_turns_from_index(self, episode_urls: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Load turns for specific episodes using the index for efficient random access.

        Args:
            episode_urls: List of episode URLs to load turns for

        Returns:
            Dictionary mapping episode URLs to their turn data
        """
        if not self._index_built:
            self._build_turn_index()

        if not self._turn_file_path:
            logger.warning("No turn file available for indexed loading")
            return {}

        logger.info(f"Loading turns for {len(episode_urls)} episodes using index...")
        start_time = time.time()

        turns_by_episode = {}
        total_turns_loaded = 0

        with gzip.open(self._turn_file_path, 'rt', encoding='utf-8') as f:
            for episode_url in episode_urls:
                if episode_url not in self._turn_index:
                    logger.debug(f"No turns found for episode: {episode_url}")
                    continue

                episode_turns = []
                offsets = self._turn_index[episode_url]

                for offset in offsets:
                    try:
                        # Seek to the specific offset
                        f.seek(offset)
                        line = f.readline()

                        if line:
                            record = json.loads(line.strip())
                            episode_turns.append(record)

                    except Exception as e:
                        logger.debug(f"Error reading turn at offset {offset}: {e}")
                        continue

                if episode_turns:
                    turns_by_episode[episode_url] = episode_turns
                    total_turns_loaded += len(episode_turns)
                    logger.debug(f"Loaded {len(episode_turns)} turns for {episode_url}")

        load_time = time.time() - start_time
        logger.info(f"✓ Loaded {total_turns_loaded} turns for {len(turns_by_episode)} episodes in {load_time:.2f} seconds")

        return turns_by_episode

    def _load_turns_for_episode_efficient(self, episode: Episode) -> None:
        """
        Load turns for a single episode using the index for efficient access.

        Args:
            episode: Episode to load turns for
        """
        if episode._turns_loaded:
            return

        if not self._index_built:
            self._build_turn_index()

        if episode.mp3_url not in self._turn_index:
            logger.debug(f"No turns found for episode: {episode.title}")
            return

        # Load turns for this specific episode
        turns_data = self._load_turns_from_index([episode.mp3_url])
        episode_turns = turns_data.get(episode.mp3_url, [])

        if episode_turns:
            try:
                episode.load_turns(episode_turns)
                logger.debug(f"Loaded {len(episode_turns)} turns for episode: {episode.title}")
            except ValueError as e:
                if "End time must be after start time" in str(e) or "Text cannot be empty" in str(e):
                    logger.debug(f"Skipping episode {episode.title} due to invalid turn data: {e}")
                    # Try to load turns with filtering
                    try:
                        valid_turns = []
                        for turn in episode_turns:
                            start_time = self._safe_float(turn.get('startTime', 0))
                            end_time = self._safe_float(turn.get('endTime', 0))
                            text = self._safe_string(turn.get('turnText', ''))
                            if end_time > start_time and text.strip():
                                valid_turns.append(turn)
                        episode.load_turns(valid_turns)
                        logger.debug(f"Successfully loaded {len(valid_turns)} valid turns for episode {episode.title}")
                    except Exception as e2:
                        logger.debug(f"Failed to load even filtered turns for episode {episode.title}: {e2}")
                else:
                    raise e
        else:
            logger.debug(f"No turns found for episode: {episode.title}")

    def build_turn_index_async(self) -> None:
        """
        Build the turn index in a background thread.
        This allows the dataset to be used while the index is being built.
        """
        if self._index_built:
            return

        def build_index():
            try:
                self._build_turn_index()
            except Exception as e:
                logger.error(f"Error building turn index: {e}")

        # Start index building in background thread
        thread = threading.Thread(target=build_index, daemon=True)
        thread.start()
        logger.info("Turn index building started in background thread")

    def get_index_status(self) -> Dict[str, Any]:
        """
        Get the status of the turn index.

        Returns:
            Dictionary with index status information
        """
        return {
            'index_built': self._index_built,
            'episodes_indexed': len(self._turn_index),
            'turn_file_path': self._turn_file_path,
            'local_mode': self._local_mode
        }