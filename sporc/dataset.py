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
                 local_data_dir: Optional[str] = None):
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
        """
        self.cache_dir = custom_cache_dir if custom_cache_dir else cache_dir
        self.use_auth_token = use_auth_token
        self.streaming = streaming
        self.custom_cache_dir = custom_cache_dir
        self.local_data_dir = local_data_dir

        # Data storage
        self._dataset = None
        self._podcasts: Dict[str, Podcast] = {}
        self._episodes: List[Episode] = []
        self._loaded = False
        self._selective_mode = False
        self._local_mode = local_data_dir is not None

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
                        record[field] = float(record[field])
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

        for record in self._dataset:
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
            mp3url = episode.get('mp3url')
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
            podcast_title = episode_dict.get('podTitle', 'Unknown Podcast')
            ep_title = episode_dict.get('epTitle', 'NO_TITLE')
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
            logger.debug(f"Podcast '{pt}' has {len(eps)} episodes: {[e.get('epTitle', 'NO_TITLE') for e in eps]}")

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
            podcast = Podcast(
                title=podcast_title,
                description=first_episode.get('podDescription', ''),
                rss_url=first_episode.get('rssUrl', ''),
                language=first_episode.get('language', 'en'),
                explicit=bool(first_episode.get('explicit', 0)),
                image_url=first_episode.get('imageUrl'),
                itunes_author=first_episode.get('itunesAuthor'),
                itunes_owner_name=first_episode.get('itunesOwnerName'),
                host=first_episode.get('host'),
                created_on=first_episode.get('createdOn'),
                last_update=first_episode.get('lastUpdate'),
                oldest_episode_date=first_episode.get('oldestEpisodeDate'),
            )

            # Create episode objects
            for episode_dict in episode_dicts:
                episode = self._create_episode_from_dict(episode_dict)
                podcast.add_episode(episode)
                self._episodes.append(episode)
                created_episodes += 1

            self._podcasts[podcast_title] = podcast

        creation_time = time.time() - creation_start
        logger.info(f"✓ Object creation completed in {creation_time:.2f} seconds")
        logger.debug(f"  Created {created_podcasts} Podcast objects, {created_episodes} Episode objects")

        # Load turns for all episodes
        logger.info("Loading speaker turn data for episodes...")
        turns_start = time.time()
        self._load_turns_for_episodes(speaker_turns)
        turns_time = time.time() - turns_start
        logger.info(f"✓ Speaker turn loading completed in {turns_time:.2f} seconds")

        self._loaded = True
        total_process_time = time.time() - process_start_time

        logger.info(f"✓ Dataset processing completed in {total_process_time:.2f} seconds")
        logger.info(f"Final dataset: {len(self._podcasts):,} podcasts, {len(self._episodes):,} episodes")

    def load_podcast_subset(self, **criteria) -> None:
        """
        Load a subset of podcasts into memory based on filtering criteria.

        This method allows you to filter podcasts during the initial loading phase
        and then have O(1) access to the selected subset. This is useful when you
        want to work with a specific genre, host, or other criteria without loading
        the entire dataset.

        Args:
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
        """
        if not self.streaming:
            logger.warning("load_podcast_subset() is designed for streaming mode. "
                          "In memory mode, all data is already loaded.")
            return

        import time
        start_time = time.time()

        logger.info(f"Loading podcast subset with criteria: {criteria}")

        # Clear existing data
        self._podcasts.clear()
        self._episodes.clear()
        self._selective_mode = True

        # Separate episode data from speaker turn data
        logger.info("Scanning dataset to separate episode and speaker turn data...")
        scan_start = time.time()
        episode_data = []
        speaker_turns = []
        record_count = 0

        for record in self._dataset:
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
            mp3url = episode.get('mp3url')
            if mp3url and mp3url not in seen_mp3urls:
                deduped_episode_data.append(episode)
                seen_mp3urls.add(mp3url)
        episode_data = deduped_episode_data

        # Group episodes by podcast and apply filters (taking advantage of contiguous nature)
        logger.info("Grouping episodes by podcast and applying filters...")
        grouping_start = time.time()
        podcast_groups: Dict[str, List[Dict[str, Any]]] = {}
        podcast_metadata: Dict[str, Dict[str, Any]] = {}
        current_podcast_title = None
        current_episodes = []
        current_metadata = None

        # Process episodes in order (they're already grouped by podcast)
        for episode_dict in episode_data:
            podcast_title = episode_dict.get('podTitle', 'Unknown Podcast')

            # Check if we've moved to a new podcast
            if current_podcast_title is None or podcast_title != current_podcast_title:
                # Process the previous podcast if it exists
                if current_podcast_title is not None and current_episodes:
                    # Check if this podcast matches our criteria
                    if self._podcast_matches_criteria(current_podcast_title, current_metadata, criteria):
                        podcast_groups[current_podcast_title] = current_episodes.copy()
                        podcast_metadata[current_podcast_title] = current_metadata.copy()

                # Start new podcast
                current_podcast_title = podcast_title
                current_episodes = []
                current_metadata = {
                    'episodes': [],
                    'categories': set(),
                    'hosts': set(),
                    'total_duration': 0.0,
                    'language': episode_dict.get('language', 'en'),
                    'explicit': bool(episode_dict.get('explicit', 0))
                }

            # Add episode to current podcast
            current_episodes.append(episode_dict)
            current_metadata['episodes'].append(episode_dict)
            current_metadata['total_duration'] += float(episode_dict.get('durationSeconds', 0))

            # Collect categories
            for i in range(1, 11):
                category = episode_dict.get(f'category{i}')
                if category:
                    current_metadata['categories'].add(category)

            # Collect hosts
            host_names = episode_dict.get('hostPredictedNames', [])
            if isinstance(host_names, str):
                if host_names != "NO_HOST_PREDICTED":
                    current_metadata['hosts'].add(host_names)
            else:
                current_metadata['hosts'].update(host_names)

        # Process the last podcast
        if current_podcast_title is not None and current_episodes:
            if self._podcast_matches_criteria(current_podcast_title, current_metadata, criteria):
                podcast_groups[current_podcast_title] = current_episodes
                podcast_metadata[current_podcast_title] = current_metadata

        grouping_time = time.time() - grouping_start
        logger.info(f"✓ Episode grouping completed in {grouping_time:.2f} seconds")
        logger.info(f"  Podcasts matching criteria: {len(podcast_groups)}")

        # Create Podcast and Episode objects for filtered podcasts
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
            podcast = Podcast(
                title=podcast_title,
                description=first_episode.get('podDescription', ''),
                rss_url=first_episode.get('rssUrl', ''),
                language=first_episode.get('language', 'en'),
                explicit=bool(first_episode.get('explicit', 0)),
                image_url=first_episode.get('imageUrl'),
                itunes_author=first_episode.get('itunesAuthor'),
                itunes_owner_name=first_episode.get('itunesOwnerName'),
                host=first_episode.get('host'),
                created_on=first_episode.get('createdOn'),
                last_update=first_episode.get('lastUpdate'),
                oldest_episode_date=first_episode.get('oldestEpisodeDate'),
            )

            # Create episode objects
            for episode_dict in episode_dicts:
                episode = self._create_episode_from_dict(episode_dict)
                podcast.add_episode(episode)
                self._episodes.append(episode)
                created_episodes += 1

            self._podcasts[podcast_title] = podcast

        creation_time = time.time() - creation_start
        logger.info(f"✓ Object creation completed in {creation_time:.2f} seconds")
        logger.debug(f"  Created {created_podcasts} Podcast objects, {created_episodes} Episode objects")

        # Load turns for selected episodes
        logger.info("Loading speaker turn data for selected episodes...")
        turns_start = time.time()
        self._load_turns_for_episodes(speaker_turns)
        turns_time = time.time() - turns_start
        logger.info(f"✓ Speaker turn loading completed in {turns_time:.2f} seconds")

        self._loaded = True
        total_time = time.time() - start_time

        logger.info(f"✓ Selective loading completed in {total_time:.2f} seconds")
        logger.info(f"Final subset: {len(self._podcasts):,} podcasts, {len(self._episodes):,} episodes")

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

    def _create_episode_from_dict(self, episode_dict: Dict[str, Any]) -> Episode:
        """Create an Episode object from a dictionary."""
        # Handle host names
        host_names = episode_dict.get('hostPredictedNames', [])
        if isinstance(host_names, str):
            if host_names == "NO_HOST_PREDICTED":
                host_names = []
            else:
                host_names = [host_names]

        # Handle guest names
        guest_names = episode_dict.get('guestPredictedNames', [])
        if isinstance(guest_names, str):
            if guest_names == "NO_GUEST_PREDICTED":
                guest_names = []
            else:
                guest_names = [guest_names]

        # Handle neither names
        neither_names = episode_dict.get('neitherPredictedNames', [])
        if isinstance(neither_names, str):
            if neither_names == "NO_NEITHER_IDENTIFIED":
                neither_names = []
            else:
                neither_names = [neither_names]

        # Handle speaker labels
        main_speakers = episode_dict.get('mainEpSpeakers', [])
        if isinstance(main_speakers, str):
            if main_speakers == "SPEAKER_DATA_UNAVAILABLE":
                main_speakers = []
            else:
                main_speakers = [main_speakers]

        # Handle host speaker labels
        host_speaker_labels = episode_dict.get('hostSpeakerLabels', {})
        if isinstance(host_speaker_labels, str):
            if host_speaker_labels == "SPEAKER_DATA_UNAVAILABLE":
                host_speaker_labels = {}
            else:
                try:
                    host_speaker_labels = json.loads(host_speaker_labels)
                except (json.JSONDecodeError, TypeError):
                    host_speaker_labels = {}

        # Handle guest speaker labels
        guest_speaker_labels = episode_dict.get('guestSpeakerLabels', {})
        if isinstance(guest_speaker_labels, str):
            if guest_speaker_labels == "SPEAKER_DATA_UNAVAILABLE":
                guest_speaker_labels = {}
            else:
                try:
                    guest_speaker_labels = json.loads(guest_speaker_labels)
                except (json.JSONDecodeError, TypeError):
                    guest_speaker_labels = {}

        return Episode(
            title=episode_dict.get('epTitle', ''),
            description=episode_dict.get('epDescription', ''),
            mp3_url=episode_dict.get('mp3url', ''),
            duration_seconds=float(episode_dict.get('durationSeconds', 0)),
            transcript=episode_dict.get('transcript', ''),
            podcast_title=episode_dict.get('podTitle', ''),
            podcast_description=episode_dict.get('podDescription', ''),
            rss_url=episode_dict.get('rssUrl', ''),
            category1=episode_dict.get('category1'),
            category2=episode_dict.get('category2'),
            category3=episode_dict.get('category3'),
            category4=episode_dict.get('category4'),
            category5=episode_dict.get('category5'),
            category6=episode_dict.get('category6'),
            category7=episode_dict.get('category7'),
            category8=episode_dict.get('category8'),
            category9=episode_dict.get('category9'),
            category10=episode_dict.get('category10'),
            host_predicted_names=host_names,
            guest_predicted_names=guest_names,
            neither_predicted_names=neither_names,
            main_ep_speakers=main_speakers,
            host_speaker_labels=host_speaker_labels,
            guest_speaker_labels=guest_speaker_labels,
            overlap_prop_duration=float(episode_dict.get('overlapPropDuration', 0)),
            overlap_prop_turn_count=float(episode_dict.get('overlapPropTurnCount', 0)),
            avg_turn_duration=float(episode_dict.get('avgTurnDuration', 0)),
            total_speaker_labels=float(episode_dict.get('totalSpLabels', 0)),
            language=episode_dict.get('language', 'en'),
            explicit=bool(episode_dict.get('explicit', 0)),
            image_url=episode_dict.get('imageUrl'),
            episode_date_localized=episode_dict.get('episodeDateLocalized'),
            oldest_episode_date=episode_dict.get('oldestEpisodeDate'),
            last_update=episode_dict.get('lastUpdate'),
            created_on=episode_dict.get('createdOn'),
        )

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
            mp3_url = turn.get('mp3url')
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

        for i, episode in enumerate(self._episodes):
            if i % 1000 == 0:
                logger.debug(f"  Processed {i:,} episodes... ({episodes_with_turns} with turns, {total_turns_loaded} total turns)")

            episode_turns = turns_by_episode.get(episode.mp3_url, [])
            if episode_turns:
                episodes_with_turns += 1
                total_turns_loaded += len(episode_turns)

            episode.load_turns(episode_turns)

        loading_time = time.time() - loading_start
        total_turns_time = time.time() - turns_start

        logger.info(f"✓ Turn loading completed in {loading_time:.2f} seconds")
        if len(self._episodes) > 0:
            logger.info(f"  Episodes with turns: {episodes_with_turns:,} / {len(self._episodes):,} ({episodes_with_turns/len(self._episodes)*100:.1f}%)")
        else:
            logger.info(f"  Episodes with turns: {episodes_with_turns:,} / {len(self._episodes):,} (no episodes to process)")
        logger.info(f"  Total turns loaded: {total_turns_loaded:,}")
        logger.info(f"  Total turn processing time: {total_turns_time:.2f} seconds")

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
                    podcast_title = record.get('podTitle', '')

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
            title=podcast_info['title'],
            description=podcast_info['description'],
            rss_url=podcast_info['rss_url'],
            language=podcast_info['language'],
            explicit=podcast_info['explicit'],
            image_url=podcast_info['image_url'],
            itunes_author=podcast_info['itunes_author'],
            itunes_owner_name=podcast_info['itunes_owner_name'],
            host=podcast_info['host'],
            created_on=podcast_info['created_on'],
            last_update=podcast_info['last_update'],
            oldest_episode_date=podcast_info['oldest_episode_date'],
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
                if record.get('mp3url') == episode.mp3_url:
                    turns_data.append(record)

        episode.load_turns(turns_data)

    def search_episodes(self, **criteria) -> List[Episode]:
        """
        Search for episodes based on various criteria.

        Args:
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
            return self._search_episodes_streaming(**criteria)

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

    def _search_episodes_streaming(self, **criteria) -> List[Episode]:
        """Search for episodes in streaming mode."""
        logger.info(f"Searching for episodes with criteria: {criteria}")

        matching_episodes = []

        # Use safe iterator to handle data type inconsistencies
        for record in self._create_safe_iterator(self._dataset):
            # Only process episode records
            if 'epTitle' in record or 'podTitle' in record:
                try:
                    episode = self._create_episode_from_dict(record)

                    if self._episode_matches_criteria(episode, criteria):
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

    def iterate_episodes(self) -> Iterator[Episode]:
        """
        Iterate over all episodes in the dataset (streaming or memory mode).
        """
        if self.streaming:
            logger.info("Iterating over episodes in streaming mode...")
            try:
                for record in self._create_safe_iterator(self._dataset):
                    try:
                        episode = self._create_episode_from_dict(record)
                        yield episode
                    except Exception as e:
                        logger.debug(f"Skipping episode due to processing error: {e}")
            except Exception as e:
                logger.error(f"Exception during episode iteration: {e}")
                raise
        else:
            logger.info("Iterating over episodes in memory mode...")
            for episode in self._episodes:
                yield episode

    def iterate_podcasts(self) -> Iterator[Podcast]:
        """Iterate over podcasts without loading them all into memory."""
        if not self.streaming:
            raise RuntimeError("iterate_podcasts() is only available in streaming mode")

        import time
        start_time = time.time()
        logger.info("Iterating over podcasts in streaming mode...")

        current_podcast = None
        current_podcast_title = None
        episode_count = 0
        podcast_count = 0
        skipped_count = 0
        last_progress_time = time.time()

        yielded_titles = set()
        # Use safe iterator to handle data type inconsistencies
        for record in self._create_safe_iterator(self._dataset):
            # Only process episode records
            if 'epTitle' in record or 'podTitle' in record:
                try:
                    podcast_title = record.get('podTitle', 'Unknown Podcast')

                    # Check if we've moved to a new podcast
                    if current_podcast_title is None or podcast_title != current_podcast_title:
                        # Yield the previous podcast if it exists and hasn't been yielded yet
                        if current_podcast is not None and current_podcast_title not in yielded_titles:
                            podcast_count += 1
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
                            description=record.get('podDescription', ''),
                            rss_url=record.get('rssUrl', ''),
                            language=record.get('language', 'en'),
                            explicit=bool(record.get('explicit', 0)),
                            image_url=record.get('imageUrl'),
                            itunes_author=record.get('itunesAuthor'),
                            itunes_owner_name=record.get('itunesOwnerName'),
                            host=record.get('host'),
                            created_on=record.get('createdOn'),
                            last_update=record.get('lastUpdate'),
                            oldest_episode_date=record.get('oldestEpisodeDate'),
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
            yield current_podcast
            yielded_titles.add(current_podcast_title)

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
            return self._get_dataset_statistics_streaming()

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
                    duration = float(record.get('durationSeconds', 0))
                    total_duration += duration
                    podcast_titles.add(record.get('podTitle', 'Unknown Podcast'))

                    # Count categories
                    for i in range(1, 11):
                        category = record.get(f'category{i}')
                        if category:
                            category_counts[category] = category_counts.get(category, 0) + 1

                    # Count languages
                    language = record.get('language', 'en')
                    language_counts[language] = language_counts.get(language, 0) + 1

                    # Count speaker counts
                    speaker_count = len(record.get('mainEpSpeakers', []))
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
            'speaker_count_distribution': speaker_count_distribution,
            'duration_distribution': duration_distribution,
        }

    def __len__(self) -> int:
        """Get the number of episodes in the dataset."""
        if self.streaming and not self._selective_mode:
            raise RuntimeError(
                "len() is not available in streaming mode unless a subset has been loaded. "
                "Use load_podcast_subset() first or iterate_episodes() to count manually."
            )
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
                    podcast_title = record.get('podTitle', 'Unknown Podcast')

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
                            description=record.get('podDescription', ''),
                            rss_url=record.get('rssUrl', ''),
                            language=record.get('language', 'en'),
                            explicit=bool(record.get('explicit', 0)),
                            image_url=record.get('imageUrl'),
                            itunes_author=record.get('itunesAuthor'),
                            itunes_owner_name=record.get('itunesOwnerName'),
                            host=record.get('host'),
                            created_on=record.get('createdOn'),
                            last_update=record.get('lastUpdate'),
                            oldest_episode_date=record.get('oldestEpisodeDate'),
                        )

                    # Check if this episode has the subcategory
                    has_subcategory = False
                    for i in range(1, 11):
                        category = record.get(f'category{i}')
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