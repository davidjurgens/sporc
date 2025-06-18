"""
Main dataset class for working with the SPORC dataset.
"""

import json
import logging
from typing import List, Dict, Any, Optional, Union, Iterator
from pathlib import Path
import warnings

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


class SPORCDataset:
    """
    Main class for working with the SPORC (Structured Podcast Open Research Corpus) dataset.

    This class provides access to the SPORC dataset hosted on Hugging Face and offers
    various search and filtering capabilities for podcasts and episodes.

    The dataset can be loaded in three modes:
    - **Memory mode** (default): Loads all data into memory for fast access
    - **Streaming mode**: Loads data on-demand to reduce memory usage
    - **Selective mode**: Filters and loads specific podcasts into memory for O(1) operations
    """

    DATASET_ID = "blitt/SPoRC"
    EPISODE_SPLIT = "episodeLevelDataSample"
    SPEAKER_TURN_SPLIT = "speakerTurnDataSample"

    def __init__(self, cache_dir: Optional[str] = None, use_auth_token: Optional[str] = None,
                 streaming: bool = False):
        """
        Initialize the SPORC dataset.

        Args:
            cache_dir: Directory to cache the dataset. If None, uses default cache location.
            use_auth_token: Hugging Face authentication token. If None, uses default authentication.
            streaming: If True, use streaming mode to load data on-demand. If False, load all data into memory.

        Raises:
            AuthenticationError: If Hugging Face authentication fails
            DatasetAccessError: If the dataset cannot be accessed
        """
        self.cache_dir = cache_dir
        self.use_auth_token = use_auth_token
        self.streaming = streaming

        # Initialize data storage
        self._episode_data: Optional[Union[Dataset, IterableDataset]] = None
        self._speaker_turn_data: Optional[Union[Dataset, IterableDataset]] = None
        self._podcasts: Dict[str, Podcast] = {}
        self._episodes: List[Episode] = []
        self._loaded = False
        self._selective_mode = False  # Track if we're in selective mode

        # Load the dataset
        self._load_dataset()

    def _load_dataset(self) -> None:
        """Load the SPORC dataset from Hugging Face."""
        try:
            logger.info(f"Loading SPORC dataset from Hugging Face (streaming={self.streaming})...")

            # Load episode-level data
            self._episode_data = load_dataset(
                self.DATASET_ID,
                split=self.EPISODE_SPLIT,
                cache_dir=self.cache_dir,
                use_auth_token=self.use_auth_token,
                trust_remote_code=True,
                streaming=self.streaming
            )

            # Load speaker turn data
            self._speaker_turn_data = load_dataset(
                self.DATASET_ID,
                split=self.SPEAKER_TURN_SPLIT,
                cache_dir=self.cache_dir,
                use_auth_token=self.use_auth_token,
                trust_remote_code=True,
                streaming=self.streaming
            )

            if self.streaming:
                logger.info("Loaded dataset in streaming mode - data will be loaded on-demand")
            else:
                logger.info(f"Loaded {len(self._episode_data)} episodes and {len(self._speaker_turn_data)} speaker turns")

            # Process the data
            self._process_data()

        except Exception as e:
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
            else:
                raise DatasetAccessError(f"Failed to load dataset: {e}") from e

    def _process_data(self) -> None:
        """Process the loaded dataset into Podcast and Episode objects."""
        if self.streaming:
            logger.info("Dataset loaded in streaming mode - no data processed yet")
            self._loaded = True
            return

        logger.info("Processing dataset...")

        # Convert speaker turn data to list for easier processing
        speaker_turns = list(self._speaker_turn_data)

        # Group episodes by podcast
        podcast_groups: Dict[str, List[Dict[str, Any]]] = {}

        for episode_dict in self._episode_data:
            podcast_title = episode_dict.get('podTitle', 'Unknown Podcast')
            if podcast_title not in podcast_groups:
                podcast_groups[podcast_title] = []
            podcast_groups[podcast_title].append(episode_dict)

        # Create Podcast and Episode objects
        for podcast_title, episode_dicts in podcast_groups.items():
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

            self._podcasts[podcast_title] = podcast

        # Load turns for all episodes
        self._load_turns_for_episodes(speaker_turns)

        self._loaded = True
        logger.info(f"Processed {len(self._podcasts)} podcasts with {len(self._episodes)} total episodes")

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

        logger.info(f"Loading podcast subset with criteria: {criteria}")

        # Clear existing data
        self._podcasts.clear()
        self._episodes.clear()
        self._selective_mode = True

        # Convert speaker turn data to list for easier processing
        speaker_turns = list(self._speaker_turn_data)

        # Group episodes by podcast and apply filters
        podcast_groups: Dict[str, List[Dict[str, Any]]] = {}
        podcast_metadata: Dict[str, Dict[str, Any]] = {}

        # First pass: collect all episodes and group by podcast
        for episode_dict in self._episode_data:
            podcast_title = episode_dict.get('podTitle', 'Unknown Podcast')

            if podcast_title not in podcast_groups:
                podcast_groups[podcast_title] = []
                podcast_metadata[podcast_title] = {
                    'episodes': [],
                    'categories': set(),
                    'hosts': set(),
                    'total_duration': 0.0,
                    'language': episode_dict.get('language', 'en'),
                    'explicit': bool(episode_dict.get('explicit', 0))
                }

            podcast_groups[podcast_title].append(episode_dict)
            podcast_metadata[podcast_title]['episodes'].append(episode_dict)
            podcast_metadata[podcast_title]['total_duration'] += float(episode_dict.get('durationSeconds', 0))

            # Collect categories
            for i in range(1, 11):
                category = episode_dict.get(f'category{i}')
                if category:
                    podcast_metadata[podcast_title]['categories'].add(category)

            # Collect hosts
            host_names = episode_dict.get('hostPredictedNames', [])
            if isinstance(host_names, str):
                if host_names != "NO_HOST_PREDICTED":
                    podcast_metadata[podcast_title]['hosts'].add(host_names)
            else:
                podcast_metadata[podcast_title]['hosts'].update(host_names)

        # Second pass: apply filters
        filtered_podcasts = {}
        for podcast_title, episodes in podcast_groups.items():
            metadata = podcast_metadata[podcast_title]

            # Apply filters
            if not self._podcast_matches_criteria(podcast_title, metadata, criteria):
                continue

            filtered_podcasts[podcast_title] = episodes

        # Third pass: create Podcast and Episode objects for filtered podcasts
        for podcast_title, episode_dicts in filtered_podcasts.items():
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

            self._podcasts[podcast_title] = podcast

        # Load turns for selected episodes
        self._load_turns_for_episodes(speaker_turns)

        self._loaded = True
        logger.info(f"Loaded {len(self._podcasts)} podcasts with {len(self._episodes)} total episodes in selective mode")

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
        logger.info("Loading turn data for episodes...")

        # Group turns by episode URL
        turns_by_episode: Dict[str, List[Dict[str, Any]]] = {}
        for turn in speaker_turns:
            mp3_url = turn.get('mp3url')
            if mp3_url:
                if mp3_url not in turns_by_episode:
                    turns_by_episode[mp3_url] = []
                turns_by_episode[mp3_url].append(turn)

        # Load turns for each episode
        for episode in self._episodes:
            episode_turns = turns_by_episode.get(episode.mp3_url, [])
            episode.load_turns(episode_turns)

        logger.info(f"Loaded turns for {len(self._episodes)} episodes")

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

        # Search through episodes to find the podcast
        for episode_dict in self._episode_data:
            podcast_title = episode_dict.get('podTitle', 'Unknown Podcast')

            # Check if this episode belongs to the requested podcast
            if (name.lower() == podcast_title.lower() or
                name.lower() in podcast_title.lower()):

                # Create podcast object
                podcast = Podcast(
                    title=podcast_title,
                    description=episode_dict.get('podDescription', ''),
                    rss_url=episode_dict.get('rssUrl', ''),
                    language=episode_dict.get('language', 'en'),
                    explicit=bool(episode_dict.get('explicit', 0)),
                    image_url=episode_dict.get('imageUrl'),
                    itunes_author=episode_dict.get('itunesAuthor'),
                    itunes_owner_name=episode_dict.get('itunesOwnerName'),
                    host=episode_dict.get('host'),
                    created_on=episode_dict.get('createdOn'),
                    last_update=episode_dict.get('lastUpdate'),
                    oldest_episode_date=episode_dict.get('oldestEpisodeDate'),
                )

                # Add this episode
                episode = self._create_episode_from_dict(episode_dict)
                podcast.add_episode(episode)

                # Load turns for this episode
                self._load_turns_for_episode_streaming(episode)

                return podcast

        raise NotFoundError(f"Podcast '{name}' not found")

    def _load_turns_for_episode_streaming(self, episode: Episode) -> None:
        """Load turn data for a single episode in streaming mode."""
        turns_data = []
        for turn_dict in self._speaker_turn_data:
            if turn_dict.get('mp3url') == episode.mp3_url:
                turns_data.append(turn_dict)

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

    def _search_episodes_streaming(self, **criteria) -> List[Episode]:
        """Search for episodes in streaming mode."""
        logger.info(f"Searching for episodes with criteria {criteria} in streaming mode...")

        episodes = []

        for episode_dict in self._episode_data:
            # Create episode object
            episode = self._create_episode_from_dict(episode_dict)

            # Apply filters
            if self._episode_matches_criteria(episode, criteria):
                # Load turns for this episode
                self._load_turns_for_episode_streaming(episode)
                episodes.append(episode)

        return episodes

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
        logger.info("Getting all podcasts in streaming mode...")

        podcast_dict: Dict[str, Podcast] = {}

        for episode_dict in self._episode_data:
            podcast_title = episode_dict.get('podTitle', 'Unknown Podcast')

            if podcast_title not in podcast_dict:
                # Create new podcast
                podcast = Podcast(
                    title=podcast_title,
                    description=episode_dict.get('podDescription', ''),
                    rss_url=episode_dict.get('rssUrl', ''),
                    language=episode_dict.get('language', 'en'),
                    explicit=bool(episode_dict.get('explicit', 0)),
                    image_url=episode_dict.get('imageUrl'),
                    itunes_author=episode_dict.get('itunesAuthor'),
                    itunes_owner_name=episode_dict.get('itunesOwnerName'),
                    host=episode_dict.get('host'),
                    created_on=episode_dict.get('createdOn'),
                    last_update=episode_dict.get('lastUpdate'),
                    oldest_episode_date=episode_dict.get('oldestEpisodeDate'),
                )
                podcast_dict[podcast_title] = podcast

            # Add episode to podcast
            episode = self._create_episode_from_dict(episode_dict)
            podcast_dict[podcast_title].add_episode(episode)

            # Load turns for this episode
            self._load_turns_for_episode_streaming(episode)

        return list(podcast_dict.values())

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
        logger.info("Getting all episodes in streaming mode...")

        episodes = []

        for episode_dict in self._episode_data:
            episode = self._create_episode_from_dict(episode_dict)
            self._load_turns_for_episode_streaming(episode)
            episodes.append(episode)

        return episodes

    def iterate_episodes(self) -> Iterator[Episode]:
        """
        Iterate over episodes one at a time (streaming mode only).

        This method is only available in streaming mode and allows you to
        process episodes one at a time without loading all episodes into memory.

        Returns:
            Iterator over Episode objects

        Raises:
            RuntimeError: If not in streaming mode
        """
        if not self.streaming:
            raise RuntimeError("iterate_episodes() is only available in streaming mode")

        for episode_dict in self._episode_data:
            episode = self._create_episode_from_dict(episode_dict)
            self._load_turns_for_episode_streaming(episode)
            yield episode

    def iterate_podcasts(self) -> Iterator[Podcast]:
        """
        Iterate over podcasts one at a time (streaming mode only).

        This method is only available in streaming mode and allows you to
        process podcasts one at a time without loading all podcasts into memory.

        Returns:
            Iterator over Podcast objects

        Raises:
            RuntimeError: If not in streaming mode
        """
        if not self.streaming:
            raise RuntimeError("iterate_podcasts() is only available in streaming mode")

        podcast_dict: Dict[str, Podcast] = {}

        for episode_dict in self._episode_data:
            podcast_title = episode_dict.get('podTitle', 'Unknown Podcast')

            if podcast_title not in podcast_dict:
                # Create new podcast
                podcast = Podcast(
                    title=podcast_title,
                    description=episode_dict.get('podDescription', ''),
                    rss_url=episode_dict.get('rssUrl', ''),
                    language=episode_dict.get('language', 'en'),
                    explicit=bool(episode_dict.get('explicit', 0)),
                    image_url=episode_dict.get('imageUrl'),
                    itunes_author=episode_dict.get('itunesAuthor'),
                    itunes_owner_name=episode_dict.get('itunesOwnerName'),
                    host=episode_dict.get('host'),
                    created_on=episode_dict.get('createdOn'),
                    last_update=episode_dict.get('lastUpdate'),
                    oldest_episode_date=episode_dict.get('oldestEpisodeDate'),
                )
                podcast_dict[podcast_title] = podcast

                # Yield the podcast when we first encounter it
                yield podcast

            # Add episode to podcast
            episode = self._create_episode_from_dict(episode_dict)
            podcast_dict[podcast_title].add_episode(episode)

            # Load turns for this episode
            self._load_turns_for_episode_streaming(episode)

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
        total_duration_seconds = 0.0
        category_counts = {}
        language_counts = {}
        speaker_counts = {}
        episode_types = {
            'solo': 0,
            'interview': 0,
            'panel': 0,
            'long_form': 0,
            'short_form': 0,
        }

        podcast_titles = set()

        for episode_dict in self._episode_data:
            total_episodes += 1
            podcast_titles.add(episode_dict.get('podTitle', 'Unknown Podcast'))

            duration = float(episode_dict.get('durationSeconds', 0))
            total_duration_seconds += duration

            # Categories
            for i in range(1, 11):
                category = episode_dict.get(f'category{i}')
                if category:
                    category_counts[category] = category_counts.get(category, 0) + 1

            # Language
            language = episode_dict.get('language', 'en')
            language_counts[language] = language_counts.get(language, 0) + 1

            # Speaker count
            main_speakers = episode_dict.get('mainEpSpeakers', [])
            if isinstance(main_speakers, str):
                if main_speakers == "SPEAKER_DATA_UNAVAILABLE":
                    speaker_count = 0
                else:
                    speaker_count = 1
            else:
                speaker_count = len(main_speakers)

            speaker_counts[speaker_count] = speaker_counts.get(speaker_count, 0) + 1

            # Episode types (simplified calculation)
            host_names = episode_dict.get('hostPredictedNames', [])
            guest_names = episode_dict.get('guestPredictedNames', [])

            if isinstance(host_names, str):
                host_names = [] if host_names == "NO_HOST_PREDICTED" else [host_names]
            if isinstance(guest_names, str):
                guest_names = [] if guest_names == "NO_GUEST_PREDICTED" else [guest_names]

            total_speakers = len(host_names) + len(guest_names)

            if total_speakers == 1:
                episode_types['solo'] += 1
            elif total_speakers == 2:
                episode_types['interview'] += 1
            else:
                episode_types['panel'] += 1

            if duration > 1800:  # 30 minutes
                episode_types['long_form'] += 1
            elif duration < 600:  # 10 minutes
                episode_types['short_form'] += 1

        total_duration_hours = total_duration_seconds / 3600.0
        avg_duration_minutes = (total_duration_seconds / 60.0) / total_episodes if total_episodes > 0 else 0.0

        return {
            'total_podcasts': len(podcast_titles),
            'total_episodes': total_episodes,
            'total_duration_hours': total_duration_hours,
            'avg_episode_duration_minutes': avg_duration_minutes,
            'category_distribution': category_counts,
            'language_distribution': language_counts,
            'speaker_distribution': speaker_counts,
            'episode_types': episode_types,
        }

    def __len__(self) -> int:
        """Return the number of episodes in the dataset."""
        if self.streaming and not self._selective_mode:
            # In streaming mode, we can't easily get the length without iterating
            # This would be expensive, so we'll raise an error
            raise RuntimeError("len() is not available in streaming mode. Use iterate_episodes() instead.")
        return len(self._episodes)

    def __str__(self) -> str:
        """String representation of the dataset."""
        if self.streaming:
            if self._selective_mode:
                return f"SPORCDataset(streaming=True, selective=True, {len(self._podcasts)} podcasts, {len(self._episodes)} episodes)"
            return f"SPORCDataset(streaming=True)"
        return f"SPORCDataset({len(self._podcasts)} podcasts, {len(self._episodes)} episodes)"

    def __repr__(self) -> str:
        """Detailed string representation of the dataset."""
        if self.streaming:
            if self._selective_mode:
                return f"SPORCDataset(streaming=True, selective=True, podcasts={len(self._podcasts)}, episodes={len(self._episodes)}, loaded={self._loaded})"
            return f"SPORCDataset(streaming=True, loaded={self._loaded})"
        return (f"SPORCDataset(podcasts={len(self._podcasts)}, episodes={len(self._episodes)}, "
                f"loaded={self._loaded})")