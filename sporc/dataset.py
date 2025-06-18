"""
Main dataset class for working with the SPORC dataset.
"""

import json
import logging
from typing import List, Dict, Any, Optional, Union
from pathlib import Path
import warnings

try:
    from datasets import load_dataset, Dataset
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
    """

    DATASET_ID = "blitt/SPoRC"
    EPISODE_SPLIT = "episodeLevelDataSample"
    SPEAKER_TURN_SPLIT = "speakerTurnDataSample"

    def __init__(self, cache_dir: Optional[str] = None, use_auth_token: Optional[str] = None):
        """
        Initialize the SPORC dataset.

        Args:
            cache_dir: Directory to cache the dataset. If None, uses default cache location.
            use_auth_token: Hugging Face authentication token. If None, uses default authentication.

        Raises:
            AuthenticationError: If Hugging Face authentication fails
            DatasetAccessError: If the dataset cannot be accessed
        """
        self.cache_dir = cache_dir
        self.use_auth_token = use_auth_token

        # Initialize data storage
        self._episode_data: Optional[Dataset] = None
        self._speaker_turn_data: Optional[Dataset] = None
        self._podcasts: Dict[str, Podcast] = {}
        self._episodes: List[Episode] = []
        self._loaded = False

        # Load the dataset
        self._load_dataset()

    def _load_dataset(self) -> None:
        """Load the SPORC dataset from Hugging Face."""
        try:
            logger.info("Loading SPORC dataset from Hugging Face...")

            # Load episode-level data
            self._episode_data = load_dataset(
                self.DATASET_ID,
                split=self.EPISODE_SPLIT,
                cache_dir=self.cache_dir,
                use_auth_token=self.use_auth_token,
                trust_remote_code=True
            )

            # Load speaker turn data
            self._speaker_turn_data = load_dataset(
                self.DATASET_ID,
                split=self.SPEAKER_TURN_SPLIT,
                cache_dir=self.cache_dir,
                use_auth_token=self.use_auth_token,
                trust_remote_code=True
            )

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

    def get_all_podcasts(self) -> List[Podcast]:
        """
        Get all podcasts in the dataset.

        Returns:
            List of all Podcast objects
        """
        if not self._loaded:
            raise RuntimeError("Dataset not loaded. Call _load_dataset() first.")

        return list(self._podcasts.values())

    def get_all_episodes(self) -> List[Episode]:
        """
        Get all episodes in the dataset.

        Returns:
            List of all Episode objects
        """
        if not self._loaded:
            raise RuntimeError("Dataset not loaded. Call _load_dataset() first.")

        return self._episodes.copy()

    def get_dataset_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics about the dataset.

        Returns:
            Dictionary with dataset statistics
        """
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

    def __len__(self) -> int:
        """Return the number of episodes in the dataset."""
        return len(self._episodes)

    def __str__(self) -> str:
        """String representation of the dataset."""
        return f"SPORCDataset({len(self._podcasts)} podcasts, {len(self._episodes)} episodes)"

    def __repr__(self) -> str:
        """Detailed string representation of the dataset."""
        return (f"SPORCDataset(podcasts={len(self._podcasts)}, episodes={len(self._episodes)}, "
                f"loaded={self._loaded})")