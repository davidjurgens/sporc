"""
Podcast class for representing podcast collections.
"""

from typing import List, Optional, Dict, Any, Set
from dataclasses import dataclass, field
from datetime import datetime
import statistics

from .episode import Episode
from .exceptions import NotFoundError
from .constants import SUBCATEGORIES, MAIN_CATEGORIES


@dataclass
class Podcast:
    """
    Represents a podcast with its episodes and metadata.
    """

    # Basic podcast information
    title: str
    description: str
    rss_url: str

    # Episodes
    episodes: List[Episode] = field(default_factory=list)

    # Metadata
    language: str = "en"
    explicit: bool = False
    image_url: Optional[str] = None
    itunes_author: Optional[str] = None
    itunes_owner_name: Optional[str] = None
    host: Optional[str] = None
    created_on: Optional[int] = None
    last_update: Optional[int] = None
    oldest_episode_date: Optional[str] = None

    def __post_init__(self):
        """Validate podcast data after initialization."""
        if not self.title.strip():
            raise ValueError("Podcast title cannot be empty")
        if not self.rss_url.strip():
            raise ValueError("RSS URL cannot be empty")

    @property
    def num_episodes(self) -> int:
        """Get the number of episodes in this podcast."""
        return len(self.episodes)

    @property
    def host_names(self) -> List[str]:
        """Get all unique host names across all episodes."""
        host_names = set()
        for episode in self.episodes:
            host_names.update(episode.host_names)
        return sorted(list(host_names))

    @property
    def guest_names(self) -> List[str]:
        """Get all unique guest names across all episodes."""
        guest_names = set()
        for episode in self.episodes:
            guest_names.update(episode.guest_names)
        return sorted(list(guest_names))

    @property
    def categories(self) -> List[str]:
        """Get all unique categories across all episodes."""
        categories = set()
        for episode in self.episodes:
            categories.update(episode.categories)
        return sorted(list(categories))

    @property
    def subcategories(self) -> List[str]:
        """Get all unique subcategories across all episodes."""
        subcategories = set()
        for episode in self.episodes:
            for category in episode.categories:
                if category in SUBCATEGORIES:
                    subcategories.add(category)
        return sorted(list(subcategories))

    @property
    def main_categories(self) -> List[str]:
        """Get all unique main categories across all episodes."""
        main_categories = set()
        for episode in self.episodes:
            for category in episode.categories:
                if category in MAIN_CATEGORIES:
                    main_categories.add(category)
        return sorted(list(main_categories))

    @property
    def primary_category(self) -> Optional[str]:
        """Get the most common primary category across episodes."""
        if not self.episodes:
            return None

        category_counts = {}
        for episode in self.episodes:
            if episode.primary_category:
                category_counts[episode.primary_category] = category_counts.get(episode.primary_category, 0) + 1

        if not category_counts:
            return None

        return max(category_counts, key=category_counts.get)

    @property
    def primary_subcategory(self) -> Optional[str]:
        """Get the most common subcategory across episodes."""
        if not self.episodes:
            return None

        subcategory_counts = {}
        for episode in self.episodes:
            for category in episode.categories:
                if category in SUBCATEGORIES:
                    subcategory_counts[category] = subcategory_counts.get(category, 0) + 1

        if not subcategory_counts:
            return None

        return max(subcategory_counts, key=subcategory_counts.get)

    @property
    def total_duration_seconds(self) -> float:
        """Get the total duration of all episodes in seconds."""
        return sum(episode.duration_seconds for episode in self.episodes)

    @property
    def total_duration_hours(self) -> float:
        """Get the total duration of all episodes in hours."""
        return self.total_duration_seconds / 3600.0

    @property
    def avg_episode_duration_minutes(self) -> float:
        """Get the average episode duration in minutes."""
        if not self.episodes:
            return 0.0
        return sum(episode.duration_minutes for episode in self.episodes) / len(self.episodes)

    @property
    def shortest_episode(self) -> Optional[Episode]:
        """Get the shortest episode."""
        if not self.episodes:
            return None
        return min(self.episodes, key=lambda x: x.duration_seconds)

    @property
    def longest_episode(self) -> Optional[Episode]:
        """Get the longest episode."""
        if not self.episodes:
            return None
        return max(self.episodes, key=lambda x: x.duration_seconds)

    @property
    def earliest_episode_date(self) -> Optional[datetime]:
        """Get the date of the earliest episode."""
        dates = [episode.episode_date for episode in self.episodes if episode.episode_date]
        if not dates:
            return None
        return min(dates)

    @property
    def latest_episode_date(self) -> Optional[datetime]:
        """Get the date of the latest episode."""
        dates = [episode.episode_date for episode in self.episodes if episode.episode_date]
        if not dates:
            return None
        return max(dates)

    @property
    def solo_episodes(self) -> List[Episode]:
        """Get all solo episodes (single host, no guests)."""
        return [episode for episode in self.episodes if episode.is_solo]

    @property
    def interview_episodes(self) -> List[Episode]:
        """Get all interview episodes (host + guest)."""
        return [episode for episode in self.episodes if episode.is_interview]

    @property
    def panel_episodes(self) -> List[Episode]:
        """Get all panel episodes (multiple hosts/guests)."""
        return [episode for episode in self.episodes if episode.is_panel]

    @property
    def long_form_episodes(self) -> List[Episode]:
        """Get all long-form episodes (>30 minutes)."""
        return [episode for episode in self.episodes if episode.is_long_form]

    @property
    def short_form_episodes(self) -> List[Episode]:
        """Get all short-form episodes (<10 minutes)."""
        return [episode for episode in self.episodes if episode.is_short_form]

    def get_episodes_by_host(self, host_name: str) -> List[Episode]:
        """
        Get all episodes hosted by a specific person.

        Args:
            host_name: Name of the host to search for

        Returns:
            List of episodes hosted by the specified person
        """
        return [
            episode for episode in self.episodes
            if host_name in episode.host_names
        ]

    def get_episodes_by_guest(self, guest_name: str) -> List[Episode]:
        """
        Get all episodes featuring a specific guest.

        Args:
            guest_name: Name of the guest to search for

        Returns:
            List of episodes featuring the specified guest
        """
        return [
            episode for episode in self.episodes
            if guest_name in episode.guest_names
        ]

    def get_episodes_by_category(self, category: str) -> List[Episode]:
        """
        Get all episodes in a specific category.

        Args:
            category: Category to search for

        Returns:
            List of episodes in the specified category
        """
        return [
            episode for episode in self.episodes
            if category in episode.categories
        ]

    def get_episodes_by_subcategory(self, subcategory: str) -> List[Episode]:
        """
        Get all episodes in a specific subcategory.

        Args:
            subcategory: Subcategory to search for

        Returns:
            List of episodes in the specified subcategory
        """
        if subcategory not in SUBCATEGORIES:
            return []

        return [
            episode for episode in self.episodes
            if subcategory in episode.categories
        ]

    def get_episodes_by_duration_range(self, min_minutes: float = 0, max_minutes: float = float('inf')) -> List[Episode]:
        """
        Get episodes within a specific duration range.

        Args:
            min_minutes: Minimum duration in minutes
            max_minutes: Maximum duration in minutes

        Returns:
            List of episodes within the duration range
        """
        return [
            episode for episode in self.episodes
            if min_minutes <= episode.duration_minutes <= max_minutes
        ]

    def get_episodes_by_speaker_count(self, min_speakers: int = 0, max_speakers: int = float('inf')) -> List[Episode]:
        """
        Get episodes with a specific number of speakers.

        Args:
            min_speakers: Minimum number of speakers
            max_speakers: Maximum number of speakers

        Returns:
            List of episodes with the specified speaker count
        """
        return [
            episode for episode in self.episodes
            if min_speakers <= episode.num_main_speakers <= max_speakers
        ]

    def get_episodes_by_date_range(self, start_date: datetime, end_date: datetime) -> List[Episode]:
        """
        Get episodes within a specific date range.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of episodes within the date range
        """
        return [
            episode for episode in self.episodes
            if (episode.episode_date and
                start_date <= episode.episode_date <= end_date)
        ]

    def get_episode_by_title(self, title: str) -> Optional[Episode]:
        """
        Get an episode by its title.

        Args:
            title: Episode title to search for

        Returns:
            Episode with the specified title, or None if not found
        """
        for episode in self.episodes:
            if episode.title.lower() == title.lower():
                return episode
        return None

    def get_episode_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics about the podcast episodes.

        Returns:
            Dictionary with podcast statistics
        """
        if not self.episodes:
            return {
                'num_episodes': 0,
                'total_duration_hours': 0.0,
                'avg_episode_duration_minutes': 0.0,
                'episode_types': {},
                'category_distribution': {},
                'speaker_distribution': {},
                'date_range': None,
            }

        # Episode type distribution
        episode_types = {
            'solo': len(self.solo_episodes),
            'interview': len(self.interview_episodes),
            'panel': len(self.panel_episodes),
            'long_form': len(self.long_form_episodes),
            'short_form': len(self.short_form_episodes),
        }

        # Category distribution
        category_counts = {}
        for episode in self.episodes:
            for category in episode.categories:
                category_counts[category] = category_counts.get(category, 0) + 1

        # Speaker count distribution
        speaker_counts = {}
        for episode in self.episodes:
            speaker_count = episode.num_main_speakers
            speaker_counts[speaker_count] = speaker_counts.get(speaker_count, 0) + 1

        # Duration statistics
        durations = [episode.duration_minutes for episode in self.episodes]

        return {
            'num_episodes': self.num_episodes,
            'total_duration_hours': self.total_duration_hours,
            'avg_episode_duration_minutes': self.avg_episode_duration_minutes,
            'min_episode_duration_minutes': min(durations),
            'max_episode_duration_minutes': max(durations),
            'median_episode_duration_minutes': statistics.median(durations),
            'episode_types': episode_types,
            'category_distribution': category_counts,
            'speaker_distribution': speaker_counts,
            'host_names': self.host_names,
            'guest_names': self.guest_names,
            'date_range': {
                'earliest': self.earliest_episode_date.isoformat() if self.earliest_episode_date else None,
                'latest': self.latest_episode_date.isoformat() if self.latest_episode_date else None,
            },
        }

    def add_episode(self, episode: Episode) -> None:
        """
        Add an episode to this podcast.

        Args:
            episode: Episode to add
        """
        if episode.podcast_title != self.title:
            raise ValueError(f"Episode belongs to podcast '{episode.podcast_title}', not '{self.title}'")

        self.episodes.append(episode)
        # Sort episodes by date if available, otherwise by title
        self.episodes.sort(key=lambda x: (x.episode_date or datetime.min, x.title))

    def remove_episode(self, episode: Episode) -> None:
        """
        Remove an episode from this podcast.

        Args:
            episode: Episode to remove
        """
        if episode in self.episodes:
            self.episodes.remove(episode)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the podcast to a dictionary representation.

        Returns:
            Dictionary representation of the podcast
        """
        return {
            'title': self.title,
            'description': self.description,
            'rss_url': self.rss_url,
            'num_episodes': self.num_episodes,
            'total_duration_hours': self.total_duration_hours,
            'avg_episode_duration_minutes': self.avg_episode_duration_minutes,
            'host_names': self.host_names,
            'guest_names': self.guest_names,
            'categories': self.categories,
            'primary_category': self.primary_category,
            'language': self.language,
            'explicit': self.explicit,
            'image_url': self.image_url,
            'itunes_author': self.itunes_author,
            'itunes_owner_name': self.itunes_owner_name,
            'host': self.host,
            'date_range': {
                'earliest': self.earliest_episode_date.isoformat() if self.earliest_episode_date else None,
                'latest': self.latest_episode_date.isoformat() if self.latest_episode_date else None,
            },
            'episode_types': {
                'solo': len(self.solo_episodes),
                'interview': len(self.interview_episodes),
                'panel': len(self.panel_episodes),
                'long_form': len(self.long_form_episodes),
                'short_form': len(self.short_form_episodes),
            },
        }

    def __str__(self) -> str:
        """String representation of the podcast."""
        return f"Podcast('{self.title}', {self.num_episodes} episodes, {self.total_duration_hours:.1f}h)"

    def __repr__(self) -> str:
        """Detailed string representation of the podcast."""
        return (f"Podcast(title='{self.title}', num_episodes={self.num_episodes}, "
                f"total_duration_hours={self.total_duration_hours:.1f}, "
                f"host_names={self.host_names})")

    def __len__(self) -> int:
        """Return the number of episodes in this podcast."""
        return self.num_episodes

    def __iter__(self):
        """Iterate over episodes in chronological order."""
        return iter(self.episodes)

    def __getitem__(self, index):
        """Get an episode by index (chronological order)."""
        return self.episodes[index]