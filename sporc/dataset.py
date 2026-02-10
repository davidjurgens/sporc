"""
Main dataset class for working with the SPORC dataset.
"""

import logging
import random
from typing import List, Dict, Any, Optional, Iterator

from .podcast import Podcast
from .episode import Episode
from .exceptions import (
    SPORCError,
    DatasetAccessError,
    AuthenticationError,
    IndexNotBuiltError,
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

    All operations use the Parquet backend for fast O(1) lookups. If no local
    ``parquet_dir`` is given, the parquet files are automatically downloaded
    from HuggingFace using ``huggingface_hub.snapshot_download()``.
    """

    DATASET_ID = "blitt/SPoRC"

    def __init__(self, parquet_dir: Optional[str] = None,
                 use_auth_token: Optional[str] = None,
                 cache_dir: Optional[str] = None):
        """
        Initialize the SPORC dataset.

        Args:
            parquet_dir: Directory containing the partitioned Parquet layout.
                         If None, downloads from HuggingFace automatically.
            use_auth_token: Hugging Face token for authentication.
                            If None, uses cached credentials.
            cache_dir: Directory to cache the downloaded dataset.
                       If None, uses the default HuggingFace cache location.
        """
        if parquet_dir is None:
            from huggingface_hub import snapshot_download
            logger.info("Downloading SPORC parquet data from HuggingFace...")
            parquet_dir = snapshot_download(
                repo_id=self.DATASET_ID,
                repo_type="dataset",
                token=use_auth_token,
                cache_dir=cache_dir,
            )

        from .parquet_backend import ParquetBackend
        logger.info("Initializing Parquet backend from %s", parquet_dir)
        self._parquet_backend = ParquetBackend(parquet_dir)
        self._loaded = True

    # ------------------------------------------------------------------
    # Public search / retrieval API
    # ------------------------------------------------------------------

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
        pinfo = self._parquet_backend.get_podcast_by_name(name)
        return self._parquet_backend.build_podcast_object(pinfo["podcast_id"])

    def search_episodes(self, max_episodes: Optional[int] = None,
                        sampling_mode: str = "first", **criteria) -> List[Episode]:
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

        Returns:
            List of episodes matching the criteria
        """
        rows = self._parquet_backend.search_episodes(**criteria)
        episodes = []
        for row in rows:
            pid = row.get("podcast_id", "")
            eid = row.get("episode_id", "")
            try:
                ep = self._parquet_backend.build_episode_object(pid, eid)
                episodes.append(ep)
            except Exception as e:
                logger.debug("Skipping episode during search: %s", e)
        if max_episodes and len(episodes) > max_episodes:
            if sampling_mode == "first":
                episodes = episodes[:max_episodes]
            else:
                episodes = random.sample(episodes, max_episodes)
        return episodes

    def search_episodes_by_subcategory(self, subcategory: str,
                                       **additional_criteria) -> List[Episode]:
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
        pids = self._parquet_backend.get_podcasts_by_category(subcategory)
        podcasts = []
        for pid in pids:
            try:
                podcasts.append(self._parquet_backend.build_podcast_object(pid))
            except Exception as e:
                logger.debug("Skipping podcast during subcategory search: %s", e)
        return podcasts

    def get_all_podcasts(self) -> List[Podcast]:
        """
        Get all podcasts in the dataset.

        Returns:
            List of all Podcast objects
        """
        podcast_ids = self._parquet_backend._podcast_df["podcast_id"].tolist()
        podcasts = []
        for pid in podcast_ids:
            try:
                podcasts.append(self._parquet_backend.build_podcast_object(pid))
            except Exception as e:
                logger.debug("Skipping podcast: %s", e)
        return podcasts

    def get_all_episodes(self) -> List[Episode]:
        """
        Get all episodes in the dataset.

        Returns:
            List of all Episode objects
        """
        rows = self._parquet_backend.search_episodes()
        episodes = []
        for row in rows:
            try:
                ep = self._parquet_backend.build_episode_object(
                    row["podcast_id"], row["episode_id"]
                )
                episodes.append(ep)
            except Exception as e:
                logger.debug("Skipping episode: %s", e)
        return episodes

    def iterate_episodes(self, max_episodes: Optional[int] = None,
                         sampling_mode: str = "first") -> Iterator[Episode]:
        """
        Iterate over all episodes in the dataset.

        Args:
            max_episodes: Maximum number of episodes to yield (None for all)
            sampling_mode: How to sample episodes ("first" or "random")
        """
        rows = self._parquet_backend.search_episodes()
        if max_episodes and len(rows) > max_episodes:
            if sampling_mode == "first":
                rows = rows[:max_episodes]
            else:
                rows = random.sample(rows, max_episodes)
        for row in rows:
            try:
                ep = self._parquet_backend.build_episode_object(
                    row["podcast_id"], row["episode_id"]
                )
                yield ep
            except Exception as e:
                logger.debug("Skipping episode during iteration: %s", e)

    def iterate_podcasts(self, max_podcasts: Optional[int] = None,
                         sampling_mode: str = "first") -> Iterator[Podcast]:
        """
        Iterate over all podcasts in the dataset.

        Args:
            max_podcasts: Maximum number of podcasts to yield (None for all)
            sampling_mode: How to sample podcasts ("first" or "random")
        """
        podcast_ids = self._parquet_backend._podcast_df["podcast_id"].tolist()
        if max_podcasts and len(podcast_ids) > max_podcasts:
            if sampling_mode == "first":
                podcast_ids = podcast_ids[:max_podcasts]
            else:
                podcast_ids = random.sample(podcast_ids, max_podcasts)
        for pid in podcast_ids:
            try:
                yield self._parquet_backend.build_podcast_object(pid)
            except Exception as e:
                logger.debug("Skipping podcast during iteration: %s", e)

    def get_dataset_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics about the dataset.

        Returns:
            Dictionary with dataset statistics
        """
        return self._parquet_backend.get_statistics()

    def load_turns_for_episode(self, episode: Episode) -> None:
        """
        Load turn data for a specific episode on-demand.

        Args:
            episode: Episode to load turns for
        """
        if episode._turns_loaded:
            return
        if episode._turn_loader is not None:
            episode._turn_loader()

    def load_turns_for_podcast(self, podcast: Podcast) -> None:
        """
        Load turn data for all episodes in a podcast on-demand.

        Args:
            podcast: Podcast to load turns for
        """
        for episode in podcast.episodes:
            self.load_turns_for_episode(episode)

    # ------------------------------------------------------------------
    # Search, retrieval & precomputed index methods
    # ------------------------------------------------------------------

    def search_turns(self, query: str, *, mode: str = "fts",
                     podcast_id: Optional[str] = None,
                     episode_id: Optional[str] = None,
                     speaker_role: Optional[str] = None,
                     limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Search turn text across the corpus using full-text search.

        Args:
            query: Search query string.
            mode: "fts" (BM25 ranked), "exact" (ILIKE), or "regex".
            podcast_id: Filter to a specific podcast.
            episode_id: Filter to a specific episode.
            speaker_role: Filter by speaker role.
            limit: Maximum results to return.
            offset: Number of results to skip.

        Returns:
            List of turn dicts with score.
        """
        return self._parquet_backend.search_turns(
            query, mode=mode, podcast_id=podcast_id,
            episode_id=episode_id, speaker_role=speaker_role,
            limit=limit, offset=offset,
        )

    def search_episodes_by_text(self, query: str, *, mode: str = "fts",
                                limit: int = 100) -> List[Dict[str, Any]]:
        """
        Find episodes containing matching text.

        Args:
            query: Search query string.
            mode: "fts" (BM25 ranked), "exact" (ILIKE), or "regex".
            limit: Maximum number of episodes.

        Returns:
            List of dicts with episode_id, podcast_id, match_count, best_score.
        """
        return self._parquet_backend.search_episodes_by_text(
            query, mode=mode, limit=limit,
        )

    def search_by_speaker_name(self, name: str, *, role: Optional[str] = None,
                               exact: bool = False,
                               limit: int = 100) -> List[Dict[str, Any]]:
        """
        Find episodes featuring a speaker by name.

        Args:
            name: Speaker name to search for.
            role: Filter by role ("host"/"guest"/None).
            exact: Require exact name match.
            limit: Maximum results.

        Returns:
            List of dicts with episode_id, podcast_id, name_original, role.
        """
        return self._parquet_backend.search_by_speaker_name(
            name, role=role, exact=exact, limit=limit,
        )

    def concordance(self, word: str, *, context_words: int = 10,
                    speaker_role: Optional[str] = None,
                    podcast_id: Optional[str] = None,
                    limit: int = 100) -> List[Dict[str, Any]]:
        """
        Key Word In Context search.

        Args:
            word: Word or phrase to find.
            context_words: Number of context words on each side.
            speaker_role: Filter by speaker role.
            podcast_id: Filter to a specific podcast.
            limit: Maximum results.

        Returns:
            List of KWIC dicts with left_context, keyword, right_context,
            and metadata.
        """
        return self._parquet_backend.concordance(
            word, context_words=context_words,
            speaker_role=speaker_role, podcast_id=podcast_id,
            limit=limit,
        )

    def get_episode_metrics(self, episode_id: str) -> Optional[Dict[str, Any]]:
        """
        Get precomputed metrics for an episode.

        Args:
            episode_id: The episode to look up.

        Returns:
            Dict of metrics or None if not found.
        """
        return self._parquet_backend.get_episode_metrics(episode_id)

    def filter_episodes_by_metrics(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Filter episodes by precomputed metrics.

        Keyword Args:
            min_word_count, max_word_count, min_turn_count, max_turn_count,
            min_speaking_rate, max_speaking_rate,
            min_discourse_marker_rate, max_discourse_marker_rate,
            min_host_proportion, max_host_proportion,
            min_avg_gap, max_avg_gap, limit.

        Returns:
            List of episode metric dicts.
        """
        return self._parquet_backend.filter_episodes_by_metrics(**kwargs)

    def get_turn_metrics(self, podcast_id: str,
                         episode_id: str) -> List[Dict[str, Any]]:
        """
        Get precomputed turn-level metrics for an episode.

        Args:
            podcast_id: The podcast partition key.
            episode_id: The episode to look up.

        Returns:
            List of turn metric dicts sorted by turn_count.
        """
        return self._parquet_backend.get_turn_metrics(podcast_id, episode_id)

    def estimate_word_audio(self, podcast_id: str, episode_id: str,
                            word: str,
                            occurrence: int = 0) -> Optional[Dict[str, Any]]:
        """
        Estimate audio time range for a word occurrence in an episode.

        Args:
            podcast_id: The podcast partition key.
            episode_id: The episode to search in.
            word: The word to locate.
            occurrence: Which occurrence (0-indexed).

        Returns:
            Dict with mp3_url, estimated_start, estimated_end,
            turn_start, turn_end, turn_text, confidence.
        """
        return self._parquet_backend.estimate_word_audio(
            podcast_id, episode_id, word, occurrence=occurrence,
        )

    # ------------------------------------------------------------------
    # Dunder methods
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        """Get the number of episodes in the dataset."""
        return self._parquet_backend.num_episodes

    def __str__(self) -> str:
        """String representation of the dataset."""
        return (f"SPORCDataset(parquet, {self._parquet_backend.num_podcasts} podcasts, "
                f"{self._parquet_backend.num_episodes} episodes)")

    def __repr__(self) -> str:
        """Detailed string representation of the dataset."""
        return (f"SPORCDataset(parquet, podcasts={self._parquet_backend.num_podcasts}, "
                f"episodes={self._parquet_backend.num_episodes}, loaded=True)")
