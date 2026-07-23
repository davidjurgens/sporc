"""
Main dataset class for working with the SPORC dataset.
"""

import contextlib
import json
import logging
import os
import random
import re
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

# Ids are fixed-width md5 prefixes (see the dataset manifest), which lets a bare
# list of strings be sorted into ids and titles without the caller labelling it.
_PODCAST_ID_RE = re.compile(r"^[0-9a-f]{12}$")
_EPISODE_ID_RE = re.compile(r"^[0-9a-f]{16}$")

# Concurrent downloads used by prefetch(). The Hub allows a few thousand
# requests per five-minute window across everything a token is doing, and each
# download costs about two of them. Since 1.1 a prefetch resolves to a few large
# part files rather than thousands of small ones, so there is nothing to gain
# from a wide pool: bandwidth, not latency, is the limit now.
_PREFETCH_WORKERS = 4


def _normalize_subset(subset: Any) -> Dict[str, List[str]]:
    """
    Turn a subset spec into ``{podcast_ids, podcast_titles, episode_ids}``.

    Accepts a dict with any of those keys, a list of ids/titles, a single
    string, or a path to a ``.json`` or newline-delimited text file holding
    either. Bare strings are classified by shape: 12 hex chars is a podcast id,
    16 hex chars an episode id, anything else a podcast title.
    """
    if isinstance(subset, (str, os.PathLike)) and os.path.exists(subset):
        with open(subset) as f:
            text = f.read()
        if str(subset).endswith(".json"):
            subset = json.loads(text)
        else:
            subset = [
                line.strip() for line in text.splitlines()
                if line.strip() and not line.startswith("#")
            ]

    if isinstance(subset, str):
        subset = [subset]

    empty: Dict[str, List[str]] = {
        "podcast_ids": [], "podcast_titles": [], "episode_ids": [],
    }

    if isinstance(subset, dict):
        unknown = set(subset) - set(empty)
        if unknown:
            raise ValueError(
                f"Unknown subset keys: {sorted(unknown)}. "
                f"Expected any of {sorted(empty)}."
            )
        return {k: [str(v) for v in subset.get(k, [])] for k in empty}

    if isinstance(subset, (list, tuple, set)):
        out = dict(empty)
        for item in subset:
            s = str(item).strip()
            if not s:
                continue
            if _PODCAST_ID_RE.match(s):
                out["podcast_ids"].append(s)
            elif _EPISODE_ID_RE.match(s):
                out["episode_ids"].append(s)
            else:
                out["podcast_titles"].append(s)
        return out

    raise ValueError(
        f"Unsupported subset spec of type {type(subset).__name__}. Pass a dict, "
        "a list of ids/titles, or a path to a .json or .txt file."
    )


class SPORCDataset:
    """
    Main class for working with the SPORC (Structured Podcast Open Research Corpus) dataset.

    This class provides access to the SPORC dataset hosted on Hugging Face and offers
    various search and filtering capabilities for podcasts and episodes.

    All operations use the Parquet backend for fast O(1) lookups. If no local
    ``parquet_dir`` is given, data comes from HuggingFace: the metadata catalogs
    are downloaded up front (~195 MB) and each podcast's partitions are fetched
    the first time they are touched, so only the data actually used is
    transferred. See ``__init__`` for pinning a run to a fixed subset or
    disabling downloads entirely.

    Example:
        >>> sporc = SPORCDataset(subset=["Radiolab"], allow_downloads=False)
        >>> podcast = sporc.search_podcast("Radiolab")
    """

    DATASET_ID = "blitt/SPoRC"

    # Pre-1.0 jsonlines exports. Still hosted on HuggingFace (~23 GB) but never
    # read by this package, which is Parquet-only.
    _LEGACY_PATTERNS = ["*.jsonl.gz"]

    # Full-text search index (~14 GB). Only needed by search_turns(),
    # search_episodes_by_text() and concordance(), so it is opt-in.
    _SEARCH_DB_PATTERN = "metadata/turns_search.duckdb"

    # The turn text the search index deliberately omits (~19 GB). Ranked search
    # works without it, returning turn identifiers; substring and regex search
    # need it, and fall back to scanning local Parquet when it is absent.
    _TEXT_DB_PATTERN = "metadata/turns_text.duckdb"

    # Catalogs needed before anything can be looked up. Listed explicitly rather
    # than globbed: snapshot_download(allow_patterns=...) enumerates every file
    # in the repo to match them, whereas fetching known names directly costs one
    # request each.
    _CORE_METADATA = [
        "manifest.json",
        "metadata/podcast_catalog.parquet",
        "metadata/episode_catalog.parquet",
        "metadata/category_index.parquet",
        "metadata/hostname_index.parquet",
        # Required from dataset 1.1: without it no podcast can be located
        # inside the part files.
        "metadata/shard_map.parquet",
    ]

    # Built by scripts/build_indexes.py. Absent from older revisions of the
    # dataset, in which case the features that need them raise on use.
    #
    # The host indexes are small (~14 MB together) and answer host lookups
    # straight from the catalog metadata, so fetching them here means
    # get_podcasts_by_host()/search_by_host() work out of the box -- and
    # offline, under allow_downloads=False -- instead of each call reaching back
    # to the Hub for a file that was never downloaded.
    _OPTIONAL_METADATA = [
        "metadata/speaker_name_index.parquet",
        "metadata/episode_metrics.parquet",
        "metadata/host_index.parquet",
        "metadata/host_episode_index.parquet",
    ]

    @classmethod
    def _download_metadata(cls, token: Optional[str], cache_dir: Optional[str],
                           include_search_db: bool,
                           include_turn_text: bool = False) -> str:
        """
        Fetch the metadata catalogs and return the snapshot directory holding
        them.

        Returns:
            Local snapshot root that repo-relative paths resolve against.

        Raises:
            DatasetAccessError: if a catalog the package cannot work without is
                missing from the dataset.
        """
        from huggingface_hub import hf_hub_download
        from huggingface_hub.errors import EntryNotFoundError

        wanted = list(cls._CORE_METADATA) + list(cls._OPTIONAL_METADATA)
        if include_search_db:
            wanted.append(cls._SEARCH_DB_PATTERN)
        if include_turn_text:
            wanted.append(cls._TEXT_DB_PATTERN)

        root = None
        for rel in wanted:
            try:
                path = hf_hub_download(
                    repo_id=cls.DATASET_ID, repo_type="dataset", filename=rel,
                    token=token, cache_dir=cache_dir,
                )
            except EntryNotFoundError:
                if rel in cls._CORE_METADATA:
                    raise DatasetAccessError(
                        f"{cls.DATASET_ID} is missing required metadata file "
                        f"{rel!r}. The dataset layout may have changed."
                    )
                logger.warning(
                    "Optional index %s is not in the dataset; features that "
                    "need it will raise IndexNotBuiltError.", rel,
                )
                continue
            if rel == "manifest.json":
                root = os.path.dirname(path)

        if root is None:
            raise DatasetAccessError(
                f"Could not locate manifest.json in {cls.DATASET_ID}."
            )
        return root

    def __init__(self, parquet_dir: Optional[str] = None,
                 use_auth_token: Optional[str] = None,
                 cache_dir: Optional[str] = None,
                 lazy: bool = True,
                 subset: Optional[Any] = None,
                 allow_downloads: bool = True,
                 include_search_db: bool = False,
                 include_turn_text: bool = False,
                 load_audio_features: bool = False,
                 ignore_patterns: Optional[List[str]] = None):
        """
        Initialize the SPORC dataset.

        By default only the metadata catalogs are downloaded (~195 MB) and
        per-podcast data is fetched as it is touched. The layout is partitioned
        by podcast, so working with a handful of podcasts costs a handful of
        small files rather than the whole ~57 GB corpus.

        Args:
            parquet_dir: Directory containing the partitioned Parquet layout.
                         If given, nothing is ever downloaded.
                         If None, data comes from HuggingFace.
            use_auth_token: Hugging Face token. If None, uses cached credentials.
            cache_dir: HuggingFace cache location. If None, uses the default.
            lazy: Fetch per-podcast partitions on demand (default). Set False to
                  download the whole corpus up front (~43 GB across ~540 files).
            subset: Data to fetch up front, so that later access needs no
                    network. Accepts a list of podcast ids or titles, a dict
                    with ``podcast_ids`` / ``podcast_titles`` / ``episode_ids``
                    keys, or a path to a JSON or newline-delimited text file
                    holding either.
            load_audio_features: Join acoustic features onto every turn.
                          Off by default: the acoustics are a separate 14.5 GB
                          tree, and reading a podcast's turns would pull its
                          acoustics part too even when nothing reads an MFCC --
                          the difference between 27 GB and 40 GB of downloads
                          for the tutorial workload. Until you set this True,
                          Turn's audio fields are all None and
                          get_audio_features() returns {}.
            allow_downloads: When False, never fetch anything beyond what is
                             already local; requests for absent data raise
                             ``DataNotLocalError``. Combine with ``subset`` to
                             pin a run to an exact slice of the corpus.
            include_search_db: Download the ~14 GB full-text search database,
                               needed by search_turns(),
                               search_episodes_by_text() and concordance().
            include_turn_text: Also download the ~19 GB turn-text database.
                               Ranked search works without it but returns turn
                               identifiers rather than the matching text;
                               substring and regex search need it and otherwise
                               scan whatever Parquet is local.
            ignore_patterns: Override download exclusions for the non-lazy path.
                             Passed through to ``snapshot_download``.

        Raises:
            ValueError: if ``subset`` is given without any resolvable entries.
        """
        source = None

        if parquet_dir is None:
            if lazy:
                logger.info("Downloading SPORC metadata catalogs (~195 MB)...")
                parquet_dir = self._download_metadata(
                    token=use_auth_token, cache_dir=cache_dir,
                    include_search_db=include_search_db,
                    include_turn_text=include_turn_text,
                )
                from .source import HubDataSource
                source = HubDataSource(
                    repo_id=self.DATASET_ID,
                    root=parquet_dir,
                    token=use_auth_token,
                    cache_dir=cache_dir,
                    allow_downloads=allow_downloads,
                )
            else:
                from huggingface_hub import snapshot_download

                if ignore_patterns is None:
                    ignore_patterns = list(self._LEGACY_PATTERNS)
                    if not include_search_db:
                        ignore_patterns.append(self._SEARCH_DB_PATTERN)
                    if not include_turn_text:
                        ignore_patterns.append(self._TEXT_DB_PATTERN)
                logger.info(
                    "Downloading the full SPORC corpus from HuggingFace "
                    "(this is large and may take a while; excluding: %s)",
                    ", ".join(ignore_patterns) or "nothing",
                )
                parquet_dir = snapshot_download(
                    repo_id=self.DATASET_ID,
                    repo_type="dataset",
                    token=use_auth_token,
                    cache_dir=cache_dir,
                    ignore_patterns=ignore_patterns,
                )

        from .parquet_backend import ParquetBackend
        logger.info("Initializing Parquet backend from %s", parquet_dir)
        self._parquet_backend = ParquetBackend(
            parquet_dir, source=source,
            load_audio_features=load_audio_features)
        self._loaded = True

        if subset is not None:
            self.prefetch(subset)

    # ------------------------------------------------------------------
    # Prefetching
    # ------------------------------------------------------------------

    def prefetch(self, subset: Any) -> Dict[str, Any]:
        """
        Download the data for a subset of the corpus up front.

        Resolves the subset to podcasts and fetches their partitions, so later
        access to them needs no network. Runs even when the dataset was created
        with ``allow_downloads=False``: pinning a run to a slice of the corpus
        means fetching that slice and nothing else.

        Args:
            subset: A list of podcast ids/titles or episode ids, a dict with
                    ``podcast_ids`` / ``podcast_titles`` / ``episode_ids``, or a
                    path to a JSON or newline-delimited file holding either.

        Returns:
            Dict with ``podcasts`` (count resolved), ``files`` (count now
            available locally) and ``unresolved`` (entries not found).

            ``files`` is not a download count: it counts partitions present
            afterwards, so already-cached files and a local ``parquet_dir``
            both report nonzero without fetching anything.

        Raises:
            ValueError: if the subset resolves to no podcasts at all.
        """
        spec = _normalize_subset(subset)
        backend = self._parquet_backend

        pids: List[str] = []
        unresolved: List[str] = []

        # Checked against the catalog rather than trusted: a subset file is
        # hand-written, and a typo'd id should be reported, not silently fetched
        # as a 404.
        for pid in spec["podcast_ids"]:
            if backend.has_podcast(pid):
                pids.append(pid)
            else:
                unresolved.append(pid)

        for title in spec["podcast_titles"]:
            try:
                pids.append(backend.get_podcast_by_name(title)["podcast_id"])
            except NotFoundError:
                unresolved.append(title)

        for eid in spec["episode_ids"]:
            row = backend.get_episode_by_id(eid)
            if row is None:
                unresolved.append(eid)
            else:
                pids.append(str(row["podcast_id"]))

        # Preserve caller order but drop repeats (several episodes often share
        # a podcast, and its partitions only need fetching once).
        pids = list(dict.fromkeys(pids))

        if not pids:
            raise ValueError(
                "subset resolved to no podcasts. Unresolved entries: "
                f"{unresolved[:10]}"
            )

        if unresolved:
            logger.warning(
                "subset: %d entries could not be resolved and were skipped "
                "(first few: %s)", len(unresolved), unresolved[:5],
            )

        from .source import HubDataSource

        # Prefetching a local directory downloads nothing, but still reports a
        # nonzero "files" (which counts partitions now *available*, not ones
        # fetched), so the return value alone looks like a successful fetch.
        if not isinstance(getattr(backend, "_source", None), HubDataSource):
            logger.warning(
                "prefetch() has nothing to fetch: this dataset reads a local "
                "directory (parquet_dir=...), which never downloads. Data "
                "absent from it stays absent. Omit parquet_dir to fetch from "
                "HuggingFace."
            )

        logger.info("Prefetching data for %d podcast(s)...", len(pids))

        # Resolve the podcasts to the distinct part files that hold them, and
        # fetch each file once. Podcasts share parts -- about a thousand to a
        # file, and neighbours in the category ordering land together -- so a
        # few hundred podcasts from one category usually come down as a handful
        # of files. Fetching per podcast instead would request the same part
        # hundreds of times, which is what made prefetching a subset hit the
        # Hub's rate limit and fail with HTTP 429.
        smap = backend.shard_map
        rels = []
        for tree in ("episodes", "turns_text", "acoustics", "turns_metrics"):
            for part in smap.parts_for(tree, pids):
                rels.append(smap.relpath(tree, part))
        rels = list(dict.fromkeys(rels))

        logger.info("%d podcast(s) resolve to %d part file(s)",
                    len(pids), len(rels))

        # Modest concurrency: these are ~100 MB files, so a handful in flight
        # saturates the link, and the Hub counts every request against a
        # five-minute window shared with the rest of the session.
        files = 0
        source = getattr(backend, "_source", None)
        with (source.downloads_enabled() if source is not None
              else contextlib.nullcontext()):
            if len(rels) > 1 and _PREFETCH_WORKERS > 1:
                from concurrent.futures import ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=_PREFETCH_WORKERS) as pool:
                    for got in pool.map(
                            lambda r: source.path(r) is not None, rels):
                        files += bool(got)
            else:
                for rel in rels:
                    files += bool(source.path(rel) is not None)

        logger.info("Prefetch complete: %d podcast(s), %d file(s)",
                    len(pids), files)
        return {"podcasts": len(pids), "files": files, "unresolved": unresolved}

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

        Note:
            Matching happens in the metadata catalog, but building each Episode
            reads that podcast's partition. Pass ``max_episodes`` to bound the
            work: a bare category search matches tens of thousands of episodes
            across thousands of podcasts.
        """
        rows = self._parquet_backend.search_episodes(**criteria)

        # Cut the candidate rows down before building anything: each build
        # reads (and on a lazy source, downloads) a partition, so applying the
        # limit afterwards would fetch ~1 GB to return ten episodes.
        if max_episodes is not None and sampling_mode != "first":
            rows = random.sample(rows, len(rows))

        episodes = []
        for row in rows:
            if max_episodes is not None and len(episodes) >= max_episodes:
                break
            try:
                ep = self._parquet_backend.build_episode_object(
                    row.get("podcast_id", ""), row.get("episode_id", ""),
                )
                episodes.append(ep)
            except Exception as e:
                logger.debug("Skipping episode during search: %s", e)
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
        self._warn_if_whole_corpus(
            len(pids), f"search_podcasts_by_subcategory({subcategory!r})",
            "the backend's get_podcasts_by_category(), which returns ids, "
            "and build only the ones you need")
        podcasts = []
        for pid in pids:
            try:
                podcasts.append(self._parquet_backend.build_podcast_object(pid))
            except Exception as e:
                logger.debug("Skipping podcast during subcategory search: %s", e)
        return podcasts

    def _warn_if_whole_corpus(self, n: int, what: str, instead: str) -> None:
        """
        Warn before materializing the entire corpus.

        Building an object reads that podcast's row group, so these two methods
        cost one read per podcast. On the full dataset that is 228,099 reads
        covering the whole 15 GB episodes tree, and on a lazy Hub source every
        one of them is a download. Both used to do that in silence, which on a
        metered connection is an unpleasant way to find out.
        """
        if n < self._WHOLE_CORPUS_WARN_AT:
            return
        logger.warning(
            "%s is about to build %s objects, one partition read each. "
            "On a lazy source that downloads most of the corpus. Use %s "
            "to bound the work.", what, f"{n:,}", instead)

    # Roughly the point where this stops being a subset and starts being a
    # download. A tutorial subset (403 podcasts) should say nothing.
    _WHOLE_CORPUS_WARN_AT = 5000

    def get_all_podcasts(self) -> List[Podcast]:
        """
        Get all podcasts in the dataset.

        Returns:
            List of all Podcast objects

        Note:
            Costs one partition read per podcast. Prefer
            ``iterate_podcasts(max_podcasts=N)`` on the full corpus.
        """
        podcast_ids = self._parquet_backend.get_all_podcast_ids()
        self._warn_if_whole_corpus(len(podcast_ids), "get_all_podcasts()",
                                   "iterate_podcasts(max_podcasts=N)")
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

        Note:
            Costs one partition read per episode's podcast. Prefer
            ``iterate_episodes(max_episodes=N)`` on the full corpus.
        """
        rows = self._parquet_backend.search_episodes()
        self._warn_if_whole_corpus(len(rows), "get_all_episodes()",
                                   "iterate_episodes(max_episodes=N)")
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
        podcast_ids = self._parquet_backend.get_all_podcast_ids()
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

    def get_podcasts_by_host(self, name: str, *,
                             exact: bool = False) -> List[str]:
        """
        Podcast ids hosted by *name*, from the shipped host index.

        Answered from ``metadata/host_index.parquet`` alone (no part-file
        downloads). ``exact`` requires a full case-insensitive match; otherwise
        a substring matches.
        """
        return self._parquet_backend.get_podcasts_by_host(name, exact=exact)

    def search_by_host(self, name: str, *, exact: bool = False,
                       limit: int = 100) -> List[Dict[str, Any]]:
        """
        Find episodes hosted by *name*, from the shipped host episode index.

        Covers hosts only, so it avoids the guest-mention artefact that
        :meth:`search_by_speaker_name` warns about. Answered from
        ``metadata/host_episode_index.parquet`` alone.

        Returns:
            List of dicts with episode_id, podcast_id, name_original.
        """
        return self._parquet_backend.search_by_host(
            name, exact=exact, limit=limit,
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
