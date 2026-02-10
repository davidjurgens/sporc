"""
Parquet-based backend for the SPORC dataset.

Loads in-memory indexes from pre-built Parquet catalog files and reads
per-podcast partitioned data on demand, providing O(1) lookups by podcast,
hostname, and category.
"""

import hashlib
import json
import logging
import os
import pickle
import re
import time
from typing import Any, Callable, Dict, List, Optional

import pyarrow as pa
import pyarrow.feather as feather
import pyarrow.parquet as pq

from .episode import Episode
from .exceptions import DatasetAccessError, IndexNotBuiltError, NotFoundError
from .podcast import Podcast
from .turn import Turn

logger = logging.getLogger(__name__)

# Version tag embedded in the cache so that code changes automatically
# invalidate stale caches.
_CACHE_VERSION = 2


class ParquetBackend:
    """
    Backend that reads from a partitioned Parquet layout produced by
    ``scripts/convert_to_parquet.py``.

    At startup it loads lightweight dict indexes into memory for O(1)
    lookups.  The heavier catalog DataFrames (used only for search /
    statistics) are loaded lazily on first use.

    On the first load, indexes are built from the Parquet files and
    cached in Arrow IPC (feather) and pickle formats so that subsequent
    loads take only a few seconds.
    """

    def __init__(self, data_dir: str) -> None:
        start = time.time()
        self.data_dir = data_dir
        self._meta_dir = os.path.join(data_dir, "metadata")

        if not os.path.isdir(self._meta_dir):
            raise DatasetAccessError(
                f"Parquet metadata directory not found: {self._meta_dir}. "
                "Run scripts/convert_to_parquet.py first."
            )

        # Lazy-loaded DataFrames (only materialized when search/stats are used)
        self._podcast_df = None
        self._episode_df = None
        self._num_podcasts: int = 0
        self._num_episodes: int = 0

        # Lazy-loaded indexes for search / metrics
        self._speaker_index_df = None
        self._episode_metrics_df = None
        self._search_db_con = None

        cache_idx = os.path.join(self._meta_dir, "_index_cache.pkl")
        if self._load_cache(cache_idx):
            elapsed = time.time() - start
            logger.info(
                "ParquetBackend loaded from cache in %.2fs  "
                "(%s podcasts, %s episodes)",
                elapsed,
                f"{self._num_podcasts:,}",
                f"{self._num_episodes:,}",
            )
            return

        # --- Build from Parquet files ---
        self._build_indexes()

        # Save cache for fast subsequent loads
        self._save_cache(cache_idx)

        elapsed = time.time() - start
        logger.info(
            "ParquetBackend loaded in %.2fs  (%s podcasts, %s episodes)",
            elapsed,
            f"{self._num_podcasts:,}",
            f"{self._num_episodes:,}",
        )

    # ------------------------------------------------------------------
    # Lazy DataFrame access
    # ------------------------------------------------------------------
    def _ensure_podcast_df(self):
        """Materialize the podcast DataFrame on first access."""
        if self._podcast_df is not None:
            return
        cache_path = os.path.join(self._meta_dir, "_podcast_df.arrow")
        if os.path.exists(cache_path):
            logger.info("Loading podcast DataFrame from feather cache")
            self._podcast_df = feather.read_feather(cache_path)
        else:
            logger.info("Loading podcast DataFrame from parquet")
            path = os.path.join(self._meta_dir, "podcast_catalog.parquet")
            self._podcast_df = pq.read_table(path).to_pandas()

    def _ensure_episode_df(self):
        """Materialize the episode DataFrame on first access."""
        if self._episode_df is not None:
            return
        cache_path = os.path.join(self._meta_dir, "_episode_df.arrow")
        if os.path.exists(cache_path):
            logger.info("Loading episode DataFrame from feather cache")
            self._episode_df = feather.read_feather(cache_path)
        else:
            logger.info("Loading episode DataFrame from parquet")
            path = os.path.join(self._meta_dir, "episode_catalog.parquet")
            self._episode_df = pq.read_table(path).to_pandas()

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _fingerprint(meta_dir: str) -> str:
        """Hash of parquet file sizes/mtimes to detect data changes."""
        parts = []
        for name in sorted(os.listdir(meta_dir)):
            if name.endswith(".parquet"):
                p = os.path.join(meta_dir, name)
                st = os.stat(p)
                parts.append(f"{name}:{st.st_size}:{st.st_mtime_ns}")
        return hashlib.md5("|".join(parts).encode()).hexdigest()

    def _load_cache(self, cache_path: str) -> bool:
        """Load pre-built lightweight indexes from pickle cache."""
        if not os.path.exists(cache_path):
            return False
        try:
            with open(cache_path, "rb") as f:
                cache = pickle.load(f)
            if cache.get("version") != _CACHE_VERSION:
                logger.info("Cache version mismatch, rebuilding")
                return False
            if cache.get("fingerprint") != self._fingerprint(self._meta_dir):
                logger.info("Parquet files changed, rebuilding cache")
                return False
            logger.info("Loading indexes from cache %s", cache_path)
            self._pid_to_idx = cache["pid_to_idx"]
            self._title_lower_to_pid = cache["title_lower_to_pid"]
            self._eid_to_idx = cache["eid_to_idx"]
            self._pid_to_ep_idxs = cache["pid_to_ep_idxs"]
            self._category_to_pids = cache["category_to_pids"]
            self._hostname_to_pids = cache["hostname_to_pids"]
            self._manifest = cache["manifest"]
            self._num_podcasts = cache["num_podcasts"]
            self._num_episodes = cache["num_episodes"]
            return True
        except Exception as e:
            logger.warning("Failed to load cache: %s", e)
            return False

    def _save_cache(self, cache_path: str) -> None:
        """Persist lightweight indexes to pickle, DataFrames to feather."""
        # Save dict indexes (small, fast to pickle)
        cache = {
            "version": _CACHE_VERSION,
            "fingerprint": self._fingerprint(self._meta_dir),
            "pid_to_idx": self._pid_to_idx,
            "title_lower_to_pid": self._title_lower_to_pid,
            "eid_to_idx": self._eid_to_idx,
            "pid_to_ep_idxs": self._pid_to_ep_idxs,
            "category_to_pids": self._category_to_pids,
            "hostname_to_pids": self._hostname_to_pids,
            "manifest": self._manifest,
            "num_podcasts": self._num_podcasts,
            "num_episodes": self._num_episodes,
        }
        try:
            with open(cache_path, "wb") as f:
                pickle.dump(cache, f, protocol=pickle.HIGHEST_PROTOCOL)
            logger.info("Saved index cache to %s", cache_path)
        except Exception as e:
            logger.warning("Failed to save index cache: %s", e)

        # Save DataFrames as Arrow IPC (feather) for fast memory-mapped reads
        try:
            if self._podcast_df is not None:
                pc_cache = os.path.join(self._meta_dir, "_podcast_df.arrow")
                feather.write_feather(
                    pa.Table.from_pandas(self._podcast_df), pc_cache,
                    compression="lz4",
                )
            if self._episode_df is not None:
                ec_cache = os.path.join(self._meta_dir, "_episode_df.arrow")
                feather.write_feather(
                    pa.Table.from_pandas(self._episode_df), ec_cache,
                    compression="lz4",
                )
            logger.info("Saved DataFrame caches as feather")
        except Exception as e:
            logger.warning("Failed to save feather caches: %s", e)

    # ------------------------------------------------------------------
    # Index building (first load)
    # ------------------------------------------------------------------
    def _build_indexes(self) -> None:
        """Build all in-memory indexes from the Parquet catalog files."""
        meta_dir = self._meta_dir

        # --- Podcast catalog ---
        pc_path = os.path.join(meta_dir, "podcast_catalog.parquet")
        logger.info("Loading podcast catalog from %s", pc_path)
        pc_table = pq.read_table(pc_path)
        self._podcast_df = pc_table.to_pandas()
        self._num_podcasts = len(self._podcast_df)

        # Build lookup indexes from pyarrow columns (much faster than iterrows)
        pc_pids = pc_table.column("podcast_id").to_pylist()
        pc_titles = pc_table.column("pod_title").to_pylist()
        self._pid_to_idx: Dict[str, int] = {
            pid: i for i, pid in enumerate(pc_pids)
        }
        self._title_lower_to_pid: Dict[str, str] = {
            title.lower(): pid for pid, title in zip(pc_pids, pc_titles)
        }

        # --- Episode catalog: read only the columns we need for indexing ---
        ec_path = os.path.join(meta_dir, "episode_catalog.parquet")
        logger.info("Loading episode catalog from %s", ec_path)
        # Read full table for DataFrame, but build indexes from arrow columns
        ec_table = pq.read_table(ec_path)
        self._episode_df = ec_table.to_pandas()
        self._num_episodes = len(self._episode_df)

        ec_eids = ec_table.column("episode_id").to_pylist()
        ec_pids = ec_table.column("podcast_id").to_pylist()
        self._eid_to_idx: Dict[str, int] = {
            eid: i for i, eid in enumerate(ec_eids)
        }
        self._pid_to_ep_idxs: Dict[str, List[int]] = {}
        for i, pid in enumerate(ec_pids):
            self._pid_to_ep_idxs.setdefault(pid, []).append(i)

        # --- Category index ---
        ci_path = os.path.join(meta_dir, "category_index.parquet")
        logger.info("Loading category index from %s", ci_path)
        ci_table = pq.read_table(ci_path)
        ci_cats = ci_table.column("category").to_pylist()
        ci_pids = ci_table.column("podcast_id").to_pylist()
        self._category_to_pids: Dict[str, set] = {}
        for cat, pid in zip(ci_cats, ci_pids):
            self._category_to_pids.setdefault(cat, set()).add(pid)

        # --- Hostname index ---
        hi_path = os.path.join(meta_dir, "hostname_index.parquet")
        logger.info("Loading hostname index from %s", hi_path)
        hi_table = pq.read_table(hi_path)
        hi_hosts = hi_table.column("hostname").to_pylist()
        hi_pids = hi_table.column("podcast_id").to_pylist()
        self._hostname_to_pids: Dict[str, set] = {}
        for host, pid in zip(hi_hosts, hi_pids):
            self._hostname_to_pids.setdefault(host, set()).add(pid)

        # --- Load manifest ---
        manifest_path = os.path.join(self.data_dir, "manifest.json")
        if os.path.exists(manifest_path):
            with open(manifest_path) as f:
                self._manifest = json.load(f)
        else:
            self._manifest = {}

    # ------------------------------------------------------------------
    # Podcast lookups
    # ------------------------------------------------------------------
    def get_podcast_by_name(self, name: str) -> Dict[str, Any]:
        """
        Lookup a podcast by title (exact, then case-insensitive, then partial).

        Returns:
            Dict with podcast catalog fields.

        Raises:
            NotFoundError: if no match is found.
        """
        name_lower = name.lower()

        # Exact (case-insensitive)
        pid = self._title_lower_to_pid.get(name_lower)
        if pid is not None:
            return self._podcast_row_to_dict(self._pid_to_idx[pid])

        # Partial match
        for title_lower, pid in self._title_lower_to_pid.items():
            if name_lower in title_lower:
                return self._podcast_row_to_dict(self._pid_to_idx[pid])

        raise NotFoundError(f"Podcast '{name}' not found")

    def get_podcast_by_id(self, podcast_id: str) -> Dict[str, Any]:
        """Lookup a podcast by its podcast_id."""
        idx = self._pid_to_idx.get(podcast_id)
        if idx is None:
            raise NotFoundError(f"Podcast id '{podcast_id}' not found")
        return self._podcast_row_to_dict(idx)

    def get_podcasts_by_category(self, category: str) -> List[str]:
        """Return podcast_ids that have episodes in *category*."""
        cat_lower = category.lower()
        for cat, pids in self._category_to_pids.items():
            if cat.lower() == cat_lower:
                return list(pids)
        return []

    def get_podcasts_by_hostname(self, hostname: str) -> List[str]:
        """Return podcast_ids whose RSS URL matches *hostname*."""
        return list(self._hostname_to_pids.get(hostname, []))

    # ------------------------------------------------------------------
    # Episode lookups
    # ------------------------------------------------------------------
    def get_episodes_for_podcast(
        self, podcast_id: str, include_transcript: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Return episode metadata for a podcast.

        If *include_transcript* is True, reads the per-podcast partition
        file (which contains transcripts).  Otherwise returns catalog data
        (no transcripts, in-memory).
        """
        if include_transcript:
            return self._read_podcast_episodes_partition(podcast_id)

        ep_idxs = self._pid_to_ep_idxs.get(podcast_id, [])
        return [self._episode_row_to_dict(i) for i in ep_idxs]

    def get_episode_by_id(self, episode_id: str) -> Optional[Dict[str, Any]]:
        """Lookup a single episode by episode_id (in-memory catalog)."""
        idx = self._eid_to_idx.get(episode_id)
        if idx is None:
            return None
        return self._episode_row_to_dict(idx)

    # ------------------------------------------------------------------
    # Turn lookups
    # ------------------------------------------------------------------
    def get_turns_for_episode(
        self,
        podcast_id: str,
        episode_id: str,
        include_audio: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Read turn data for a single episode from the partition files.

        Args:
            podcast_id: The podcast partition key.
            episode_id: Episode to filter for.
            include_audio: If True, join audio features onto each turn row.

        Returns:
            List of turn dicts, sorted by start_time.
        """
        text_path = os.path.join(
            self.data_dir, "turns", f"podcast_id={podcast_id}", "text.parquet"
        )
        if not os.path.exists(text_path):
            return []

        import pyarrow.compute as pc

        text_table = pq.ParquetFile(text_path).read()
        # Filter using pyarrow compute (avoids pandas conversion for non-matching rows)
        mask = pc.equal(text_table.column("episode_id"), episode_id)
        text_table = text_table.filter(mask)

        if text_table.num_rows == 0:
            return []

        if include_audio:
            audio_path = os.path.join(
                self.data_dir,
                "turns",
                f"podcast_id={podcast_id}",
                "audio_features.parquet",
            )
            if os.path.exists(audio_path):
                audio_table = pq.ParquetFile(audio_path).read()
                audio_mask = pc.equal(audio_table.column("episode_id"), episode_id)
                audio_table = audio_table.filter(audio_mask)
                # Drop duplicate key columns before join, convert to pandas for merge
                text_df = text_table.to_pandas()
                audio_df = audio_table.to_pandas()
                text_df = text_df.merge(
                    audio_df.drop(columns=["episode_id", "podcast_id", "mp3_url"], errors="ignore"),
                    on=["turn_count", "start_time"],
                    how="left",
                    suffixes=("", "_audio"),
                )
                text_df = text_df.sort_values("start_time")
                return text_df.to_dict(orient="records")

        # Sort by start_time using pyarrow (no pandas conversion needed)
        sort_indices = pc.sort_indices(text_table, sort_keys=[("start_time", "ascending")])
        text_table = text_table.take(sort_indices)
        # Convert column-oriented pydict to list of row dicts
        cols = text_table.to_pydict()
        n = text_table.num_rows
        return [{k: cols[k][i] for k in cols} for i in range(n)]

    # ------------------------------------------------------------------
    # Search / filter
    # ------------------------------------------------------------------
    def search_episodes(self, **criteria) -> List[Dict[str, Any]]:
        """
        Filter the in-memory episode catalog by various criteria.

        Supported criteria keys:
            min_duration, max_duration, min_speakers, max_speakers,
            host_name, guest_name, category, subcategory, language,
            podcast_name, podcast_id,
            min_overlap_prop_duration, max_overlap_prop_duration.
        """
        self._ensure_episode_df()
        df = self._episode_df

        if "min_duration" in criteria:
            df = df[df["duration_seconds"] >= criteria["min_duration"]]
        if "max_duration" in criteria:
            df = df[df["duration_seconds"] <= criteria["max_duration"]]
        if "min_speakers" in criteria:
            df = df[df["num_main_speakers"] >= criteria["min_speakers"]]
        if "max_speakers" in criteria:
            df = df[df["num_main_speakers"] <= criteria["max_speakers"]]
        if "language" in criteria:
            lang = criteria["language"].lower()
            df = df[df["language"].str.lower() == lang]
        if "podcast_id" in criteria:
            df = df[df["podcast_id"] == criteria["podcast_id"]]
        if "podcast_name" in criteria:
            pname = criteria["podcast_name"].lower()
            matching_pids = [
                pid
                for title_lower, pid in self._title_lower_to_pid.items()
                if pname in title_lower
            ]
            df = df[df["podcast_id"].isin(matching_pids)]
        if "category" in criteria:
            cat = criteria["category"].lower()
            # Use pre-built index for fast category lookup
            matching_pids = set()
            for c, pids in self._category_to_pids.items():
                if cat in c.lower():
                    matching_pids.update(pids)
            if matching_pids:
                df = df[df["podcast_id"].isin(matching_pids)]
            else:
                df = df.iloc[0:0]
        if "subcategory" in criteria:
            subcat = criteria["subcategory"].lower()
            matching_pids = set()
            for c, pids in self._category_to_pids.items():
                if subcat in c.lower():
                    matching_pids.update(pids)
            if matching_pids:
                df = df[df["podcast_id"].isin(matching_pids)]
            else:
                df = df.iloc[0:0]
        if "host_name" in criteria:
            import numpy as np
            hname = criteria["host_name"].lower()
            names_col = df["host_predicted_names"].values
            mask = np.array([
                any(hname in str(n).lower() for n in names)
                if isinstance(names, list) else False
                for names in names_col
            ])
            df = df[mask]
        if "guest_name" in criteria:
            import numpy as np
            gname = criteria["guest_name"].lower()
            names_col = df["guest_predicted_names"].values
            mask = np.array([
                any(gname in str(n).lower() for n in names)
                if isinstance(names, list) else False
                for names in names_col
            ])
            df = df[mask]
        if "min_overlap_prop_duration" in criteria:
            df = df[df["overlap_prop_duration"] >= criteria["min_overlap_prop_duration"]]
        if "max_overlap_prop_duration" in criteria:
            df = df[df["overlap_prop_duration"] <= criteria["max_overlap_prop_duration"]]

        return df.to_dict(orient="records")

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------
    def get_statistics(self) -> Dict[str, Any]:
        """Compute dataset statistics from the in-memory catalogs."""
        self._ensure_podcast_df()
        self._ensure_episode_df()
        pc = self._podcast_df
        ec = self._episode_df

        total_podcasts = len(pc)
        total_episodes = len(ec)
        total_duration_hours = ec["duration_seconds"].sum() / 3600.0
        avg_duration_minutes = (
            ec["duration_seconds"].mean() / 60.0 if total_episodes else 0.0
        )

        # Category distribution
        cat_cols = [f"category{i}" for i in range(1, 11)]
        category_counts: Dict[str, int] = {}
        for col in cat_cols:
            if col in ec.columns:
                for val in ec[col].dropna():
                    v = str(val).strip()
                    if v:
                        category_counts[v] = category_counts.get(v, 0) + 1

        # Language distribution
        language_counts = ec["language"].value_counts().to_dict()

        # Speaker distribution
        speaker_counts = ec["num_main_speakers"].value_counts().to_dict()

        return {
            "total_podcasts": total_podcasts,
            "total_episodes": total_episodes,
            "total_duration_hours": float(total_duration_hours),
            "avg_episode_duration_minutes": float(avg_duration_minutes),
            "category_distribution": category_counts,
            "language_distribution": {str(k): int(v) for k, v in language_counts.items()},
            "speaker_distribution": {int(k): int(v) for k, v in speaker_counts.items()},
        }

    # ------------------------------------------------------------------
    # DuckDB (optional)
    # ------------------------------------------------------------------
    def query_duckdb(self, sql: str) -> Any:
        """
        Run a SQL query against the Parquet layout using DuckDB.

        Requires ``duckdb`` to be installed (``pip install duckdb``).

        The query can reference files via their paths, e.g.::

            SELECT * FROM read_parquet('/shared/6/projects/sporc/v1/metadata/podcast_catalog.parquet')
            WHERE pod_title ILIKE '%comedy%'
        """
        try:
            import duckdb
        except ImportError:
            raise ImportError(
                "DuckDB is required for SQL queries. Install it with: "
                "pip install duckdb"
            )
        con = duckdb.connect()
        return con.execute(sql).fetchdf()

    # ------------------------------------------------------------------
    # Object construction helpers
    # ------------------------------------------------------------------
    def build_podcast_object(self, podcast_id: str, include_turns: bool = False) -> Podcast:
        """
        Construct a full ``Podcast`` object with ``Episode`` children.

        Episodes are populated from the per-podcast partition file (includes
        transcripts).  If *include_turns* is True, turns are also loaded.
        """
        pinfo = self.get_podcast_by_id(podcast_id)
        podcast = Podcast(
            title=pinfo["pod_title"],
            description=pinfo.get("pod_description", ""),
            rss_url=pinfo["rss_url"],
            language=pinfo.get("language", "en"),
            explicit=bool(pinfo.get("explicit", 0)),
            image_url=pinfo.get("image_url"),
            itunes_author=pinfo.get("itunes_author"),
        )

        ep_rows = self._read_podcast_episodes_partition(podcast_id)
        for erow in ep_rows:
            episode = self._episode_dict_to_object(erow, pinfo)
            if include_turns:
                self._load_turns_into_episode(episode, podcast_id, erow["episode_id"])
            else:
                # Set up lazy turn loader
                pid = podcast_id
                eid = erow["episode_id"]
                episode._turn_loader = lambda e=episode, p=pid, ei=eid: self._load_turns_into_episode(e, p, ei)
            podcast.episodes.append(episode)

        return podcast

    def build_episode_object(
        self,
        podcast_id: str,
        episode_id: str,
        include_turns: bool = False,
    ) -> Episode:
        """Construct a single ``Episode`` object."""
        ep_rows = self._read_podcast_episodes_partition(podcast_id)
        for erow in ep_rows:
            if erow.get("episode_id") == episode_id:
                pinfo = self.get_podcast_by_id(podcast_id)
                episode = self._episode_dict_to_object(erow, pinfo)
                if include_turns:
                    self._load_turns_into_episode(episode, podcast_id, episode_id)
                else:
                    pid = podcast_id
                    eid = episode_id
                    episode._turn_loader = lambda e=episode, p=pid, ei=eid: self._load_turns_into_episode(e, p, ei)
                return episode
        raise NotFoundError(f"Episode '{episode_id}' not found in podcast '{podcast_id}'")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _podcast_row_to_dict(self, idx: int) -> Dict[str, Any]:
        self._ensure_podcast_df()
        row = self._podcast_df.iloc[idx]
        return row.to_dict()

    def _episode_row_to_dict(self, idx: int) -> Dict[str, Any]:
        self._ensure_episode_df()
        row = self._episode_df.iloc[idx]
        return row.to_dict()

    def _read_podcast_episodes_partition(self, podcast_id: str) -> List[Dict[str, Any]]:
        """Read the per-podcast episode partition file (includes transcripts)."""
        path = os.path.join(
            self.data_dir, "episodes", f"podcast_id={podcast_id}", "data.parquet"
        )
        if not os.path.exists(path):
            return []
        table = pq.ParquetFile(path).read()
        return table.to_pandas().to_dict(orient="records")

    def _episode_dict_to_object(
        self, erow: Dict[str, Any], pinfo: Dict[str, Any]
    ) -> Episode:
        """Convert a partition row dict to an Episode object."""

        def _to_list(val):
            if isinstance(val, list):
                return val
            if val is None:
                return []
            return list(val) if hasattr(val, "__iter__") and not isinstance(val, str) else []

        def _to_dict(val):
            if isinstance(val, dict):
                return val
            if isinstance(val, str):
                try:
                    return json.loads(val)
                except (json.JSONDecodeError, ValueError):
                    return {}
            return {}

        title = str(erow.get("ep_title", "")).strip()
        if not title:
            title = f"Untitled Episode ({erow.get('mp3_url', 'unknown')})"
        mp3_url = str(erow.get("mp3_url", "")).strip()
        if not mp3_url:
            mp3_url = "unknown"

        return Episode(
            title=title,
            description=str(erow.get("ep_description", "")),
            mp3_url=mp3_url,
            duration_seconds=float(erow.get("duration_seconds", 0)),
            transcript=str(erow.get("transcript", "")),
            podcast_title=str(pinfo.get("pod_title", "")),
            podcast_description=str(pinfo.get("pod_description", "")),
            rss_url=str(pinfo.get("rss_url", "")),
            category1=str(erow.get("category1", "")) or None,
            category2=str(erow.get("category2", "")) or None,
            category3=str(erow.get("category3", "")) or None,
            category4=str(erow.get("category4", "")) or None,
            category5=str(erow.get("category5", "")) or None,
            category6=str(erow.get("category6", "")) or None,
            category7=str(erow.get("category7", "")) or None,
            category8=str(erow.get("category8", "")) or None,
            category9=str(erow.get("category9", "")) or None,
            category10=str(erow.get("category10", "")) or None,
            host_predicted_names=_to_list(erow.get("host_predicted_names")),
            guest_predicted_names=_to_list(erow.get("guest_predicted_names")),
            neither_predicted_names=_to_list(erow.get("neither_predicted_names")),
            main_ep_speakers=_to_list(erow.get("main_ep_speakers")),
            host_speaker_labels=_to_dict(erow.get("host_speaker_labels")),
            guest_speaker_labels=_to_dict(erow.get("guest_speaker_labels")),
            overlap_prop_duration=float(erow.get("overlap_prop_duration", 0)),
            overlap_prop_turn_count=float(erow.get("overlap_prop_turn_count", 0)),
            avg_turn_duration=float(erow.get("avg_turn_duration", 0)),
            total_speaker_labels=int(float(erow.get("total_sp_labels", 0))),
            language=str(erow.get("language", "en")),
            explicit=bool(erow.get("explicit", 0)),
            image_url=str(erow.get("image_url", "")) or None,
            episode_date_localized=str(erow.get("episode_date_localized", "")) or None,
            oldest_episode_date=str(erow.get("oldest_episode_date", "")) or None,
            last_update=str(erow.get("last_update", "")) or None,
            created_on=str(erow.get("created_on", "")) or None,
        )

    def _load_turns_into_episode(
        self, episode: Episode, podcast_id: str, episode_id: str
    ) -> None:
        """Load turn data from Parquet into an Episode object."""
        if episode._turns_loaded:
            return

        turn_rows = self.get_turns_for_episode(
            podcast_id, episode_id, include_audio=True
        )

        turns = []
        for row in turn_rows:
            speaker = row.get("speaker", [])
            if isinstance(speaker, str):
                speaker = [speaker]
            if not isinstance(speaker, list):
                try:
                    speaker = list(speaker)
                except (TypeError, ValueError):
                    speaker = [str(speaker)]
            if not speaker:
                continue

            text = str(row.get("turn_text", "")).strip()
            if not text:
                continue

            start_time = float(row.get("start_time", 0))
            end_time = float(row.get("end_time", 0))
            duration = float(row.get("duration", 0))
            if end_time <= start_time:
                continue

            try:
                turn = Turn(
                    speaker=speaker,
                    text=text,
                    start_time=start_time,
                    end_time=end_time,
                    duration=duration,
                    turn_count=int(row.get("turn_count", 0)),
                    mfcc1_sma3_mean=row.get("mfcc1_sma3_mean"),
                    mfcc2_sma3_mean=row.get("mfcc2_sma3_mean"),
                    mfcc3_sma3_mean=row.get("mfcc3_sma3_mean"),
                    mfcc4_sma3_mean=row.get("mfcc4_sma3_mean"),
                    f0_semitone_from_27_5hz_sma3nz_mean=row.get(
                        "f0_semitone_from_27_5hz_sma3nz_mean"
                    ),
                    f1_frequency_sma3nz_mean=row.get("f1_frequency_sma3nz_mean"),
                    inferred_speaker_role=str(row.get("inferred_speaker_role", "")) or None,
                    inferred_speaker_name=str(row.get("inferred_speaker_name", "")) or None,
                    mp3_url=str(row.get("mp3_url", "")) or None,
                )
                turns.append(turn)
            except (ValueError, TypeError) as e:
                logger.debug("Skipping invalid turn: %s", e)
                continue

        turns.sort(key=lambda t: t.start_time)
        episode._turns = turns
        episode._turns_loaded = True

    # ------------------------------------------------------------------
    # Lazy loaders for precomputed indexes
    # ------------------------------------------------------------------
    def _ensure_speaker_index(self) -> None:
        """Load speaker_name_index.parquet on first speaker search."""
        if self._speaker_index_df is not None:
            return
        path = os.path.join(self._meta_dir, "speaker_name_index.parquet")
        if not os.path.exists(path):
            raise IndexNotBuiltError(
                f"Speaker name index not found at {path}. "
                "Build it with: python scripts/build_indexes.py --data-dir "
                f"{self.data_dir} --phase 1"
            )
        logger.info("Loading speaker name index from %s", path)
        self._speaker_index_df = pq.read_table(path).to_pandas()

    def _ensure_episode_metrics_df(self) -> None:
        """Load episode_metrics.parquet on first metrics query."""
        if self._episode_metrics_df is not None:
            return
        path = os.path.join(self._meta_dir, "episode_metrics.parquet")
        if not os.path.exists(path):
            raise IndexNotBuiltError(
                f"Episode metrics index not found at {path}. "
                "Build it with: python scripts/build_indexes.py --data-dir "
                f"{self.data_dir} --phase 2"
            )
        logger.info("Loading episode metrics from %s", path)
        self._episode_metrics_df = pq.read_table(path).to_pandas()

    def _ensure_search_db(self) -> None:
        """Open DuckDB turns_search.duckdb on first text search."""
        if self._search_db_con is not None:
            return
        try:
            import duckdb
        except ImportError:
            raise ImportError(
                "DuckDB is required for full-text search. "
                "Install it with: pip install duckdb"
            )
        path = os.path.join(self._meta_dir, "turns_search.duckdb")
        if not os.path.exists(path):
            raise IndexNotBuiltError(
                f"DuckDB search database not found at {path}. "
                "Build it with: python scripts/build_indexes.py --data-dir "
                f"{self.data_dir} --phase 3"
            )
        logger.info("Opening DuckDB search database at %s", path)
        self._search_db_con = duckdb.connect(path, read_only=True)
        self._search_db_con.execute("LOAD fts")

    # ------------------------------------------------------------------
    # Full-text transcript search
    # ------------------------------------------------------------------
    def search_turns(
        self,
        query: str,
        *,
        mode: str = "fts",
        podcast_id: Optional[str] = None,
        episode_id: Optional[str] = None,
        speaker_role: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Search turn text across the corpus.

        Args:
            query: Search query string.
            mode: "fts" (BM25 ranked), "exact" (ILIKE), or "regex".
            podcast_id: Filter to a specific podcast.
            episode_id: Filter to a specific episode.
            speaker_role: Filter by speaker role ("host", "guest", etc.).
            limit: Maximum results to return.
            offset: Number of results to skip.

        Returns:
            List of dicts with episode_id, podcast_id, turn_count,
            turn_text, start_time, end_time, speaker_role, score.
        """
        self._ensure_search_db()
        con = self._search_db_con

        where_clauses = []
        params = []

        if podcast_id:
            where_clauses.append("t.podcast_id = ?")
            params.append(podcast_id)
        if episode_id:
            where_clauses.append("t.episode_id = ?")
            params.append(episode_id)
        if speaker_role:
            where_clauses.append("t.speaker_role = ?")
            params.append(speaker_role)

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        if mode == "fts":
            sql = f"""
                WITH scored AS (
                    SELECT *, fts_main_turns.match_bm25(row_id, ?) AS score
                    FROM turns
                    WHERE score IS NOT NULL
                )
                SELECT episode_id, podcast_id, turn_count, turn_text,
                       start_time, end_time, duration, speaker_role,
                       speaker_name, word_count, score
                FROM scored
                {"WHERE " + " AND ".join(c.replace("t.", "") for c in where_clauses) if where_clauses else ""}
                ORDER BY score DESC
                LIMIT ? OFFSET ?
            """
            result = con.execute(sql, [query] + params + [limit, offset])
        elif mode == "exact":
            sql = f"""
                SELECT episode_id, podcast_id, turn_count, turn_text,
                       start_time, end_time, duration, speaker_role,
                       speaker_name, word_count, 1.0 AS score
                FROM turns t
                {where_sql}
                {"AND" if where_clauses else "WHERE"} t.turn_text ILIKE ?
                LIMIT ? OFFSET ?
            """
            params.append(f"%{query}%")
            result = con.execute(sql, params + [limit, offset])
        elif mode == "regex":
            sql = f"""
                SELECT episode_id, podcast_id, turn_count, turn_text,
                       start_time, end_time, duration, speaker_role,
                       speaker_name, word_count, 1.0 AS score
                FROM turns t
                {where_sql}
                {"AND" if where_clauses else "WHERE"} regexp_matches(t.turn_text, ?)
                LIMIT ? OFFSET ?
            """
            params.append(query)
            result = con.execute(sql, params + [limit, offset])
        else:
            raise ValueError(f"Invalid search mode: {mode!r}. Use 'fts', 'exact', or 'regex'.")

        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def search_episodes_by_text(
        self,
        query: str,
        *,
        mode: str = "fts",
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Find episodes containing matching text.

        Args:
            query: Search query string.
            mode: "fts" (BM25 ranked), "exact" (ILIKE), or "regex".
            limit: Maximum number of episodes to return.

        Returns:
            List of dicts with episode_id, podcast_id, match_count, best_score.
        """
        self._ensure_search_db()
        con = self._search_db_con

        if mode == "fts":
            sql = """
                WITH scored AS (
                    SELECT *, fts_main_turns.match_bm25(row_id, ?) AS score
                    FROM turns
                    WHERE score IS NOT NULL
                )
                SELECT episode_id, podcast_id,
                       COUNT(*) AS match_count,
                       MAX(score) AS best_score
                FROM scored
                GROUP BY episode_id, podcast_id
                ORDER BY best_score DESC
                LIMIT ?
            """
            result = con.execute(sql, [query, limit])
        elif mode == "exact":
            sql = """
                SELECT episode_id, podcast_id,
                       COUNT(*) AS match_count,
                       1.0 AS best_score
                FROM turns
                WHERE turn_text ILIKE ?
                GROUP BY episode_id, podcast_id
                ORDER BY match_count DESC
                LIMIT ?
            """
            result = con.execute(sql, [f"%{query}%", limit])
        elif mode == "regex":
            sql = """
                SELECT episode_id, podcast_id,
                       COUNT(*) AS match_count,
                       1.0 AS best_score
                FROM turns
                WHERE regexp_matches(turn_text, ?)
                GROUP BY episode_id, podcast_id
                ORDER BY match_count DESC
                LIMIT ?
            """
            result = con.execute(sql, [query, limit])
        else:
            raise ValueError(f"Invalid search mode: {mode!r}.")

        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    # ------------------------------------------------------------------
    # Speaker name search
    # ------------------------------------------------------------------
    def search_by_speaker_name(
        self,
        name: str,
        *,
        role: Optional[str] = None,
        exact: bool = False,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Find episodes featuring a speaker by name.

        Args:
            name: Speaker name to search for.
            role: Filter by role ("host", "guest", "neither", or None for any).
            exact: If True, require exact name match; otherwise substring.
            limit: Maximum results.

        Returns:
            List of dicts with episode_id, podcast_id, name_original, role.
        """
        self._ensure_speaker_index()
        df = self._speaker_index_df

        name_lower = name.lower().strip()

        if exact:
            mask = df["name_normalized"] == name_lower
        else:
            mask = df["name_normalized"].str.contains(name_lower, na=False)

        if role:
            mask = mask & (df["role"] == role.lower())

        result = df[mask].head(limit)
        return result[["episode_id", "podcast_id", "name_original", "role"]].to_dict(
            orient="records"
        )

    # ------------------------------------------------------------------
    # Concordance / KWIC
    # ------------------------------------------------------------------
    def concordance(
        self,
        word: str,
        *,
        context_words: int = 10,
        speaker_role: Optional[str] = None,
        podcast_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Key Word In Context search.

        Args:
            word: Word or phrase to find.
            context_words: Number of context words on each side.
            speaker_role: Filter by speaker role.
            podcast_id: Filter to a specific podcast.
            limit: Maximum results.

        Returns:
            List of dicts with left_context, keyword, right_context,
            episode_id, podcast_id, speaker_role, speaker_name,
            start_time, end_time.
        """
        self._ensure_search_db()
        con = self._search_db_con

        where_clauses = ["turn_text ILIKE ?"]
        params = [f"%{word}%"]

        if speaker_role:
            where_clauses.append("speaker_role = ?")
            params.append(speaker_role)
        if podcast_id:
            where_clauses.append("podcast_id = ?")
            params.append(podcast_id)

        where_sql = " AND ".join(where_clauses)

        sql = f"""
            SELECT episode_id, podcast_id, turn_text, speaker_role,
                   speaker_name, start_time, end_time
            FROM turns
            WHERE {where_sql}
            LIMIT ?
        """
        result = con.execute(sql, params + [limit])
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        kwic_results = []
        word_pattern = re.compile(re.escape(word), re.IGNORECASE)

        for row in rows:
            row_dict = dict(zip(columns, row))
            text = row_dict["turn_text"]
            match = word_pattern.search(text)
            if not match:
                continue

            # Split into words preserving positions
            words = text.split()
            # Find the word index of the match
            char_pos = match.start()
            word_idx = len(text[:char_pos].split()) - 1
            if word_idx < 0:
                word_idx = 0

            # Count how many words the keyword spans
            kw_word_count = len(word.split())

            left_start = max(0, word_idx - context_words)
            right_end = min(len(words), word_idx + kw_word_count + context_words)

            left_ctx = " ".join(words[left_start:word_idx])
            keyword = " ".join(words[word_idx : word_idx + kw_word_count])
            right_ctx = " ".join(words[word_idx + kw_word_count : right_end])

            kwic_results.append(
                {
                    "left_context": left_ctx,
                    "keyword": keyword,
                    "right_context": right_ctx,
                    "episode_id": row_dict["episode_id"],
                    "podcast_id": row_dict["podcast_id"],
                    "speaker_role": row_dict["speaker_role"],
                    "speaker_name": row_dict["speaker_name"],
                    "start_time": row_dict["start_time"],
                    "end_time": row_dict["end_time"],
                }
            )

        return kwic_results

    # ------------------------------------------------------------------
    # Episode & Turn Metrics
    # ------------------------------------------------------------------
    def get_episode_metrics(self, episode_id: str) -> Optional[Dict[str, Any]]:
        """
        Get precomputed metrics for an episode.

        Args:
            episode_id: The episode to look up.

        Returns:
            Dict of metrics or None if not found.
        """
        self._ensure_episode_metrics_df()
        df = self._episode_metrics_df
        match = df[df["episode_id"] == episode_id]
        if match.empty:
            return None
        return match.iloc[0].to_dict()

    def filter_episodes_by_metrics(
        self,
        *,
        min_word_count: Optional[int] = None,
        max_word_count: Optional[int] = None,
        min_turn_count: Optional[int] = None,
        max_turn_count: Optional[int] = None,
        min_speaking_rate: Optional[float] = None,
        max_speaking_rate: Optional[float] = None,
        min_discourse_marker_rate: Optional[float] = None,
        max_discourse_marker_rate: Optional[float] = None,
        min_host_proportion: Optional[float] = None,
        max_host_proportion: Optional[float] = None,
        min_avg_gap: Optional[float] = None,
        max_avg_gap: Optional[float] = None,
        limit: int = 10000,
    ) -> List[Dict[str, Any]]:
        """
        Filter episodes by precomputed metrics.

        Args:
            min_word_count / max_word_count: Word count range.
            min_turn_count / max_turn_count: Turn count range.
            min_speaking_rate / max_speaking_rate: Words per second range.
            min_discourse_marker_rate / max_discourse_marker_rate: Markers per 1000 words.
            min_host_proportion / max_host_proportion: Host word proportion range.
            min_avg_gap / max_avg_gap: Average gap duration range.
            limit: Maximum results.

        Returns:
            List of episode metric dicts.
        """
        self._ensure_episode_metrics_df()
        df = self._episode_metrics_df

        if min_word_count is not None:
            df = df[df["total_word_count"] >= min_word_count]
        if max_word_count is not None:
            df = df[df["total_word_count"] <= max_word_count]
        if min_turn_count is not None:
            df = df[df["total_turn_count"] >= min_turn_count]
        if max_turn_count is not None:
            df = df[df["total_turn_count"] <= max_turn_count]
        if min_speaking_rate is not None:
            df = df[df["avg_words_per_second"] >= min_speaking_rate]
        if max_speaking_rate is not None:
            df = df[df["avg_words_per_second"] <= max_speaking_rate]
        if min_discourse_marker_rate is not None:
            df = df[df["discourse_marker_rate"] >= min_discourse_marker_rate]
        if max_discourse_marker_rate is not None:
            df = df[df["discourse_marker_rate"] <= max_discourse_marker_rate]
        if min_host_proportion is not None:
            df = df[df["host_word_proportion"] >= min_host_proportion]
        if max_host_proportion is not None:
            df = df[df["host_word_proportion"] <= max_host_proportion]
        if min_avg_gap is not None:
            df = df[df["avg_gap_duration"] >= min_avg_gap]
        if max_avg_gap is not None:
            df = df[df["avg_gap_duration"] <= max_avg_gap]

        return df.head(limit).to_dict(orient="records")

    def get_turn_metrics(
        self, podcast_id: str, episode_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get precomputed turn-level metrics for an episode.

        Args:
            podcast_id: The podcast partition key.
            episode_id: The episode to look up.

        Returns:
            List of turn metric dicts sorted by turn_count.
        """
        metrics_path = os.path.join(
            self.data_dir,
            "turns",
            f"podcast_id={podcast_id}",
            "metrics.parquet",
        )
        if not os.path.exists(metrics_path):
            raise IndexNotBuiltError(
                f"Turn metrics not found at {metrics_path}. "
                "Build them with: python scripts/build_indexes.py --data-dir "
                f"{self.data_dir} --phase 2"
            )

        import pyarrow.compute as pc

        table = pq.read_table(metrics_path)
        mask = pc.equal(table.column("episode_id"), episode_id)
        table = table.filter(mask)

        if table.num_rows == 0:
            return []

        sort_indices = pc.sort_indices(table, sort_keys=[("turn_count", "ascending")])
        table = table.take(sort_indices)

        cols = table.to_pydict()
        n = table.num_rows
        return [{k: cols[k][i] for k in cols} for i in range(n)]

    # ------------------------------------------------------------------
    # Audio word estimation
    # ------------------------------------------------------------------
    def estimate_word_audio(
        self,
        podcast_id: str,
        episode_id: str,
        word: str,
        occurrence: int = 0,
    ) -> Optional[Dict[str, Any]]:
        """
        Estimate audio time range for a word in a turn.

        Uses character offset and speaking rate for approximate timing.

        Args:
            podcast_id: The podcast partition key.
            episode_id: The episode to search in.
            word: The word to locate.
            occurrence: Which occurrence (0-indexed) to return.

        Returns:
            Dict with mp3_url, estimated_start, estimated_end,
            turn_start, turn_end, turn_text, confidence.
            None if the word is not found.
        """
        turns = self.get_turns_for_episode(podcast_id, episode_id)
        if not turns:
            return None

        word_lower = word.lower()
        found_count = 0

        for turn in turns:
            text = str(turn.get("turn_text", ""))
            text_lower = text.lower()

            # Find all occurrences of the word in this turn
            start_search = 0
            while True:
                idx = text_lower.find(word_lower, start_search)
                if idx == -1:
                    break

                if found_count == occurrence:
                    # Estimate timing based on character position
                    turn_start = float(turn.get("start_time", 0))
                    turn_end = float(turn.get("end_time", 0))
                    turn_duration = turn_end - turn_start

                    if turn_duration <= 0 or len(text) == 0:
                        return None

                    # Linear interpolation based on character offset
                    char_ratio_start = idx / len(text)
                    char_ratio_end = (idx + len(word)) / len(text)

                    est_start = turn_start + char_ratio_start * turn_duration
                    est_end = turn_start + char_ratio_end * turn_duration

                    # Confidence: lower for longer turns (less precise)
                    confidence = min(1.0, 10.0 / max(turn_duration, 1.0))

                    return {
                        "mp3_url": turn.get("mp3_url", ""),
                        "estimated_start": round(est_start, 2),
                        "estimated_end": round(est_end, 2),
                        "turn_start": turn_start,
                        "turn_end": turn_end,
                        "turn_text": text,
                        "confidence": round(confidence, 3),
                    }

                found_count += 1
                start_search = idx + 1

        return None

    # ------------------------------------------------------------------
    # Counts
    # ------------------------------------------------------------------
    @property
    def num_podcasts(self) -> int:
        return self._num_podcasts

    @property
    def num_episodes(self) -> int:
        return self._num_episodes

    @property
    def manifest(self) -> Dict[str, Any]:
        return dict(self._manifest)
