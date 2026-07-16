"""
Resolution of dataset files, either from a local directory or lazily from the
HuggingFace Hub.

The Parquet layout is Hive-partitioned by ``podcast_id``, so a single podcast's
episodes and turns live in a handful of files. That makes it possible to fetch
only the partitions actually touched instead of the whole ~57 GB corpus, which
is what :class:`HubDataSource` does.

Downloaded files land in the same snapshot directory that ``snapshot_download``
would populate, so a lazily-built cache and a fully downloaded one are the same
thing on disk.
"""

import logging
import os
from contextlib import contextmanager
from typing import Optional

from .exceptions import DataNotLocalError

logger = logging.getLogger(__name__)


class DataSource:
    """A source of dataset files, addressed by repo-relative path."""

    #: Directory that repo-relative paths resolve against.
    root: str

    def path(self, relpath: str) -> Optional[str]:
        """
        Resolve *relpath* to a local file, fetching it if necessary.

        Returns:
            Local path, or None if the file does not exist in the dataset.

        Raises:
            DataNotLocalError: if the file is absent locally and this source is
                not permitted to fetch it.
        """
        raise NotImplementedError

    def exists_locally(self, relpath: str) -> bool:
        """Whether *relpath* is already on disk, without fetching anything."""
        return os.path.exists(os.path.join(self.root, relpath))

    @contextmanager
    def downloads_enabled(self):
        """
        Permit downloading inside this block, whatever the source's setting.

        Used to honour an explicit prefetch request even when the source is
        otherwise pinned to local data.
        """
        yield


class LocalDataSource(DataSource):
    """Files from a directory already on disk. Never downloads."""

    def __init__(self, root: str) -> None:
        self.root = root

    def path(self, relpath: str) -> Optional[str]:
        p = os.path.join(self.root, relpath)
        return p if os.path.exists(p) else None

    def __repr__(self) -> str:
        return f"LocalDataSource({self.root!r})"


class HubDataSource(DataSource):
    """
    Files from a HuggingFace dataset repo, downloaded on first use.

    Args:
        repo_id: Dataset repo, e.g. ``blitt/SPoRC``.
        root: Snapshot directory that already-downloaded files live in.
        token: HuggingFace auth token.
        cache_dir: HuggingFace cache location.
        allow_downloads: When False, resolve only files that are already local
            and raise :class:`DataNotLocalError` for anything else.
    """

    def __init__(self, repo_id: str, root: str, token: Optional[str] = None,
                 cache_dir: Optional[str] = None,
                 allow_downloads: bool = True) -> None:
        self.repo_id = repo_id
        self.root = root
        self.token = token
        self.cache_dir = cache_dir
        self.allow_downloads = allow_downloads
        # Paths the repo does not contain. Remembered so that repeated lookups
        # for a podcast without turns don't re-request a known 404.
        self._absent: set = set()
        self._fetched: int = 0

    @property
    def fetch_count(self) -> int:
        """Number of files downloaded on demand by this source."""
        return self._fetched

    @contextmanager
    def downloads_enabled(self):
        """Temporarily allow downloads, restoring the prior setting after."""
        previous = self.allow_downloads
        self.allow_downloads = True
        try:
            yield
        finally:
            self.allow_downloads = previous

    def path(self, relpath: str) -> Optional[str]:
        if relpath in self._absent:
            return None

        local = os.path.join(self.root, relpath)
        if os.path.exists(local):
            return local

        if not self.allow_downloads:
            raise DataNotLocalError(
                f"{relpath} is not present locally and downloads are disabled "
                "(allow_downloads=False). Re-create the dataset with "
                "allow_downloads=True, or include this data in `subset=` at "
                "load time, to fetch it."
            )

        from huggingface_hub import hf_hub_download
        from huggingface_hub.errors import EntryNotFoundError

        try:
            p = hf_hub_download(
                repo_id=self.repo_id,
                repo_type="dataset",
                filename=relpath,
                token=self.token,
                cache_dir=self.cache_dir,
            )
        except EntryNotFoundError:
            # The dataset genuinely has no such file (e.g. a podcast with no
            # turns), as opposed to a transient failure.
            self._absent.add(relpath)
            return None

        self._fetched += 1
        logger.debug("Fetched %s", relpath)
        return p

    def __repr__(self) -> str:
        return (f"HubDataSource({self.repo_id!r}, fetched={self._fetched}, "
                f"allow_downloads={self.allow_downloads})")
