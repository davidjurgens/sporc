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
import time
from contextlib import contextmanager
from typing import Optional

from .exceptions import DataNotLocalError

logger = logging.getLogger(__name__)

# Retry policy for the Hub's rate limit. Windows are five minutes long, so a
# handful of attempts with a capped wait covers one closed window without
# hanging indefinitely on a problem that will not resolve itself.
_MAX_RETRIES = 5
_RETRY_BASE_SECONDS = 10.0
_RETRY_MAX_SECONDS = 300.0


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

    def read_columns(self, relpath: str, columns):
        """
        Read *columns* from every row group of the Parquet file at *relpath*.

        The default resolves the whole file with :meth:`path` and then projects,
        which is right when the file is already local. :class:`HubDataSource`
        overrides it to range-read only the columns asked for, so a
        column-projected scan of the corpus does not pull every part in full.

        Returns None if the file does not exist in the dataset.
        """
        return self._read_projected(
            relpath, lambda pf: pf.read(columns=columns))

    def read_row_group_columns(self, relpath: str, row_group: int, columns):
        """
        Read *columns* from a single *row_group* of the file at *relpath*.

        One podcast is one row group, so this is how a projected per-podcast
        probe -- "which of this podcast's episodes have turns" -- avoids paying
        for the whole part it lives in. As with :meth:`read_columns`, the Hub
        source range-reads rather than downloading the part.

        Returns None if the file does not exist in the dataset.
        """
        return self._read_projected(
            relpath, lambda pf: pf.read_row_group(row_group, columns=columns))

    def _read_projected(self, relpath: str, reader):
        """Apply *reader* to a ParquetFile for *relpath*, or None if absent."""
        import pyarrow.parquet as pq

        p = self.path(relpath)
        if p is None:
            return None
        return reader(pq.ParquetFile(p))

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
            p = self._download_with_retry(hf_hub_download, relpath)
        except EntryNotFoundError:
            # The dataset genuinely has no such file, as opposed to a transient
            # failure.
            self._absent.add(relpath)
            return None

        self._fetched += 1
        logger.debug("Fetched %s", relpath)
        return p

    def _download_with_retry(self, hf_hub_download, relpath: str) -> str:
        """Fetch one whole file, waiting out the Hub's rate limit."""
        return self._retry_on_rate_limit(
            lambda: hf_hub_download(
                repo_id=self.repo_id,
                repo_type="dataset",
                filename=relpath,
                token=self.token,
                cache_dir=self.cache_dir,
            ),
            relpath,
        )

    def _retry_on_rate_limit(self, call, relpath: str):
        """
        Run *call*, waiting out the Hub's rate limit rather than failing.

        The Hub counts requests in fixed five-minute windows and answers 429
        once a window is spent. That is a wait, not an error: the window always
        reopens. Treating it as a failure is what turned a large prefetch into a
        partial download with an exception on the end.

        The backoff is capped rather than unbounded so a genuinely broken
        setup -- a revoked token, a repo that no longer exists -- still fails in
        a bounded time instead of retrying all afternoon.
        """
        from huggingface_hub.errors import HfHubHTTPError

        for attempt in range(_MAX_RETRIES):
            try:
                return call()
            except HfHubHTTPError as exc:
                status = getattr(getattr(exc, "response", None), "status_code",
                                 None)
                if status != 429 or attempt == _MAX_RETRIES - 1:
                    raise
                # Prefer the server's own figure; it knows when the window ends.
                retry_after = getattr(
                    getattr(exc, "response", None), "headers", {}
                ).get("Retry-After")
                try:
                    delay = float(retry_after)
                except (TypeError, ValueError):
                    delay = _RETRY_BASE_SECONDS * (2 ** attempt)
                delay = min(delay, _RETRY_MAX_SECONDS)
                logger.warning(
                    "Rate limited by the Hub while fetching %s; waiting %.0fs "
                    "(attempt %d of %d). Reduce concurrency if this repeats.",
                    relpath, delay, attempt + 1, _MAX_RETRIES,
                )
                time.sleep(delay)
        raise AssertionError("unreachable")  # pragma: no cover

    def _filesystem(self):
        """A cached HfFileSystem for range reads against this repo."""
        fs = getattr(self, "_fs", None)
        if fs is None:
            from huggingface_hub import HfFileSystem
            fs = HfFileSystem(token=self.token)
            self._fs = fs
        return fs

    def _read_projected(self, relpath: str, reader):
        """
        Range-read a projection from *relpath* without materializing the file.

        A column-projected read otherwise pays for whole part files: ``path()``
        fetches the entire object and the projection then discards ~99% of it.
        Reading through :class:`HfFileSystem` lets Parquet fetch just the footer
        and the requested column chunks over HTTP range requests, so a whole-
        corpus scan costs a few small reads per part rather than tens of
        megabytes, and a per-podcast probe costs one row group rather than the
        part it sits in.

        A file already on disk is read locally -- there is nothing to range over
        and the whole object is present. Unlike ``path()``, a range read does
        not persist the file to the snapshot cache, which is the point: a probe
        should not leave whole parts behind that nothing else asked for.
        """
        if relpath in self._absent:
            return None

        import pyarrow.parquet as pq

        local = os.path.join(self.root, relpath)
        if os.path.exists(local):
            return reader(pq.ParquetFile(local))

        if not self.allow_downloads:
            raise DataNotLocalError(
                f"{relpath} is not present locally and downloads are disabled "
                "(allow_downloads=False)."
            )

        from huggingface_hub.errors import EntryNotFoundError

        fs = self._filesystem()
        hub_path = f"datasets/{self.repo_id}/{relpath}"

        def _read():
            try:
                with fs.open(hub_path, "rb") as fh:
                    return reader(pq.ParquetFile(fh))
            except FileNotFoundError:
                raise EntryNotFoundError(f"{relpath} not found in repo")

        try:
            table = self._retry_on_rate_limit(_read, relpath)
        except EntryNotFoundError:
            self._absent.add(relpath)
            return None
        self._fetched += 1
        return table

    def __repr__(self) -> str:
        return (f"HubDataSource({self.repo_id!r}, fetched={self._fetched}, "
                f"allow_downloads={self.allow_downloads})")
