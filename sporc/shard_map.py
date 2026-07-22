"""
Locating a podcast's data within the packed Parquet layout.

Up to v1.0 the dataset gave every podcast its own directory, so finding a
podcast's turns meant building a path from its id. That produced roughly 685,000
files, which is far past what the Hub serves comfortably: fetching in bulk ran
into request rate limits and failed with HTTP 429.

The data now lives in a few hundred large part files, sorted by category and
then by podcast id, with **one row group per podcast**. ``shard_map.parquet``
records where each podcast landed, so a lookup is a dict access and a read is a
single row group rather than a whole file.

Only the trees actually used are held in memory. Most work touches episodes and
turn text and never asks for acoustics, and materialising all four trees at once
costs more than the metadata catalogs the package downloads to start with.
"""

import logging
from typing import Dict, List, NamedTuple, Optional, Set

logger = logging.getLogger(__name__)


class Location(NamedTuple):
    """Where one podcast's rows sit inside a tree."""

    part: str
    row_group: int
    num_rows: int


#: Tree name in the shard map -> directory holding that tree's part files.
TREE_DIRS = {
    "episodes": "episodes",
    "turns_text": "turns/text",
    "turns_metrics": "turns/metrics",
    "acoustics": "acoustics",
}


class ShardMap:
    """
    Index from podcast id to the part file and row group holding its rows.

    Args:
        path: Local path to ``metadata/shard_map.parquet``.
    """

    def __init__(self, path: str) -> None:
        self._path = path
        self._trees: Dict[str, Dict[str, Location]] = {}

    def _load(self, tree: str) -> Dict[str, Location]:
        """Read one tree's entries, keeping the other trees out of memory."""
        cached = self._trees.get(tree)
        if cached is not None:
            return cached

        import pyarrow.parquet as pq

        pf = pq.ParquetFile(self._path)
        table = None
        # The file is written one row group per tree, so the right rows can be
        # found from the column statistics without reading or comparing the
        # other trees' rows. Filtering the whole table instead costs seconds,
        # and this runs on the first lookup of a session.
        tree_col = pf.schema_arrow.names.index("tree")
        for g in range(pf.metadata.num_row_groups):
            stats = pf.metadata.row_group(g).column(tree_col).statistics
            if stats is not None and stats.min == tree == stats.max:
                table = pf.read_row_group(
                    g, columns=["podcast_id", "part", "row_group", "num_rows"])
                break
        if table is None:
            # Older revisions wrote the map as a single mixed row group.
            import pyarrow.compute as pc
            full = pf.read(columns=["podcast_id", "tree", "part",
                                    "row_group", "num_rows"])
            table = full.filter(pc.equal(full.column("tree"), tree))

        # Part names repeat across every podcast in the same file; interning
        # them keeps this to one string per file rather than one per podcast.
        # Plain tuples rather than Location: this builds ~200,000 of them on a
        # cold lookup, and constructing the NamedTuple instead costs half a
        # second of startup to save a conversion that only the caller sees.
        parts: Dict[str, str] = {}
        setdefault = parts.setdefault
        out = {pid: (setdefault(part, part), rg, n)
               for pid, part, rg, n in zip(table.column("podcast_id").to_pylist(),
                                           table.column("part").to_pylist(),
                                           table.column("row_group").to_pylist(),
                                           table.column("num_rows").to_pylist())}

        logger.debug("Loaded %d %s entries across %d parts",
                     len(out), tree, len(parts))
        self._trees[tree] = out
        return out

    def locate(self, tree: str, podcast_id: str) -> Optional[Location]:
        """Where *podcast_id* lives in *tree*, or None if it has no rows there."""
        entry = self._load(tree).get(podcast_id)
        return Location(*entry) if entry is not None else None

    def has(self, tree: str, podcast_id: str) -> bool:
        """
        Whether *tree* holds any rows for *podcast_id*.

        This answers in memory what used to need a file probe -- and, against
        the Hub, an HTTP request per podcast.
        """
        return podcast_id in self._load(tree)

    def podcast_ids(self, tree: str) -> Set[str]:
        """Every podcast id present in *tree*."""
        return set(self._load(tree))

    def items(self, tree: str):
        """Iterate ``(podcast_id, Location)`` for every podcast in *tree*."""
        for pid, entry in self._load(tree).items():
            yield pid, Location(*entry)

    def parts(self, tree: str) -> List[str]:
        """Every part file name in *tree*, in order."""
        # Entries are stored as plain tuples; index 0 is the part name.
        return sorted({entry[0] for entry in self._load(tree).values()})

    def parts_for(self, tree: str, podcast_ids) -> List[str]:
        """
        The part files needed to cover *podcast_ids*.

        A part holds on the order of a thousand podcasts, so a set of podcasts
        from one category usually resolves to a handful of files.
        """
        entries = self._load(tree)
        return sorted({entries[p][0] for p in podcast_ids if p in entries})

    def relpath(self, tree: str, part: str) -> str:
        """Repo-relative path of a part file."""
        return f"{TREE_DIRS[tree]}/{part}"

    def __repr__(self) -> str:
        loaded = ", ".join(f"{t}={len(v):,}" for t, v in self._trees.items())
        return f"ShardMap({self._path!r}, loaded={{{loaded}}})"
