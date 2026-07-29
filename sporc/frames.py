"""
Getting a table out of the corpus.

The object model answers questions about one episode well. Above that level it
is the wrong shape: walking it costs a Parquet footer parse per podcast, so
building twelve-turn conversation windows across a genre slice takes minutes
through ``Episode.sliding_window`` and about a second through a DataFrame. This
module is the second route -- the same files, read columnar.

Everything here is a free function taking a :class:`~sporc.parquet_backend.
ParquetBackend`; :class:`~sporc.dataset.SPORCDataset` exposes them as methods.

Three things worth knowing before you use it:

- **Frames are never cached.** Notebooks mutate what they get back
  (``turns["win"] = ...``), so handing two cells the same object would make the
  second cell's result depend on whether the first ran. Every call re-reads, and
  the catalog accessors copy. Re-reading a genre slice costs about a second.
- **``columns=`` is the argument that matters.** The full turns table is 710
  bytes a row; the three columns most analyses start from are 85. On the whole
  corpus that is the difference between 131 GB and 16 GB, which is why an
  unbounded request is refused rather than attempted.
- **``turns_frame`` sorts by ``(episode_id, start_time, turn_count)``**, which
  is the order ``Episode.turns`` uses. Sorting by ``turn_count`` instead --
  the obvious choice, since it is the join key -- disagrees on about 3.7% of
  episodes, because ``start_time`` is not monotone in it.
"""

import logging
from typing import TYPE_CHECKING, Iterable, Iterator, List, Optional

from . import schema
from .exceptions import FrameTooLargeError

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

    from .parquet_backend import ParquetBackend

logger = logging.getLogger(__name__)

#: Estimated in-memory size past which a frame request is only warned about.
FRAME_WARN_BYTES = 2 * 1024 ** 3

#: Estimated in-memory size past which a frame request is refused outright.
#: Refused rather than warned about because the failure being prevented is the
#: operating system killing a notebook kernel, which takes unsaved work with it
#: -- strictly worse than an exception you can read and act on.
FRAME_MAX_BYTES = 8 * 1024 ** 3

#: Columns the turns/metrics and acoustics trees join back on. Both, not just
#: ``turn_count``: that is unique only within an episode, so joining a whole
#: frame on it alone collapses almost every row.
_TURN_KEYS = ["episode_id", "turn_count"]

#: Columns :func:`add_speaker_columns` derives from.
_SPEAKER_SOURCES = ["speaker", "inferred_speaker_name", "inferred_speaker_role"]


def _require_pandas():
    """Import pandas, or explain why it is needed."""
    try:
        import pandas as pd
    except ImportError:  # pragma: no cover - exercised by hand, not in CI
        raise ImportError(
            "The frame API needs pandas. It is a dependency of sporc from "
            "version 1.2; an environment installed against 1.1.4 or earlier "
            "may not have it. Install it with: pip install pandas"
        )
    return pd


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------

def _check_size(table: str, columns: Optional[List[str]], rows: int,
                *, what: str, allow_large: bool) -> None:
    """
    Refuse a frame that would not fit in memory, before reading anything.

    The row count is exact (the shard map records it) and the width is estimated
    from the registry, so this costs no I/O and is wrong only about the width.
    """
    nbytes = schema.estimate_bytes(table, columns, rows)
    if nbytes < FRAME_WARN_BYTES or allow_large:
        return

    ncols = len(columns) if columns else len(schema.COLUMNS[
        schema.resolve_table(table)])
    size = schema.format_bytes(nbytes)
    summary = (f"{what} would materialize {rows:,} rows x {ncols} columns "
               f"~= {size} in memory (estimated)")

    if nbytes < FRAME_MAX_BYTES:
        logger.warning(
            "%s. Pass columns= to narrow it if the machine is tight.", summary)
        return

    widest = _widest_column(table, columns)
    raise FrameTooLargeError(
        f"{summary}."
        + (f" {widest} alone is {schema.COLUMNS[schema.resolve_table(table)][widest].nbytes}"
           " bytes/row." if widest else "")
        + "\nBound it with one of:"
        "\n  - podcast_ids=[...] or episode_ids=[...]   restrict to what you need"
        "\n  - SPORCDataset(subset=[...])               pins every frame to that slice"
        "\n  - columns=[...]                            most analyses need a handful"
        "\n  - iter_turns_frames(...)                   one DataFrame per part file"
        "\n  - allow_large=True                         if the machine can take it"
    )


def _widest_column(table: str, columns: Optional[List[str]]) -> Optional[str]:
    """The column contributing most to a request's size, for the error text."""
    known = schema.COLUMNS[schema.resolve_table(table)]
    names = list(known) if columns is None else [c for c in columns if c in known]
    if not names:
        return None
    return max(names, key=lambda c: known[c].nbytes)


def _with_keys(columns: Optional[List[str]], keys: Iterable[str]
               ) -> Optional[List[str]]:
    """
    Add *keys* to a projection that omits them.

    A join needs its keys, and silently dropping them afterwards would leave a
    frame nobody can join back to anything. Documented rather than hidden: the
    result may have columns you did not ask for.
    """
    if columns is None:
        return None
    out = list(columns)
    for key in keys:
        if key not in out:
            out.append(key)
    return out


# ---------------------------------------------------------------------------
# Catalogs
# ---------------------------------------------------------------------------

def list_catalogs() -> List[str]:
    """Every catalog readable with :func:`catalog_frame` / :func:`load_catalog`."""
    return schema.list_catalogs()


def list_tables() -> List[str]:
    """Every table the column registry describes."""
    return schema.list_tables()


def describe_columns(table: str) -> "pd.DataFrame":
    """
    What is in *table*: one row per column, with name, dtype and description.

    Answers from the static registry, so it needs no dataset, no network and no
    credentials. The acoustic feature names in particular
    (``f0_semitone_from_27_5hz_sma3nz_mean``) are long enough that people would
    rather look them up than type them from memory.
    """
    pd = _require_pandas()
    canonical = schema.resolve_table(table)
    cols = schema.COLUMNS[canonical]
    return pd.DataFrame(
        [{"name": name, "dtype": spec.dtype, "bytes_per_row": spec.nbytes,
          "description": spec.description}
         for name, spec in cols.items()]
    )


def catalog_frame(backend: "ParquetBackend", name: str,
                  columns: Optional[List[str]] = None) -> "pd.DataFrame":
    """
    Read a metadata catalog as a DataFrame.

    **Always corpus-wide.** Unlike :func:`episodes_frame` and its siblings, this
    ignores any ``subset=`` pinning: the catalogs describe all 228,099 podcasts
    and being able to reach past a slice is the reason to have them. A
    cross-genre guest network, for instance, needs the whole guest index while
    the loaded dataset is one genre.

    They are also the cheapest thing in the corpus -- the guest index is about a
    megabyte -- and arrive with any Hub-backed load.
    """
    _require_pandas()
    return backend.read_catalog(name, columns=columns).to_pandas()


def load_catalog(name: str, columns: Optional[List[str]] = None, *,
                 parquet_dir: Optional[str] = None,
                 cache_dir: Optional[str] = None,
                 use_auth_token: Optional[str] = None) -> "pd.DataFrame":
    """
    Read a catalog without constructing a dataset.

    Reading a one-megabyte index should not require loading a corpus, and
    should certainly not require knowing where HuggingFace keeps its cache::

        from sporc import load_catalog
        guests = load_catalog("guest_index")

    Fetches exactly the one file, through ``huggingface_hub``, so the cache
    layout stays that library's business.

    Args:
        name: Catalog name; see :func:`list_catalogs`. Aliases accepted.
        columns: Project to these columns.
        parquet_dir: Read from a local layout instead of the Hub.
        cache_dir: HuggingFace cache location.
        use_auth_token: HuggingFace token, for gated access.
    """
    import os

    import pyarrow.parquet as pq

    _require_pandas()
    canonical = schema.resolve_table(name)
    cols = schema.validate_columns(canonical, columns)
    rel = schema.catalog_file(canonical)

    if parquet_dir is not None:
        path = os.path.join(parquet_dir, rel)
        if not os.path.exists(path):
            from .exceptions import IndexNotBuiltError
            raise IndexNotBuiltError(
                f"{schema.catalog_label(canonical)} not found at {path}. "
                f"{schema.catalog_hint(canonical, parquet_dir)}"
            )
    else:
        from .dataset import SPORCDataset
        path = SPORCDataset._download_one(
            rel, token=use_auth_token, cache_dir=cache_dir)
        if path is None:
            from .exceptions import IndexNotBuiltError
            raise IndexNotBuiltError(
                f"{SPORCDataset.DATASET_ID} has no {rel}. "
                f"{schema.catalog_hint(canonical)}"
            )

    return pq.read_table(path, columns=cols).to_pandas()


# ---------------------------------------------------------------------------
# Derived columns
# ---------------------------------------------------------------------------

def add_speaker_columns(df: "pd.DataFrame", *, copy: bool = True
                        ) -> "pd.DataFrame":
    """
    Add the vectorised equivalents of the :class:`~sporc.turn.Turn` speaker
    properties.

    Adds ``n_speakers``, ``is_overlapping``, ``primary_speaker``, ``is_host``,
    ``is_guest``, ``has_inferred_speaker`` and ``has_inferred_role``, skipping
    any whose source column is absent.

    This exists because both of the columns it reads are easy to get wrong:

    - ``speaker`` is a **list** per turn, which the column name does not say,
      and about 45% of turns have more than one entry. The single-speaker test
      is the precondition for any within-speaker comparison, and writing it as
      ``speaker.str.len() == 1`` by hand quietly discards nearly half the data.
    - ``inferred_speaker_name`` and ``inferred_speaker_role`` hold sentinels
      rather than nulls, so ``notna()`` keeps every unattributed turn.

    Works on any turns-shaped frame, including one built with
    ``pd.read_parquet``.
    """
    pd = _require_pandas()
    from .constants import PLACEHOLDER_SPEAKERS

    out = df.copy() if copy else df

    if "speaker" in out.columns:
        # .str works on list cells too: len() is the list length and [0] the
        # first element, NaN where the list is empty -- which matches
        # Turn.primary_speaker returning None for a turn attributed to nobody.
        lengths = out["speaker"].str.len()
        out["n_speakers"] = lengths.fillna(0).astype("int64")
        out["is_overlapping"] = out["n_speakers"] > 1
        out["primary_speaker"] = out["speaker"].str[0]

    if "inferred_speaker_role" in out.columns:
        role = out["inferred_speaker_role"]
        out["is_host"] = role == "host"
        out["is_guest"] = role == "guest"
        out["has_inferred_role"] = role.notna() & ~role.isin(
            PLACEHOLDER_SPEAKERS) & (role != "")

    if "inferred_speaker_name" in out.columns:
        name = out["inferred_speaker_name"]
        out["has_inferred_speaker"] = name.notna() & ~name.isin(
            PLACEHOLDER_SPEAKERS) & (name != "")

    return out


def parse_episode_dates(df: "pd.DataFrame", column: str = "episode_date",
                        *, copy: bool = True) -> "pd.DataFrame":
    """
    Turn a millisecond-epoch date column into real timestamps.

    ``episode_catalog.episode_date`` is a **string** holding a millisecond Unix
    epoch, and so is ``episodes.episode_date_localized`` -- they are the same
    value despite the names. Passing either to ``pd.to_datetime`` directly
    yields dates in 1970 and raises nothing, which is the kind of wrong that
    survives review.

    Replaces *column* with a UTC-aware ``datetime64`` and adds a ``day`` column.
    UTC rather than local time on purpose: rendering in whatever timezone the
    machine happens to be set to puts about 13.6% of episodes on a different
    calendar day depending on where the code runs.
    """
    pd = _require_pandas()
    out = df.copy() if copy else df
    if column not in out.columns:
        return out
    ms = pd.to_numeric(out[column], errors="coerce").astype("Int64")
    out[column] = pd.to_datetime(ms, unit="ms", utc=True)
    out["day"] = out[column].dt.date
    return out


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------

def _empty_frame(table: str, columns: Optional[List[str]]) -> "pd.DataFrame":
    """A correctly-shaped empty frame, so downstream code need not special-case."""
    pd = _require_pandas()
    known = schema.COLUMNS[schema.resolve_table(table)]
    names = list(known) if columns is None else list(columns)
    return pd.DataFrame({n: pd.Series(dtype="object") for n in names})


def _sort_turns(df: "pd.DataFrame") -> "pd.DataFrame":
    """
    Order turns the way ``Episode.turns`` does.

    By ``start_time`` within an episode, with ``turn_count`` breaking ties. Not
    by ``turn_count`` alone: it is the join key and it is unique within an
    episode, but ``start_time`` is non-monotone in it for about 3.7% of
    episodes, so the two orders genuinely differ. Matching the object model is
    what lets ``window_frame`` and ``Episode.sliding_window`` agree.
    """
    keys = [k for k in ("episode_id", "start_time", "turn_count")
            if k in df.columns]
    if not keys:
        return df
    return df.sort_values(keys, kind="stable").reset_index(drop=True)


def _join_turn_side(turns: "pd.DataFrame", side: "pd.DataFrame", what: str
                    ) -> "pd.DataFrame":
    """
    Left-join metrics or acoustics onto turns on ``(episode_id, turn_count)``.

    Left rather than inner. The request that prompted this API asked for inner,
    but coverage of both trees is partial, and an inner join silently deletes
    text turns wherever a metric is missing -- the same shape of quiet loss as
    testing a sentinel column for null. A left join keeps the turn and puts NaN
    in the metric, which is visible.
    """
    if side.empty:
        return turns
    # Dedupe on both keys. The per-episode path in the backend gets away with
    # turn_count alone because its frame is already one episode; across a frame
    # that key collapses nearly everything. 81,807 turns in the corpus are
    # stored more than once, duplicated verbatim in 1.0 and carried forward, so
    # without this a merge multiplies rows on both sides.
    side = side.drop_duplicates(subset=_TURN_KEYS)
    side = side.drop(columns=["podcast_id"], errors="ignore")

    before = len(turns)
    out = turns.merge(side, on=_TURN_KEYS, how="left", suffixes=("", f"_{what}"))
    if len(out) != before:  # pragma: no cover - guarded by the dedupe above
        logger.warning(
            "Joining %s changed the row count from %d to %d; the keys are not "
            "unique.", what, before, len(out))

    probe = next((c for c in side.columns if c not in _TURN_KEYS), None)
    if probe is not None and before:
        unmatched = out[probe].isna().sum() / before
        if unmatched > 0.01:
            logger.info(
                "%.1f%% of turns have no %s row; those columns are NaN there. "
                "Coverage of this tree is partial.", 100 * unmatched, what)
    return out


def turns_frame(backend: "ParquetBackend", columns: Optional[List[str]] = None,
                *, metrics: bool = False, acoustics: bool = False,
                speakers: bool = False,
                podcast_ids: Optional[Iterable[str]] = None,
                episode_ids: Optional[Iterable[str]] = None,
                sort: bool = True,
                allow_large: bool = False) -> "pd.DataFrame":
    """
    Every turn in the current view, as a DataFrame.

    Args:
        columns: Project to these columns. **Read this argument first**: the
            full table is about 710 bytes a row, of which ``turn_text`` is 265;
            ``["episode_id", "turn_count", "start_time"]`` is 85. Join keys are
            added back if you leave them out.
        metrics: Left-join ``turns/metrics`` (word counts, gaps, speaking rate).
        acoustics: Left-join the twelve eGeMAPSv2 acoustic summaries.
        speakers: Add the derived speaker columns; see
            :func:`add_speaker_columns`.
        podcast_ids: Restrict to these podcasts. Intersected with any
            ``subset=`` pinning.
        episode_ids: Restrict to these episodes. Applied after the read, so it
            narrows rows but not the podcasts fetched -- pass ``podcast_ids``
            too if you know them.
        sort: Order by ``(episode_id, start_time, turn_count)``. Turning this
            off is faster and gives file order.
        allow_large: Skip the size guard.

    Raises:
        FrameTooLargeError: if the request would not fit in memory. The message
            names the ways to bound it.

    Note:
        The result is a fresh object every call -- nothing here is cached -- so
        it is safe to add columns to it.

    Note:
        One row per turn **in the file**, which is not quite what the object
        model gives you. Building ``Episode.turns`` silently skips two kinds of
        row: turns with empty text (0.16%) and turns with ``end_time <=
        start_time`` (0.22%). Together that is 0.33% of turns, but they are
        spread across 23% of episodes, so turn counts from the two routes
        differ more often than the overall rate suggests.

        This frame keeps them, because they are in the data. To get the object
        model's answer instead::

            turns = turns[(turns.turn_text.str.strip() != "")
                          & (turns.end_time > turns.start_time)]
    """
    pd = _require_pandas()

    cols = schema.validate_columns("turns", columns)
    ids = backend.resolve_podcast_ids(podcast_ids)
    rows = backend.tree_row_count("turns_text", ids)
    _check_size("turns", cols, rows, what="turns_frame()",
                allow_large=allow_large)

    read_cols = _with_keys(cols, _TURN_KEYS) if (metrics or acoustics) else cols
    if speakers:
        # The derived columns are computed from these; asking for them without
        # reading their sources would silently produce a frame missing most of
        # what was asked for.
        read_cols = _with_keys(read_cols, _SPEAKER_SOURCES)
    if sort and read_cols is not None and "start_time" not in read_cols:
        read_cols = read_cols + ["start_time"]

    table = backend.scan_tree("turns_text", podcast_ids=ids, columns=read_cols)
    if table.num_rows == 0:
        return _empty_frame("turns", cols)
    df = table.to_pandas()

    if episode_ids is not None:
        df = df[df["episode_id"].isin(set(episode_ids))]

    if metrics:
        df = _join_turn_side(
            df, backend.scan_tree("turns_metrics", podcast_ids=ids).to_pandas(),
            "metrics")
    if acoustics:
        df = _join_turn_side(
            df, backend.scan_tree("acoustics", podcast_ids=ids).to_pandas(),
            "acoustics")

    if sort:
        df = _sort_turns(df)
    if speakers:
        df = add_speaker_columns(df, copy=False)
    return df.reset_index(drop=True)


def iter_turns_frames(backend: "ParquetBackend",
                      columns: Optional[List[str]] = None, *,
                      metrics: bool = False, acoustics: bool = False,
                      speakers: bool = False,
                      podcast_ids: Optional[Iterable[str]] = None,
                      sort: bool = True) -> Iterator["pd.DataFrame"]:
    """
    The same turns, one DataFrame per part file.

    For work that genuinely spans the whole corpus. Each part holds on the order
    of a thousand podcasts, so memory is bounded by part size however large the
    dataset is -- which is why this needs no size guard and
    :func:`turns_frame` does.

    The frames concatenate back to what :func:`turns_frame` returns, except that
    sorting is within a part rather than across all of them::

        totals = Counter()
        for chunk in ds.iter_turns_frames(columns=["inferred_speaker_role"]):
            totals.update(chunk.inferred_speaker_role.value_counts().to_dict())

    Yields nothing if no part covers the requested podcasts.
    """
    _require_pandas()
    cols = schema.validate_columns("turns", columns)
    ids = backend.resolve_podcast_ids(podcast_ids)
    parts = backend.tree_parts("turns_text", ids)

    read_cols = _with_keys(cols, _TURN_KEYS) if (metrics or acoustics) else cols
    if speakers:
        read_cols = _with_keys(read_cols, _SPEAKER_SOURCES)
    if sort and read_cols is not None and "start_time" not in read_cols:
        read_cols = read_cols + ["start_time"]

    logger.info("Iterating %d part file(s) of turns", len(parts))
    for part in parts:
        table = backend.scan_tree("turns_text", podcast_ids=ids,
                                  columns=read_cols, parts=[part])
        if table.num_rows == 0:
            continue
        df = table.to_pandas()

        # Join per part: the podcasts in one turns part sit in the same
        # metrics and acoustics part, so this stays bounded too.
        if metrics:
            df = _join_turn_side(
                df, _side_for_part(backend, "turns_metrics", ids, df),
                "metrics")
        if acoustics:
            df = _join_turn_side(
                df, _side_for_part(backend, "acoustics", ids, df), "acoustics")

        if sort:
            df = _sort_turns(df)
        if speakers:
            df = add_speaker_columns(df, copy=False)
        yield df.reset_index(drop=True)


def _side_for_part(backend: "ParquetBackend", tree: str, ids: List[str],
                   chunk: "pd.DataFrame") -> "pd.DataFrame":
    """
    The metrics/acoustics rows matching one chunk of turns.

    Narrowed to the podcasts actually present in the chunk, so iterating stays
    bounded instead of pulling the whole side tree for every part.
    """
    pd = _require_pandas()
    if "podcast_id" in chunk.columns:
        seen = set(chunk["podcast_id"])
        present = [p for p in ids if p in seen]
    else:
        present = ids
    if not present:
        return pd.DataFrame(columns=_TURN_KEYS)
    return backend.scan_tree(tree, podcast_ids=present).to_pandas()


def turns_dataset(backend: "ParquetBackend", *,
                  podcast_ids: Optional[Iterable[str]] = None):
    """
    A ``pyarrow.dataset.Dataset`` over the turn part files, for out-of-core work.

    Nothing is read until you scan it, so this is the way to run an aggregate
    over more turns than fit in memory::

        import pyarrow.compute as pc
        dset = ds.turns_dataset()
        table = dset.to_table(columns=["inferred_speaker_role"],
                              filter=pc.field("podcast_id") == pid)

    The dataset spans whole part files, so it sees every podcast in them, not
    only the ones requested -- filter on ``podcast_id`` yourself if that
    matters. :func:`turns_frame` and :func:`iter_turns_frames` do that for you.
    """
    import pyarrow.dataset as pads

    paths = backend.part_paths("turns_text", podcast_ids)
    if not paths:
        raise FileNotFoundError(
            "No turn part files are available for the requested podcasts.")
    return pads.dataset(paths, format="parquet")


def episodes_frame(backend: "ParquetBackend",
                   columns: Optional[List[str]] = None, *,
                   parse_dates: bool = True,
                   metrics: bool = False,
                   podcast_ids: Optional[Iterable[str]] = None
                   ) -> "pd.DataFrame":
    """
    The episode catalog for the current view, as a DataFrame.

    Reads ``metadata/episode_catalog.parquet``, not the episodes tree: the
    catalog has everything below except the transcripts, and the transcripts are
    why that tree is 16 GB.

    Args:
        columns: Project to these columns.
        parse_dates: Convert ``episode_date`` from its string millisecond epoch
            to a UTC timestamp and add a ``day`` column. On by default because
            the raw column is a trap -- see :func:`parse_episode_dates`.
        metrics: Left-join ``metadata/episode_metrics.parquet``.
        podcast_ids: Restrict to these podcasts.

    Note:
        Honours ``subset=`` pinning. Use ``catalog("episodes")`` for the
        corpus-wide catalog regardless of the loaded slice.
    """
    pd = _require_pandas()
    cols = schema.validate_columns("episodes", columns)

    df = backend.restricted_episode_frame(columns=cols)

    if podcast_ids is not None:
        wanted = set(backend.resolve_podcast_ids(podcast_ids))
        df = df[df["podcast_id"].isin(wanted)]

    if parse_dates:
        df = parse_episode_dates(df, copy=False)

    if metrics:
        m = catalog_frame(backend, "episode_metrics")
        m = m.drop(columns=["podcast_id"], errors="ignore")
        df = df.merge(m, on="episode_id", how="left", suffixes=("", "_metrics"))

    return df.reset_index(drop=True)


def podcasts_frame(backend: "ParquetBackend",
                   columns: Optional[List[str]] = None, *,
                   podcast_ids: Optional[Iterable[str]] = None
                   ) -> "pd.DataFrame":
    """
    The podcast catalog for the current view, as a DataFrame.

    Note:
        Honours ``subset=`` pinning. ``pod_description`` is the wide column
        here; leave it out unless you need it.
    """
    _require_pandas()
    cols = schema.validate_columns("podcasts", columns)

    backend._ensure_podcast_df()
    # Copy, always. This frame is the backend's own cached catalog; handing back
    # a view would let `df["x"] = ...` in a notebook cell change what
    # search_episodes() sees afterwards.
    df = backend._podcast_df
    wanted = set(backend.resolve_podcast_ids(podcast_ids))
    if len(wanted) != len(df):
        df = df[df["podcast_id"].isin(wanted)]
    df = df[cols].copy() if cols else df.copy()
    return df.reset_index(drop=True)


def episode_metrics_frame(backend: "ParquetBackend",
                          columns: Optional[List[str]] = None, *,
                          podcast_ids: Optional[Iterable[str]] = None
                          ) -> "pd.DataFrame":
    """
    Per-episode conversation metrics for the current view, as a DataFrame.

    One row per episode: word and turn counts, speaking rates, host/guest
    balance, gaps and overlap. Note that the host and guest figures count only
    turns whose role actually resolved, which is a minority -- so
    ``host_word_count + guest_word_count`` is well under ``total_word_count``.
    """
    _require_pandas()
    cols = schema.validate_columns("episode_metrics", columns)
    df = catalog_frame(backend, "episode_metrics", columns=cols)

    ids = backend.resolve_podcast_ids(podcast_ids)
    if "podcast_id" in df.columns and not backend.covers_whole_catalog(ids):
        df = df[df["podcast_id"].isin(set(ids))]
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Windowing
# ---------------------------------------------------------------------------

def _window_bounds(n, size: int, overlap: int, partial: bool):
    """
    Where every window starts and ends, for episodes of lengths *n*.

    The arithmetic that ``Episode.sliding_window`` does one episode at a time,
    done for all of them at once. Returns arrays over the flattened set of
    windows: which episode each belongs to, its index within that episode, and
    its start and end offsets into that episode's turns.

    Window count matches ``sliding_window`` exactly (episode.py:609-612): an
    episode with at most *size* turns yields one short window, and a longer one
    drops the trailing remainder rather than emitting a short window at the end.
    That asymmetry is odd, but reproducing it is what makes the two routes
    comparable; ``partial=True`` keeps the tail instead.
    """
    import numpy as np

    step = size - overlap
    if partial:
        # Every window that starts inside the episode, so the last one runs
        # short rather than being dropped.
        nwin = np.maximum(1, -(-np.maximum(n - size, 0) // step) + 1)
        nwin = np.where(n <= size, 1, nwin)
    else:
        nwin = np.where(n <= size, 1, (n - size) // step + 1)
    nwin = nwin.astype(np.int64)

    episode_of = np.repeat(np.arange(len(n)), nwin)
    # Index of each window within its own episode: a "group arange", i.e.
    # 0,1,2,0,1,0,1,2,3 for groups of 3,2,4.
    win_index = np.arange(nwin.sum()) - np.repeat(np.cumsum(nwin) - nwin, nwin)

    start = win_index * step
    end = np.minimum(start + size, n[episode_of])
    return episode_of, win_index, start, end


def window_frame_from_turns(turns: "pd.DataFrame", size: int = 12,
                            overlap: int = 0, *, group: str = "episode_id",
                            text: bool = True, separator: str = " ",
                            partial: bool = False,
                            columns: Optional[List[str]] = None,
                            sort: bool = True) -> "pd.DataFrame":
    """
    Build conversation windows from a turns frame.

    Split out from :func:`window_frame` so it works on any turns-shaped frame,
    including one you built yourself with ``pd.read_parquet``.

    With ``overlap=0`` this is what ``groupby(...).cumcount() // size`` does.
    With overlap it is not: a turn then belongs to up to ``ceil(size/step)``
    windows, so the operation is an expansion rather than a grouping, and the
    cumcount idiom silently produces non-overlapping windows instead. That is
    the case worth having in a library.

    Args:
        turns: One row per turn, with at least *group* and ``turn_count``.
        size: Turns per window.
        overlap: Turns shared with the previous window; must be < size.
        group: Column defining a conversation, normally ``episode_id``.
        text: Join ``turn_text`` per window into a ``text`` column.
        separator: What to join the text with.
        partial: Keep the trailing short window that ``sliding_window`` drops.
        columns: Extra numeric turn columns to average per window.
        sort: Sort the input first. Leave on unless it is already ordered by
            ``(episode_id, start_time, turn_count)``.

    Returns:
        One row per window: the group key, ``win``, ``n_turns``,
        ``start_index``, ``end_index``, timings, speaker counts, and the text.
    """
    pd = _require_pandas()
    import numpy as np

    if size <= 0:
        raise ValueError(f"size must be positive, got {size}")
    if overlap < 0:
        raise ValueError(f"overlap must not be negative, got {overlap}")
    if overlap >= size:
        raise ValueError(
            f"overlap ({overlap}) must be less than size ({size}); they would "
            "otherwise produce windows that never advance.")
    if group not in turns.columns:
        raise ValueError(f"turns frame has no {group!r} column")

    if turns.empty:
        return pd.DataFrame(columns=[group, "win", "n_turns", "start_index",
                                     "end_index", "start_time", "end_time",
                                     "duration"] + (["text"] if text else []))

    if sort:
        # Group first, then time within it. _sort_turns' keys are the common
        # case; a caller windowing by something other than episode_id (by
        # speaker, say) needs its own column leading, or the contiguity the
        # offset arithmetic depends on does not hold.
        keys = [group] + [k for k in ("start_time", "turn_count")
                          if k in turns.columns]
        df = turns.sort_values(keys, kind="stable").reset_index(drop=True)
    else:
        df = turns.reset_index(drop=True)

    # factorize keeps first-appearance order, and the frame is grouped by
    # episode after the sort, so each episode's rows are contiguous -- which the
    # offset arithmetic below depends on.
    codes, keys = pd.factorize(df[group], sort=False)
    n = np.bincount(codes, minlength=len(keys)).astype(np.int64)
    if not (np.diff(codes) >= 0).all():
        raise ValueError(
            f"rows for the same {group} are not contiguous; pass sort=True")
    episode_start = np.concatenate([[0], np.cumsum(n)[:-1]])

    episode_of, win_index, w_start, w_end = _window_bounds(
        n, size, overlap, partial)
    w_len = (w_end - w_start).astype(np.int64)

    # Expand to one row per (window, turn) pair, then aggregate. The positional
    # index into df is the episode's offset plus the window's offset plus the
    # turn's offset within the window.
    win_of_pair = np.repeat(np.arange(len(w_len)), w_len)
    within = np.arange(w_len.sum()) - np.repeat(np.cumsum(w_len) - w_len, w_len)
    positions = (episode_start[episode_of][win_of_pair]
                 + w_start[win_of_pair] + within)

    pairs = df.iloc[positions]
    grouped = pairs.groupby(win_of_pair, sort=True)

    out = pd.DataFrame({
        group: keys[episode_of],
        "win": win_index,
        "n_turns": w_len,
        "start_index": w_start,
        "end_index": w_end,
    })

    if "podcast_id" in df.columns:
        out.insert(1, "podcast_id", grouped["podcast_id"].first().to_numpy())
    if "start_time" in df.columns:
        out["start_time"] = grouped["start_time"].min().to_numpy()
    if "end_time" in df.columns:
        out["end_time"] = grouped["end_time"].max().to_numpy()
    if {"start_time", "end_time"} <= set(df.columns):
        # Wall-clock span of the window, matching TurnWindow.duration: the last
        # turn's end minus the first turn's start, so it includes the gaps
        # rather than summing turn durations.
        out["duration"] = out["end_time"] - out["start_time"]

    if "speaker" in df.columns:
        out["n_unique_speakers"] = grouped["speaker"].apply(
            lambda s: len({lbl for cell in s if cell is not None
                           for lbl in cell})).to_numpy()
    if "inferred_speaker_role" in df.columns:
        role = pairs["inferred_speaker_role"]
        out["n_host_turns"] = role.eq("host").groupby(win_of_pair).sum().to_numpy()
        out["n_guest_turns"] = role.eq("guest").groupby(win_of_pair).sum().to_numpy()

    for col in (columns or []):
        if col in df.columns and col not in out.columns:
            out[col] = grouped[col].mean().to_numpy()

    if text and "turn_text" in df.columns:
        # Empty turn texts are joined in rather than skipped, so the result is
        # the plain concatenation of the window's rows. 0.16% of turns have
        # empty text; dropping them here would make the text disagree with
        # n_turns and with what `" ".join` over the same rows produces.
        out["text"] = grouped["turn_text"].agg(
            lambda s: separator.join(s.fillna(""))).to_numpy()

    return out


def window_frame(backend: "ParquetBackend", size: int = 12, overlap: int = 0,
                 *, columns: Optional[List[str]] = None,
                 podcast_ids: Optional[Iterable[str]] = None,
                 episode_ids: Optional[Iterable[str]] = None,
                 text: bool = True, separator: str = " ",
                 partial: bool = False,
                 allow_large: bool = False) -> "pd.DataFrame":
    """
    Conversation windows over the current view, as a DataFrame.

    The columnar equivalent of :meth:`~sporc.episode.Episode.sliding_window`,
    producing the same windows -- so the two routes are comparable -- but built
    from one columnar read rather than a partition read per podcast.

    See :func:`window_frame_from_turns` for the arguments and for why overlap is
    the case worth having in the library.
    """
    _require_pandas()

    # Validate before estimating: the size estimate divides by the step, so bad
    # arguments would surface as a ZeroDivisionError rather than as the
    # explanation window_frame_from_turns already writes.
    if size <= 0:
        raise ValueError(f"size must be positive, got {size}")
    if overlap < 0:
        raise ValueError(f"overlap must not be negative, got {overlap}")
    if overlap >= size:
        raise ValueError(
            f"overlap ({overlap}) must be less than size ({size}); they would "
            "otherwise produce windows that never advance.")

    need = {"episode_id", "turn_count", "start_time", "end_time", "podcast_id"}
    if text:
        need.add("turn_text")
    need.update(["speaker", "inferred_speaker_role"])
    if columns:
        need.update(columns)
    read = [c for c in schema.table_columns("turns") if c in need]

    # The expansion multiplies rows by roughly size/step, so the guard has to
    # run on the windows rather than on the turns.
    step = size - overlap
    rows = backend.tree_row_count("turns_text", podcast_ids)
    _check_size("turns", read, int(rows * (size / step)),
                what=f"window_frame(size={size}, overlap={overlap})",
                allow_large=allow_large)

    turns = turns_frame(backend, read, podcast_ids=podcast_ids,
                        episode_ids=episode_ids, sort=True, allow_large=True)
    return window_frame_from_turns(
        turns, size=size, overlap=overlap, text=text, separator=separator,
        partial=partial, columns=columns, sort=False)
