"""
What is in each table, and what each column means.

This module is static data. It imports nothing from the rest of the package, it
does not touch the filesystem, and it does not need pandas -- so
``sporc.describe_columns("acoustics")`` answers offline, with no dataset
constructed and no Hub credentials. Looking up what
``f0_semitone_from_27_5hz_sma3nz_mean`` is should not cost a download.

It serves three jobs beyond documentation:

- ``columns=`` is validated against it before any I/O, so a mistyped column
  fails immediately with a suggestion rather than after a part file has been
  fetched. The failure it guards against is the one that motivated this whole
  API: a plausible-looking wrong string that quietly matches nothing.
- The byte-per-row figures let the frame API estimate a request's memory cost
  from the columns asked for. They are measured, not guessed -- pandas
  ``memory_usage(deep=True)`` over the tutorial subset -- and approximate.
- It is the single home for the twelve eGeMAPSv2 acoustic names, which were
  previously spelled out in three places with nothing keeping them in step.

``tests/test_schema.py`` checks the registry against the real Arrow schemas of a
layout on disk, so drift fails CI rather than misleading a reader.
"""

import difflib
from collections import OrderedDict
from typing import Dict, FrozenSet, Iterable, List, NamedTuple, Optional


class ColumnSpec(NamedTuple):
    """One column: its Arrow type, its cost in memory, and what it means."""

    dtype: str
    #: Approximate bytes per row once in a pandas DataFrame, measured with
    #: ``memory_usage(deep=True)``. Object columns dominate: a turn's text is
    #: 265 bytes a row where its start time is 8.
    nbytes: int
    description: str


class TableSpec(NamedTuple):
    """Where a table lives and how its rows are identified."""

    #: ``"tree"`` for the packed per-podcast part files, ``"catalog"`` for a
    #: single file under ``metadata/``.
    kind: str
    #: Tree name (see :data:`sporc.shard_map.TREE_DIRS`) or catalog file stem.
    source: str
    #: Columns that uniquely identify a row.
    keys: tuple
    #: How the table should be ordered for analysis, where that is meaningful.
    sort_keys: tuple = ()


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------

TABLES: Dict[str, TableSpec] = {
    # Trees: packed part files, one row group per podcast.
    "turns": TableSpec("tree", "turns_text", ("episode_id", "turn_count"),
                       ("episode_id", "start_time", "turn_count")),
    "turns_metrics": TableSpec("tree", "turns_metrics",
                               ("episode_id", "turn_count")),
    "acoustics": TableSpec("tree", "acoustics", ("episode_id", "turn_count")),
    "episodes_full": TableSpec("tree", "episodes", ("episode_id",)),

    # Catalogs: one file each under metadata/.
    "episodes": TableSpec("catalog", "episode_catalog", ("episode_id",)),
    "podcasts": TableSpec("catalog", "podcast_catalog", ("podcast_id",)),
    "episode_metrics": TableSpec("catalog", "episode_metrics", ("episode_id",)),
    "guest_index": TableSpec("catalog", "guest_index",
                             ("name_normalized", "podcast_id")),
    "guest_episode_index": TableSpec("catalog", "guest_episode_index",
                                     ("name_normalized", "episode_id")),
    "host_index": TableSpec("catalog", "host_index",
                            ("name_normalized", "podcast_id")),
    "host_episode_index": TableSpec("catalog", "host_episode_index",
                                    ("name_normalized", "episode_id")),
    "speaker_name_index": TableSpec("catalog", "speaker_name_index",
                                    ("name_normalized", "episode_id", "role")),
    "category_index": TableSpec("catalog", "category_index",
                                ("category", "podcast_id")),
    "hostname_index": TableSpec("catalog", "hostname_index",
                                ("hostname", "podcast_id")),
    "shard_map": TableSpec("catalog", "shard_map", ("podcast_id", "tree")),
}

#: Other names people reasonably reach for, mapped to the canonical one. The
#: tree names and the catalog file stems both appear in the layout on disk, so
#: both are accepted.
ALIASES: Dict[str, str] = {
    "turns_text": "turns",
    "text": "turns",
    "turn": "turns",
    "metrics": "turns_metrics",
    "turn_metrics": "turns_metrics",
    "episode_catalog": "episodes",
    "podcast_catalog": "podcasts",
    "episode": "episodes",
    "podcast": "podcasts",
    "acoustic": "acoustics",
    "audio": "acoustics",
}

#: Trees that carry a ``podcast_id`` column. ``turns_metrics`` does not, which
#: is why a podcast filter cannot be pushed into a scan of it -- the join to the
#: text turns is what narrows it.
TREE_HAS_PODCAST_ID: FrozenSet[str] = frozenset({
    "episodes", "turns_text", "acoustics"})

#: Catalogs that ship with the dataset metadata, and what to say when one is
#: missing. Two different situations: most simply arrive with any Hub-backed
#: load, but the speaker name index is built by a script.
CATALOG_HINTS: Dict[str, str] = {
    "speaker_name_index": (
        "Build it with: python scripts/build_indexes.py "
        "--data-dir {data_dir} --phase 1"),
    "episode_metrics": (
        "Build it with: python scripts/build_indexes.py "
        "--data-dir {data_dir} --phase 2"),
    "_default": (
        "It ships with the dataset metadata; update to a dataset build that "
        "includes it."),
}


# ---------------------------------------------------------------------------
# Columns
# ---------------------------------------------------------------------------

def _cols(*rows) -> "OrderedDict[str, ColumnSpec]":
    return OrderedDict((name, ColumnSpec(dtype, nbytes, desc))
                       for name, dtype, nbytes, desc in rows)


_TURNS = _cols(
    ("episode_id", "string", 73,
     "Episode this turn belongs to. md5 of the episode's mp3 URL, first 16 hex "
     "characters."),
    ("podcast_id", "string", 69,
     "Podcast this turn belongs to. md5 of the podcast's RSS URL, first 12 hex "
     "characters."),
    ("speaker", "list<string>", 120,
     "**A list**, not a string: every diarization label active during the turn. "
     "About 45% of turns have more than one, so testing `len == 1` discards "
     "nearly half the corpus. Labels are per-episode -- SPEAKER_00 in one "
     "episode is a different person from SPEAKER_00 in the next -- and the list "
     "can be empty where diarization produced no segments."),
    ("turn_text", "string", 265,
     "What was said. By far the widest column: budget 265 bytes a row for it, "
     "against 8 for a timestamp."),
    ("start_time", "double", 8, "Seconds from the start of the episode."),
    ("end_time", "double", 8, "Seconds from the start of the episode."),
    ("duration", "double", 8, "end_time minus start_time, in seconds."),
    ("turn_count", "int32", 4,
     "Position of the turn in the episode's diarization sequence. Unique within "
     "an episode, and the join key against turns_metrics and acoustics -- but "
     "**not** the temporal order: start_time is non-monotone in it for about "
     "3.7% of episodes. Sort on start_time when you mean time."),
    ("token_count", "int32", 8,
     "Timestamped tokens the transcript aligned to this turn. Counts "
     "punctuation separately, so it runs ~21% above a word count. Null for the "
     "9.9% of turns carried over from dataset 1.0."),
    ("inferred_speaker_name", "string", 75,
     "Who was speaking, or the sentinel `NO_INFERRED_SPEAKER` for the 81-90% of "
     "turns never resolved to a person. **Never null**, so dropna() removes "
     "nothing and grouping by it pools hundreds of people into one speaker. "
     "Filter with sporc.PLACEHOLDER_SPEAKERS."),
    ("inferred_speaker_role", "string", 71,
     "'host', 'guest', or the sentinel `NO_INFERRED_ROLE` for most turns. "
     "**Never null**. Because the sentinel dominates, `role != 'host'` is not "
     "'guest'."),
    ("speakers_recomputed", "bool", 1,
     "False for turns carried over unchanged from dataset 1.0, whose "
     "speaker-to-word matching could not be redone. These are exactly the turns "
     "with a null token_count."),
)

_TURNS_METRICS = _cols(
    ("episode_id", "string", 73, "Episode this turn belongs to."),
    ("turn_count", "int32", 4,
     "Position in the episode's turn sequence. With episode_id, the join key "
     "back to the turns table. This tree carries no podcast_id."),
    ("word_count", "int32", 4,
     "Whitespace-separated words in the turn. Agrees with Turn.word_count and "
     "with episode_metrics.total_word_count; differs from token_count."),
    ("words_per_second", "float", 4, "word_count divided by turn duration."),
    ("gap_from_prev", "float", 4,
     "Silence in seconds between the previous turn's end and this one's start. "
     "Negative where the turns overlap; null on an episode's first turn."),
    ("overlap_with_prev", "float", 4,
     "Seconds this turn overlaps the previous one. Null on the first turn."),
    ("discourse_marker_count", "int16", 2,
     "Discourse markers in the turn ('you know', 'I mean', 'like', ...)."),
    ("char_count", "int32", 4, "Characters in the turn text."),
)

_ACOUSTICS = _cols(
    ("episode_id", "string", 73, "Episode this turn belongs to."),
    ("podcast_id", "string", 69, "Podcast this turn belongs to."),
    ("turn_count", "int32", 4,
     "Position in the episode's turn sequence. With episode_id, the join key "
     "back to the turns table. Join on the pair: turn_count alone is unique "
     "only within an episode, so joining on it across a frame collapses "
     "almost everything."),
    # The twelve eGeMAPSv2 features. SPoRC publishes per-turn summary
    # statistics, not frame-level contours: each value is one number averaged
    # over a turn that may span many words. Names follow openSMILE's eGeMAPSv2
    # set, lowercased. `sma3` = smoothed by a 3-frame moving average; `nz` =
    # computed over non-zero (voiced) frames only.
    ("mfcc1_sma3_mean", "double", 8,
     "Mean of Mel-frequency cepstral coefficient 1 over the turn. MFCC1 tracks "
     "overall spectral energy, so it moves with loudness and with recording "
     "level. Not comparable across recordings with different mastering."),
    ("mfcc1_sma3_stdev", "double", 8,
     "Standard deviation of MFCC1 across the turn's frames. New in dataset 1.1."),
    ("mfcc2_sma3_mean", "double", 8,
     "Mean of MFCC2, a broad measure of spectral tilt (timbre)."),
    ("mfcc2_sma3_stdev", "double", 8, "Standard deviation of MFCC2."),
    ("mfcc3_sma3_mean", "double", 8, "Mean of MFCC3, further spectral shape."),
    ("mfcc3_sma3_stdev", "double", 8, "Standard deviation of MFCC3."),
    ("mfcc4_sma3_mean", "double", 8, "Mean of MFCC4, further spectral shape."),
    ("mfcc4_sma3_stdev", "double", 8, "Standard deviation of MFCC4."),
    ("f0_semitone_from_27_5hz_sma3nz_mean", "double", 8,
     "Mean fundamental frequency (pitch) over the turn, in semitones above "
     "27.5 Hz (A0) rather than Hz -- a log scale, so differences are "
     "comparable across voices of different pitch. Averaged over voiced frames "
     "only, so unvoiced frames are excluded rather than counted as zero."),
    ("f0_semitone_from_27_5hz_sma3nz_stdev", "double", 8,
     "Standard deviation of F0 across voiced frames: pitch variability, which "
     "is roughly what 'expressiveness' means acoustically."),
    ("f1_frequency_sma3nz_mean", "double", 8,
     "Mean centre frequency of the first formant (F1) in Hz over voiced "
     "frames. F1 tracks vowel height. There is no F2 or F3 in SPoRC, and one "
     "turn-level mean spans every vowel in the turn -- see sporc.phonetics to "
     "re-derive per-word formants from the source audio."),
    ("f1_frequency_sma3nz_stdev", "double", 8,
     "Standard deviation of F1 across voiced frames."),
)

_EPISODES = _cols(
    ("episode_id", "string", 73,
     "md5 of the episode's mp3 URL, first 16 hex characters. Use this rather "
     "than the title: titles are not unique within a podcast."),
    ("podcast_id", "string", 69,
     "md5 of the podcast's RSS URL, first 12 hex characters."),
    ("ep_title", "string", 95, "Episode title from the RSS feed."),
    ("mp3_url", "string", 110, "Where the audio was fetched from."),
    ("duration_seconds", "double", 8, "Episode length in seconds."),
    ("category1", "string", 60,
     "Primary Apple Podcasts category. category1 through category10 hold the "
     "feed's categories in order; most episodes fill only the first few."),
    ("host_predicted_names", "list<string>", 95,
     "**A list.** Host names predicted from the episode's text. Predicted for "
     "every episode, which is a different population from the diarized hosts "
     "in host_index."),
    ("guest_predicted_names", "list<string>", 95,
     "**A list.** Guest names predicted from the episode's text."),
    ("num_main_speakers", "int64", 8, "Distinct speakers diarization found."),
    ("language", "string", 55, "ISO language code from the feed."),
    ("explicit", "int64", 8, "1 if the feed marks the episode explicit."),
    ("episode_date", "string", 78,
     "Publication time as a **string holding a millisecond Unix epoch**, in "
     "UTC. Cast before use: pd.to_datetime(s.astype('Int64'), unit='ms', "
     "utc=True), or let episodes_frame(parse_dates=True) do it. Passing the "
     "raw string to pd.to_datetime yields dates in 1970 without raising. This "
     "is the same value as episodes_full.episode_date_localized, despite the "
     "names."),
    ("overlap_prop_duration", "double", 8,
     "Proportion of the episode's duration with more than one speaker active."),
    ("avg_turn_duration", "double", 8, "Mean turn length in seconds."),
    ("total_sp_labels", "int64", 8, "Total speaker labels assigned."),
)
for _i in range(2, 11):
    _EPISODES[f"category{_i}"] = ColumnSpec(
        "string", 60, f"Apple Podcasts category {_i}, or null. See category1.")

_PODCASTS = _cols(
    ("podcast_id", "string", 69,
     "md5 of the RSS URL, first 12 hex characters."),
    ("rss_url", "string", 110, "Feed the podcast was collected from."),
    ("pod_title", "string", 85, "Podcast title."),
    ("pod_description", "string", 400,
     "Feed description. Wide -- exclude it unless you need it."),
    ("language", "string", 55, "ISO language code from the feed."),
    ("explicit", "int64", 8, "1 if the feed marks the podcast explicit."),
    ("image_url", "string", 110, "Cover art."),
    ("itunes_author", "string", 85, "Author field from the feed."),
    ("episode_count", "int64", 8, "Episodes collected for this podcast."),
    ("total_duration_seconds", "double", 8, "Summed episode duration."),
    ("primary_category", "string", 60,
     "The podcast's first category. Part files are ordered by this, so "
     "podcasts in the same category tend to share a part file."),
    ("all_categories", "list<string>", 90, "**A list** of every category."),
    ("host_names", "list<string>", 90,
     "**A list** of host names from the feed. A different source from the "
     "episode-level host_predicted_names, and the two can disagree."),
    ("earliest_date", "string", 78, "Oldest episode date, as a string."),
    ("latest_date", "string", 78, "Newest episode date, as a string."),
)

_EPISODE_METRICS = _cols(
    ("episode_id", "string", 73, "Episode these metrics describe."),
    ("podcast_id", "string", 69, "Podcast the episode belongs to."),
    ("total_word_count", "int32", 4, "Words across every turn."),
    ("total_turn_count", "int32", 4, "Turns in the episode."),
    ("unique_speaker_count", "int32", 4, "Distinct diarization labels."),
    ("avg_turn_duration", "double", 8, "Mean turn length, seconds."),
    ("median_turn_duration", "double", 8, "Median turn length, seconds."),
    ("avg_words_per_second", "double", 8, "Mean speaking rate."),
    ("host_word_count", "int32", 4,
     "Words in turns whose role resolved to host. Excludes the sentinel-role "
     "turns, which are most of them, so this is not total minus guest."),
    ("guest_word_count", "int32", 4, "Words in turns whose role resolved to guest."),
    ("host_turn_proportion", "double", 8, "Share of turns attributed to a host."),
    ("host_word_proportion", "double", 8, "Share of words attributed to a host."),
    ("avg_gap_duration", "double", 8, "Mean silence between turns, seconds."),
    ("total_overlap_duration", "double", 8, "Summed overlapping speech, seconds."),
    ("discourse_marker_count", "int32", 4, "Discourse markers in the episode."),
    ("discourse_marker_rate", "double", 8, "Markers per 1,000 words."),
    ("speaking_rate_host", "double", 8, "Host words per second."),
    ("speaking_rate_guest", "double", 8, "Guest words per second."),
)

_EPISODES_FULL = _cols(
    ("episode_id", "string", 73, "md5 of the mp3 URL, first 16 hex characters."),
    ("podcast_id", "string", 69, "md5 of the RSS URL, first 12 hex characters."),
    ("ep_title", "string", 95, "Episode title."),
    ("ep_description", "string", 500, "Episode description. Wide."),
    ("mp3_url", "string", 110, "Where the audio was fetched from."),
    ("duration_seconds", "double", 8, "Episode length in seconds."),
    ("transcript", "string", 25000,
     "The full episode transcript, undiarized. This one column is why the "
     "episodes tree is 16 GB: budget ~25 kB per row for it."),
    ("rss_url", "string", 110, "Podcast feed URL."),
    ("pod_title", "string", 85, "Podcast title, repeated on every episode."),
    ("pod_description", "string", 400, "Podcast description, repeated."),
    ("host_predicted_names", "list<string>", 95, "**A list** of predicted hosts."),
    ("guest_predicted_names", "list<string>", 95, "**A list** of predicted guests."),
    ("neither_predicted_names", "list<string>", 90,
     "**A list** of named people who are neither host nor guest."),
    ("main_ep_speakers", "list<string>", 90,
     "**A list** of the diarization labels carrying most of the speech."),
    ("host_speaker_labels", "string", 90,
     "JSON object mapping host name to diarization label. '{}' where no host "
     "was resolved -- which is what makes an episode 'diarized but not "
     "attributed'."),
    ("guest_speaker_labels", "string", 90,
     "JSON object mapping guest name to diarization label. '{}' for most."),
    ("num_main_speakers", "int64", 8, "Distinct speakers diarization found."),
    ("overlap_prop_duration", "double", 8,
     "Proportion of episode duration with overlapping speech."),
    ("overlap_prop_turn_count", "double", 8,
     "Proportion of turns with more than one speaker."),
    ("avg_turn_duration", "double", 8, "Mean turn length in seconds."),
    ("total_sp_labels", "int64", 8, "Total speaker labels assigned."),
    ("language", "string", 55, "ISO language code."),
    ("explicit", "int64", 8, "1 if marked explicit."),
    ("image_url", "string", 110, "Cover art."),
    ("episode_date_localized", "string", 78,
     "Publication time as a **string holding a millisecond Unix epoch**. "
     "Despite the name this is the same value as episode_catalog.episode_date, "
     "byte for byte; neither is more localized than the other. Episode."
     "episode_date parses it with datetime.fromtimestamp, which is local-naive "
     "-- prefer Episode.episode_datetime, which is UTC."),
    ("oldest_episode_date", "string", 78, "Oldest date in the feed, as a string."),
    ("last_update", "string", 78, "Feed's last-update stamp, as a string."),
    ("created_on", "string", 78, "Feed's creation stamp, as a string."),
    ("itunes_author", "string", 85, "Author field from the feed."),
    ("itunes_owner_name", "string", 85, "Owner field from the feed."),
    ("host", "string", 60, "Hostname serving the feed."),
)
for _i in range(1, 11):
    _EPISODES_FULL[f"category{_i}"] = ColumnSpec(
        "string", 60, f"Apple Podcasts category {_i}, or null.")

_NAME_INDEX = _cols(
    ("name_normalized", "string", 75,
     "Lowercased, stripped name. What lookups match against."),
    ("name_original", "string", 75, "The name as it appeared."),
    ("podcast_id", "string", 69, "Podcast the name appears in."),
)
_EPISODE_NAME_INDEX = _cols(
    ("name_normalized", "string", 75, "Lowercased, stripped name."),
    ("name_original", "string", 75, "The name as it appeared."),
    ("podcast_id", "string", 69, "Podcast the episode belongs to."),
    ("episode_id", "string", 73, "Episode the name appears in."),
)

COLUMNS: Dict[str, "OrderedDict[str, ColumnSpec]"] = {
    "turns": _TURNS,
    "turns_metrics": _TURNS_METRICS,
    "acoustics": _ACOUSTICS,
    "episodes": _EPISODES,
    "episodes_full": _EPISODES_FULL,
    "podcasts": _PODCASTS,
    "episode_metrics": _EPISODE_METRICS,
    "guest_index": _NAME_INDEX,
    "host_index": _NAME_INDEX,
    "guest_episode_index": _EPISODE_NAME_INDEX,
    "host_episode_index": _EPISODE_NAME_INDEX,
    "speaker_name_index": _cols(
        ("name_normalized", "string", 75, "Lowercased, stripped name."),
        ("name_original", "string", 75, "The name as it appeared."),
        ("role", "string", 55, "'host' or 'guest' as attributed in that episode."),
        ("episode_id", "string", 73, "Episode the name appears in."),
        ("podcast_id", "string", 69, "Podcast the episode belongs to."),
    ),
    "category_index": _cols(
        ("category", "string", 60, "Apple Podcasts category."),
        ("podcast_id", "string", 69, "Podcast with episodes in it."),
    ),
    "hostname_index": _cols(
        ("hostname", "string", 70, "Host part of the podcast's RSS URL."),
        ("podcast_id", "string", 69, "Podcast served from it."),
    ),
    "shard_map": _cols(
        ("podcast_id", "string", 69, "Podcast located by this row."),
        ("tree", "string", 60, "Which tree: episodes, turns_text, "
                               "turns_metrics or acoustics."),
        ("part", "string", 70, "Part file holding the podcast's rows."),
        ("row_group", "int64", 8,
         "Row group within that part. One podcast is exactly one row group, "
         "which is what makes a per-podcast read cheap."),
        ("num_rows", "int64", 8,
         "Rows the podcast has in that tree. Summing this is how the frame API "
         "knows a request's size before reading anything."),
    ),
}

#: Fallback width for a column the registry does not know, so a dataset built
#: with columns newer than this package still gets an estimate rather than a
#: crash. Deliberately pessimistic: guessing low is what lets a request through
#: that should have been refused.
DEFAULT_COLUMN_BYTES = 100


# ---------------------------------------------------------------------------
# Lookups
# ---------------------------------------------------------------------------

def list_tables() -> List[str]:
    """Every table name the registry knows, in a sensible reading order."""
    return list(TABLES)


def resolve_table(name: str) -> str:
    """
    Canonical table name for *name*, accepting the documented aliases.

    Raises:
        ValueError: naming the closest matches and then the full list.
    """
    if name in TABLES:
        return name
    key = str(name).strip().lower()
    if key in TABLES:
        return key
    if key in ALIASES:
        return ALIASES[key]
    close = difflib.get_close_matches(key, list(TABLES) + list(ALIASES), n=3)
    hint = f" Did you mean {', '.join(close)}?" if close else ""
    raise ValueError(
        f"Unknown table {name!r}.{hint} "
        f"Known tables: {', '.join(list_tables())}.")


def table_columns(table: str) -> List[str]:
    """Every column in *table*, in file order."""
    return list(COLUMNS[resolve_table(table)])


def validate_columns(table: str, columns: Optional[Iterable[str]]
                     ) -> Optional[List[str]]:
    """
    Check *columns* against *table*, returning them as a list.

    ``None`` passes through, meaning every column.

    This runs before any I/O. A mistyped column name is the same class of
    mistake as filtering on a sentinel that does not exist -- it looks right and
    does nothing -- so it is worth refusing at the point of the call rather than
    after a part file has been downloaded.

    Raises:
        ValueError: naming the unknown columns and their closest matches.
    """
    if columns is None:
        return None
    canonical = resolve_table(table)
    known = COLUMNS[canonical]
    requested = list(columns)
    if not requested:
        raise ValueError(
            f"columns=[] would select nothing from {canonical!r}. "
            "Pass columns=None for every column.")
    unknown = [c for c in requested if c not in known]
    if unknown:
        parts = []
        for c in unknown:
            close = difflib.get_close_matches(c, list(known), n=2)
            parts.append(f"{c!r}" + (f" (did you mean {', '.join(close)}?)"
                                     if close else ""))
        raise ValueError(
            f"Unknown column(s) for table {canonical!r}: {'; '.join(parts)}. "
            f"Available: {', '.join(known)}.")
    return requested


def estimate_bytes(table: str, columns: Optional[Iterable[str]],
                   rows: int) -> int:
    """
    Approximate in-memory size of *rows* rows of *columns* from *table*.

    Rough on purpose. The figures come from ``memory_usage(deep=True)`` over a
    real slice, and text columns vary between podcasts, so treat this as the
    right order of magnitude rather than a number to budget against. Its job is
    to tell 2 GB apart from 130 GB, which it does comfortably.
    """
    canonical = resolve_table(table)
    known = COLUMNS[canonical]
    names = list(known) if columns is None else list(columns)
    per_row = sum(known[c].nbytes if c in known else DEFAULT_COLUMN_BYTES
                  for c in names)
    return per_row * max(0, int(rows))


def format_bytes(n: int) -> str:
    """Render a byte count the way an error message should read."""
    step = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if step < 1024 or unit == "TB":
            return f"{step:.0f} {unit}" if unit in ("B", "KB") \
                else f"{step:.1f} {unit}"
        step /= 1024
    return f"{step:.1f} TB"  # pragma: no cover


def catalog_hint(name: str, data_dir: str = "<data-dir>") -> str:
    """What to tell someone whose copy of the dataset lacks catalog *name*."""
    template = CATALOG_HINTS.get(name, CATALOG_HINTS["_default"])
    return template.format(data_dir=data_dir)


def catalog_label(name: str) -> str:
    """Human-readable name for a catalog, for use in error messages."""
    return name.replace("_", " ").capitalize()


def list_catalogs() -> List[str]:
    """Every catalog that can be read with ``catalog()`` / ``load_catalog()``."""
    return [n for n, spec in TABLES.items() if spec.kind == "catalog"]


def catalog_file(name: str) -> str:
    """Repo-relative path of a catalog, e.g. ``metadata/guest_index.parquet``."""
    canonical = resolve_table(name)
    spec = TABLES[canonical]
    if spec.kind != "catalog":
        raise ValueError(
            f"{canonical!r} is a data tree, not a catalog. Catalogs: "
            f"{', '.join(list_catalogs())}. Read a tree with turns_frame() or "
            "episodes_frame().")
    return f"metadata/{spec.source}.parquet"
