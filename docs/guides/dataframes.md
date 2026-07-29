# DataFrames

`sporc` has two ways to reach the data. The object model (`Podcast`, `Episode`,
`Turn`) is the right shape for questions about one episode. Above that level it
is the wrong shape, because walking it parses a Parquet footer per podcast:

| Building 12-turn windows over 7,625 episodes | Time |
|---|---:|
| `episode.sliding_window()` | ~1.4 min |
| `ds.window_frame(size=12)` | ~1 s |

This page covers the second route. Same files, read as columns.

```python
import sporc

ds = sporc.SPORCDataset(subset="news")

turns = ds.turns_frame(columns=["episode_id", "turn_count", "start_time",
                                "inferred_speaker_name"])
turns.groupby("episode_id").size().describe()
```

## Start with `columns=`

This is the argument that decides whether a request is comfortable or
impossible. Turn text is 265 bytes a row; a timestamp is 8.

| Request | Per row | 1.4M turns | Whole corpus |
|---|---:|---:|---:|
| Everything | 710 B | 1.0 GB | 131 GB |
| `["episode_id", "turn_count", "start_time"]` | 85 B | 119 MB | 15.7 GB |
| `["turn_count", "start_time"]` | 12 B | 17 MB | 2.2 GB |

A request estimated over 8 GB raises `FrameTooLargeError` rather than running.
The shard map records the row count, so the check is exact, costs nothing, and
happens immediately:

```
turns_frame() would materialize 185,190,000 rows x 12 columns ~= 131 GB in memory
(estimated; turn_text alone is 265 bytes/row). Bound it with one of:
  - podcast_ids=[...] or episode_ids=[...]   restrict to what you need
  - SPORCDataset(subset=[...])               pins every frame to that slice
  - columns=[...]                            most analyses need a handful
  - iter_turns_frames(...)                   one DataFrame per part file
  - allow_large=True                         if the machine can take it
```

Loading with `subset=` pins every frame to that slice, so the guard rarely comes
up in practice. It is there for the case where someone forgets.

Column names are checked against the schema before any file is opened, so a typo
fails at the call rather than after a download:

```python
ds.turns_frame(columns=["turn_txt"])
# ValueError: Unknown column(s) for table 'turns': 'turn_txt'
#   (did you mean turn_text?)
```

## What is in each table

```python
ds.columns("acoustics")          # name, dtype, bytes_per_row, description
sporc.describe_columns("turns")  # same, no dataset needed
```

Or from the shell, which needs neither data nor a token:

```console
$ sporc columns acoustics
$ sporc columns turns --grep sentinel
```

## The four accessors

```python
ds.turns_frame(columns=None, metrics=False, acoustics=False, speakers=False)
ds.episodes_frame(columns=None, parse_dates=True, metrics=False)
ds.podcasts_frame(columns=None)
ds.episode_metrics_frame(columns=None)
```

`episodes_frame` reads the episode catalog rather than the episodes tree. The
catalog carries everything except the transcripts, and the transcripts are why
that tree is 16 GB.

Nothing is cached. Every call re-reads, which costs about a second for a genre
slice, so it is safe to add columns to what you get back:

```python
turns = ds.turns_frame()
turns["long"] = turns.duration > 30      # cannot affect anything else
```

### Joining metrics and acoustics

```python
turns = ds.turns_frame(metrics=True, acoustics=True)
```

Both join on `(episode_id, turn_count)`, and both are **left** joins. Coverage of
the metrics and acoustics trees is partial, so an inner join would delete text
turns wherever a metric happens to be missing. A left join keeps the turn and
puts `NaN` in the metric, which you can see. The join logs at INFO when more than
1% of turns went unmatched.

Join on both keys if you write this yourself. `turn_count` is unique within an
episode but not across one, so joining a whole frame on it alone collapses
almost every row.

## Two columns that mislead

### Speakers are a list, and 45% of turns have more than one

`speaker` holds every diarization label active in the turn. The name does not
say so, and the common idiom for finding single-speaker turns quietly discards
nearly half the corpus:

```python
solo = turns[turns.speaker.str.len() == 1]   # drops 44.8% of turns
```

That may well be what you want; a within-speaker comparison needs it. But it
should be a decision. `speakers=True` adds the derived columns so the test is
explicit:

```python
turns = ds.turns_frame(speakers=True)
# n_speakers, is_overlapping, primary_speaker,
# is_host, is_guest, has_inferred_speaker, has_inferred_role
```

These match the `Turn` properties exactly, so the two routes can be mixed.
`sporc.add_speaker_columns(df)` does the same to a frame from anywhere.

Note also that diarization labels are per-episode. `SPEAKER_00` in one episode is
a different person from `SPEAKER_00` in the next, so counting episodes per label
overstates reach. `sporc.ANON_SPEAKER_RE` matches them.

### The inferred-speaker columns are never null

`inferred_speaker_name` and `inferred_speaker_role` hold sentinel strings, not
nulls, for turns that were diarized but never attributed to a person. In the news
slice that is **89.6%** of turns; in the tutorial subset, 81.2%.

```python
turns.inferred_speaker_name.isna().sum()   # 0, always
turns.dropna(subset=["inferred_speaker_name"])   # removes nothing
```

So the obvious null test keeps every placeholder row, and grouping by speaker
pools hundreds of different people into one. Worse, the sentinel's name is
guessable and the guess is wrong. A filter on `NO_INFERRED_NAME`, a value that
does not occur, never fires, and the output looks entirely reasonable. That
happened, and overstated a published figure sixfold.

```python
from sporc import PLACEHOLDER_SPEAKERS, is_placeholder_speaker

named = turns[~turns.inferred_speaker_name.isin(PLACEHOLDER_SPEAKERS)]
```

or `speakers=True` and filter on `has_inferred_speaker`. Since the role sentinel
dominates too, `role != "host"` does not mean "guest". Most turns are neither.

## Dates

`episode_date` is a **string holding a millisecond Unix epoch**. Casting it to an
integer and parsing, which is the natural thing to try, reads it as nanoseconds
and gives you 1970 without raising:

```python
pd.to_datetime(eps.episode_date.astype("Int64"))          # 1970-01-01
pd.to_datetime(eps.episode_date.astype("Int64"), unit="ms", utc=True)  # 2020
```

`episodes_frame(parse_dates=True)` is the default and does the second, adding a
UTC `day` column. `sporc.parse_episode_dates(df)` does it to a frame you built
yourself.

UTC matters here. Rendering in the machine's local timezone puts about 13.6% of
episodes on a different calendar day depending on where the code runs, which is
enough to move counts in a daily series. `Episode.episode_date` is local-naive
for backwards compatibility; `Episode.episode_datetime` is UTC-aware and is the
one to use.

Despite the names, `episode_catalog.episode_date` and the episodes tree's
`episode_date_localized` are the same value, byte for byte. Neither is more
localized than the other.

## Conversation windows

```python
w = ds.window_frame(size=12, overlap=6)
```

One row per window: `episode_id`, `podcast_id`, `win`, `n_turns`,
`start_index`, `end_index`, `start_time`, `end_time`, `duration`,
`n_unique_speakers`, `n_host_turns`, `n_guest_turns`, `text`.

With no overlap this is what the familiar idiom does:

```python
turns["win"] = turns.groupby("episode_id").cumcount() // 12
```

With overlap that idiom silently stops being right. A turn then belongs to
several windows, so the operation is an expansion rather than a grouping, and
`cumcount()` produces non-overlapping windows while looking correct. That case is
the reason this is in the library.

`window_frame` produces the same windows as `Episode.sliding_window`, including
its habit of dropping a trailing partial window (an episode of 30 turns at
`size=7` yields four windows covering 28). Pass `partial=True` to keep the
remainder. `sporc.window_frame_from_turns(df, ...)` runs on any turns-shaped
frame.

## Order: sort on time, not on `turn_count`

`turns_frame` sorts by `(episode_id, start_time, turn_count)`, which is what
`Episode.turns` does. Sorting by `turn_count` is tempting, since it is the join
key, but it gives a different answer on about 3.7% of episodes, because
`start_time` is not monotone in it. Windows built the two ways disagree on those
episodes, and nothing will tell you.

## Corpus-wide catalogs

The catalogs cover all 228,099 podcasts, and they are the cheapest thing in the
corpus. The guest index is about a megabyte, and they arrive with any Hub-backed
load.

```python
guests = ds.catalog("guest_index")
ds.catalogs()     # what names are accepted
```

`catalog()` **ignores `subset=` pinning**, deliberately: reaching past the loaded
slice is the reason it exists. A cross-genre guest network needs the whole index
while the dataset in hand is one genre. The `*_frame()` accessors do the
opposite and stay inside the slice.

Reading one does not require a dataset at all:

```python
from sporc import load_catalog

guests = load_catalog("guest_index")     # fetches one ~1 MB file
```

## When it does not fit

Two escape hatches, for work spanning more turns than fit in memory.

Chunks, one DataFrame per part file. Memory is bounded by part size however
large the corpus is, so no guard applies:

```python
from collections import Counter

totals = Counter()
for chunk in ds.iter_turns_frames(columns=["inferred_speaker_role"]):
    totals.update(chunk.inferred_speaker_role.value_counts().to_dict())
```

Or the files themselves, which feed `pyarrow`, DuckDB and `pd.read_parquet`
equally well:

```python
paths = ds.parquet_paths("turns_text")

import duckdb
duckdb.query(f"SELECT inferred_speaker_role, count(*) "
             f"FROM read_parquet({paths}) GROUP BY 1")
```

`ds.turns_dataset()` wraps the same files as a `pyarrow.dataset.Dataset` for
predicate-pushdown scans. Note it spans whole part files, so it sees every
podcast in them rather than only the ones requested.

## One difference from the object model

`turns_frame` gives one row per turn **in the file**. Building `Episode.turns`
skips two kinds of row: turns with empty text (0.16%) and turns where
`end_time <= start_time` (0.22%). Together that is a third of a percent of turns,
but they are spread across 23% of episodes, so turn counts from the two routes
differ more often than the rate suggests.

The frame keeps them, because they are in the data. To match the object model:

```python
turns = turns[(turns.turn_text.str.strip() != "")
              & (turns.end_time > turns.start_time)]
```

## Memory tips

`astype("category")` on `episode_id` and `podcast_id` pays for itself on any
frame above a few hundred thousand rows, since as strings they cost 73 and 69
bytes a row. Remember `groupby(..., observed=True)` afterwards, or pandas will
build a row for every unused category pair.
