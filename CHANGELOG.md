# Changelog

## 1.2.0

There is now a way to get a table out of the library. `turns_frame()`,
`episodes_frame()`, `podcasts_frame()` and `episode_metrics_frame()` return
pandas DataFrames; `window_frame()` returns conversation windows; `catalog()`
reads the corpus-wide metadata indexes. Before this, any question above the
level of one episode had to leave the library and read the Parquet directly,
because walking the object model parses a file footer per podcast -- building
12-turn windows across 7,625 episodes took about 1.4 minutes that way, against
roughly a second as a frame. Both produce the same windows.

`columns=` is the argument that matters and the documentation leads with it: the
full turns table is about 710 bytes a row, of which `turn_text` alone is 265,
while the three columns most analyses start from come to 85. Column names are
checked against the schema before any file is opened, so a typo fails at the
call rather than after a download. A request estimated over 8 GB raises
`FrameTooLargeError` instead of running -- the row count is exact and free
(the shard map records it), and the alternative is the operating system killing
a notebook kernel. `iter_turns_frames()` yields one frame per part file for work
that genuinely spans the corpus, and `parquet_paths()` hands over the file paths
for DuckDB or `pd.read_parquet`.

Frames are never cached. Notebooks add columns to what they get back, so
handing two cells the same object would make the second cell's result depend on
whether the first had run; the catalog accessors copy for the same reason.

**`PLACEHOLDER_SPEAKERS` is now exported from the package root.**
`inferred_speaker_name` and `inferred_speaker_role` contain sentinel strings
rather than nulls -- `NO_INFERRED_SPEAKER` and `NO_INFERRED_ROLE`, on 81-90% of
turns depending on the slice -- so `dropna()` removes nothing and `IS NOT NULL`
keeps every placeholder row. The constant existed but lived in `sporc.phonetics`,
which pulls in torch, so nobody could reach it; the value is also guessable and
the guess is wrong, and a filter on the plausible-looking `NO_INFERRED_NAME`
never fires. That happened in teaching material built on this package and
overstated a figure sixfold. The sentinels now live in `sporc.constants` with
`NO_INFERRED_SPEAKER`, `NO_INFERRED_ROLE`, `SPEAKER_UNKNOWN`, `ANON_SPEAKER_RE`
and `is_placeholder_speaker`; `sporc.phonetics.PLACEHOLDER_SPEAKERS` still
resolves to the same object. `Turn` gains `has_inferred_speaker` and
`has_inferred_role`, and `sporc.add_speaker_columns()` is the vectorised
equivalent.

`Episode.episode_datetime` is a new UTC-aware publication time.
`Episode.episode_date` is unchanged but was always local-naive: it renders the
same instant in whatever timezone the machine is set to, and about 13.6% of
episodes fall on a different calendar day between `Asia/Tokyo` and
`America/Detroit`. The underlying field is a millisecond epoch stored as a
string, and casting it to an integer and parsing -- the obvious move -- reads it
as nanoseconds and yields 1970 without raising, so `episodes_frame()` parses it
by default. Despite the names, `episode_catalog.episode_date` and the episodes
tree's `episode_date_localized` are the same value, byte for byte.

`Turn.primary_speaker` returns `None` for a turn with no speakers instead of
raising `IndexError`. An empty speaker list is a documented legal state -- where
diarization produced no segments, the transcript arrives as one unattributed
turn -- so raising on it was a bug, and `None` also matches what the vectorised
equivalent produces.

New `sporc/schema.py` describes every table and column: dtype, approximate bytes
per row, and a sentence on what it means, including the twelve eGeMAPSv2
acoustic names spelled out. It is static data with no I/O and no pandas, so
`sporc.describe_columns("acoustics")`, `ds.columns(...)` and the new
`sporc columns <table>` command all answer offline with no dataset and no token.
It is also the single home for those acoustic names, which were previously
written out in three places with nothing keeping them in step.

Catalog access no longer requires knowing the HuggingFace cache layout.
`sporc.load_catalog("guest_index")` fetches that one file (about a megabyte)
without constructing a dataset. Internally the ten catalog loaders that each did
their own path join and existence probe now share one helper routed through the
data source, which also fixes a latent bug: an optional catalog present in the
dataset but not downloaded at construction raised `IndexNotBuiltError` instead
of being fetched. Under `allow_downloads=False` a missing catalog now raises
`DataNotLocalError` rather than `IndexNotBuiltError`; both derive from
`SPORCError`.

`catalog()` deliberately ignores `subset=` pinning while the `*_frame()`
accessors honour it. Reaching past the loaded slice is the reason the catalogs
are worth having: a cross-genre guest network needs the whole index while the
dataset in hand is one genre.

**pandas is a core dependency again.** The 1.1.4 notes below say it moved to the
`phonetics` extra because only that module used it. That was wrong: building the
indexes calls `to_pandas()` unconditionally and the warm-cache path reads the
catalogs back through feather, so a `ParquetBackend` could never be constructed
without pandas. Listing it as optional only meant a bare install failed at first
use rather than at install time.

Two things worth recording about how the two routes differ. `turns_frame` sorts
by `(episode_id, start_time, turn_count)`, matching `Episode.turns`; sorting by
`turn_count` -- tempting, since it is the join key -- gives a different answer on
about 3.7% of episodes, because `start_time` is not monotone in it. And building
`Episode.turns` silently skips rows with empty text (0.16%) and rows with
`end_time <= start_time` (0.22%), which together touch 23% of episodes. The
frames keep those rows, because they are in the data; the DataFrames guide gives
the one-line filter that reproduces the object model's answer.

`Episode.sliding_window` is unchanged, including its habit of dropping a
trailing partial window. `window_frame` reproduces that by default so the two
agree, and takes `partial=True` to keep the remainder.

## 1.1.4

`SPORCDataset(subset=...)` now pins the dataset to the fetched slice. Before, it
prefetched the subset but left the backend reporting the whole catalog (228,099
podcasts), so `iterate_episodes()`, `get_all_podcasts()` and the counts walked
straight out of the slice on disk -- raising `DataNotLocalError` with downloads
off, or quietly pulling the rest of the corpus with them on. Iterating,
counting, searching and statistics now see only the subset; direct lookups by
id still reach the whole catalog. Bare `prefetch()` is unchanged (it only
downloads); the new backend method `restrict_to_podcasts()` does the pinning,
and `prefetch()` now also returns the resolved `podcast_ids`.

Bounded episode iteration no longer materializes the whole catalog.
`iterate_episodes(max_episodes=N)` and `search_episodes(..., max_episodes=N)`
asked the backend for every matching row -- up to the full ~1.1M-episode catalog
as Python dicts -- and only then sliced off `N`. The cap and sampling mode are
now pushed into the backend, which applies them to the DataFrame before the
conversion, so a bounded call pays for `N` rows rather than the whole catalog.
The unfiltered catalog is also converted once and cached (cleared when a
`subset` restriction changes), so repeated full passes reuse it. And
`search_episodes()` with no `max_episodes` now warns before building in bulk,
matching the guard `get_all_episodes()` and `get_all_podcasts()` already carry.

Reading a podcast's partitions is faster. Every read rebuilt a
`pq.ParquetFile`, which reparses the file footer -- about 12 ms against a packed
part file, versus 0.1 ms for the row-group read itself, so the footer parse
dominated. Open handles are now cached (one footer parse per part file), so the
podcasts packed into a shared part -- neighbours in the category ordering --
reuse a single parse.

The base install is leaner. `pip install sporc` now pulls only
`huggingface_hub` and `pyarrow` -- the two libraries the core package actually
imports. `pandas`, `numpy`, and `requests` were listed as core dependencies but
are used only inside `sporc/phonetics.py`, which imports them lazily and sits
behind the optional `phonetics` extra, so they have moved there (`pip install
sporc[phonetics]` is now self-sufficient). `tqdm` backed the build scripts in
`scripts/`, never the shipped package, so it moved to the `dev` extra alongside
`pandas`/`numpy`, which the test suite needs. If you relied on pandas or numpy
coming along with a bare `pip install sporc`, add them explicitly. Nothing in
the SPORC API changed.

The minimum Python is now 3.9 (was 3.8). The `huggingface_hub>=1.2.0` floor
already required 3.9, so `>=3.8` never actually resolved -- Python 3.8 is also
past end-of-life.

## 1.1.3

Diarized guests are a shipped index, so the tutorial subset build no longer
scans the corpus.

The dataset now ships `metadata/guest_index.parquet` (diarized guest ->
podcast, 25,165 rows) and `metadata/guest_episode_index.parquet` (-> episode,
25,728 rows), built from `guest_speaker_labels` -- the diarization labels, so a
name is someone who actually spoke, not merely someone `guest_predicted_names`
listed as mentioned. There are 24,026 distinct diarized guests, against 385,932
predicted guest rows in `speaker_name_index`; the gap is the mention artefact.

`scripts/build_tutorial_subset.py` read those diarization labels itself,
range-reading `guest_speaker_labels` out of all ~140 episode parts on every run
-- the step that made the build slow against the Hub. It now reads the ~0.6 MB
index instead, falling back to the part scan only when the index is absent
(older dataset).

New `get_podcasts_by_guest()` and `search_by_guest()` mirror the host methods
and, unlike `search_by_speaker_name(role="guest")`, return appearances rather
than mentions. Both are answered from the guest indexes alone, which the default
metadata download now fetches.

## 1.1.2

Host lookups are answered from small metadata indexes, and the host/guest
episode filters actually work.

The dataset now ships `metadata/host_index.parquet` (host name -> podcast,
249,196 rows) and `metadata/host_episode_index.parquet` (host name -> episode,
535,337 rows), both built from the catalogs. The default metadata download
fetches them alongside the other catalogs, so `get_podcasts_by_host()` and
`search_by_host()` work out of the box -- and under `allow_downloads=False`.
Before this, nothing downloaded the files and every call raised
`IndexNotBuiltError` against a fresh Hub-backed dataset.

`search_episodes(host_name=...)` and `search_episodes(guest_name=...)` are now
answered through those indexes (guest names through the `guest` rows of
`speaker_name_index.parquet`) rather than by scanning every episode's predicted
name list in Python. When the indexes are absent -- an older dataset build --
both fall back to the row scan.

That row scan had a latent bug: it tested each cell with `isinstance(names,
list)`, but `to_pandas()` returns the predicted-name columns as numpy arrays, so
the test never matched and both filters silently returned nothing. The fallback
now accepts any non-string iterable, so `host_name`/`guest_name` search returns
results whether or not the new indexes are present.

The `guest_name` filter carries the same mention-vs-appearance caveat as
`search_by_speaker_name(role="guest")`: predicted guest names include people who
were only discussed. Host names carry no such artefact.

## 1.1.1

Column-projected reads against the Hub no longer download whole part files.

A part file is tens to ~144 MB and holds many podcasts. Reading only a couple
of columns from one still fetched the entire object and then discarded ~99% of
it, because `path()` downloads a whole file and the column projection ran after.
Two access patterns paid for this:

- Building the tutorial subset scans `guest_speaker_labels` across every episode
  part to find diarized guests -- two columns out of ~140 parts, which came to
  ~15 GB downloaded to read well under 100 MB.
- Per-podcast turn probes (`episode_has_turn_data`) read a single column of one
  row group but pulled the whole turns part it lived in, ~100 MB per uncached
  podcast.

Both now range-read over `HfFileSystem`, so Parquet fetches just the footer and
the requested column chunks. `DataSource` gains `read_columns` (all row groups)
and `read_row_group_columns` (one podcast's row group); the Hub source serves
them with HTTP range requests and keeps the existing 429 backoff.

Whole-object reads are unchanged. A full read of a podcast still fetches its part
in full and caches it, so category-ordered iteration -- where neighbours share a
file -- stays cheap. Only column-projected reads of a part not already on disk
take the range path, and a range read does not persist the part to the cache,
so a probe leaves nothing behind for a later reader to trip over.

## 1.1.0

Supports SPoRC dataset version 1.1. **This release is required for that
dataset, and does not read the 1.0 layout.** If you need the old data, pin
`sporc<1.1`.

### The dataset is packed differently

Version 1.0 gave every podcast its own directory, which came to roughly 685,000
files. Downloading in bulk made hundreds of thousands of requests and ran into
Hugging Face's rate limits, failing with HTTP 429 partway through. The data now
sits in 543 data files, with each podcast occupying exactly one row group and
`metadata/shard_map.parquet` recording where.

Reading one podcast still costs one request. Reading everything costs a few
hundred instead of hundreds of thousands.

- `metadata/shard_map.parquet` is fetched as part of the core metadata.
- `has_turn_data()` is now answered from memory. It used to probe for a file,
  which against the Hub meant an HTTP request per podcast.
- `prefetch()` resolves the podcasts you asked for to the distinct part files
  holding them and fetches each once. Podcasts share parts, and neighbours in
  the category ordering land together, so a few hundred podcasts from one
  category usually come down as a handful of files.
- Concurrent prefetch downloads dropped from 16 to 4. The files are large now,
  so bandwidth rather than latency sets the pace, and a narrower pool leaves
  room under the request limit.

### Rate limiting is handled rather than raised

HTTP 429 is a wait, not a failure: the Hub's request window always reopens.
Downloads now back off and retry, preferring the server's `Retry-After`, up to a
capped delay. Other HTTP errors still fail immediately, because a revoked token
will not fix itself. Requires `huggingface_hub>=1.2.0`.

### Turn coverage roughly doubled

Dataset 1.1 merged in 358,509 episodes that had been diarized but never joined
to the corpus, taking coverage from 372,604 episodes (33%) to 731,113 (65%).
Speaker labels for the new episodes are anonymous — `SPEAKER_00` and the like —
with no inferred names or roles, so name-based analyses see no benefit while
turn-structure analyses see nearly twice the data. `Episode.has_turn_data` is
still the way to tell a coverage gap from a genuinely turn-less episode.

### Turn changes

- `Turn.speakers_recomputed` says whether a turn's speaker labels came from the
  corrected matcher in dataset 1.1 or were carried over from 1.0 unchanged.
  Filter on it if your work needs a single consistent method.
- **`Turn.word_count` counts words, and `Turn.token_count` is new.** The
  dataset carries two counts per turn that mean different things:
  `turns/text.token_count` counts the timestamped tokens the transcript aligned
  to the turn, punctuation included, while `turns/metrics.word_count` and
  `episode_metrics.total_word_count` count whitespace-separated words. The
  median ratio between them is 1.21. Both were called `word_count` while 1.1
  was being built; the dataset renamed the first one before release, and the
  client reads either name.

  `Turn.word_count` is the words one. It is defined for every turn, and a
  turn's count now adds up to the episode totals it belongs to. `token_count`
  exposes the aligner's number, and is `None` for the 18,250,545 turns (9.9%,
  across 84,760 episodes) carried over from dataset 1.0 — exactly those with
  `speakers_recomputed = False`, a correspondence that holds across all
  185,218,224 rows. Version 1.0 had no such column at all and the word lists it
  came from are gone.

- `Turn.token_count` returns `None` rather than `NaN` when absent. Joining the
  acoustic features on goes through pandas, which represents a missing integer
  as `float('nan')`; since NaN is not None the stored value was handed back
  untouched, and summing over an affected episode produced NaN rather than a
  number.
- **A turn may now have an empty speaker list.** Where diarization produced no
  segments, the transcript arrives as one unattributed turn. Previously `Turn`
  rejected this and the backend discarded such rows, which would have made
  twelve episodes' text unreachable rather than merely unattributed. Code that
  assumes `turn.speaker[0]` exists needs a guard.

### Acoustic features are no longer loaded unless you ask

Reading a podcast's turns also read its acoustics, always. That is a separate
14.5 GB tree of 140 part files, and in the packed layout a read costs a whole
part, so anything touching `episode.turns` paid for MFCCs whether or not it
looked at one. Six of the eight tutorial notebooks never read a single acoustic
value.

`Turn`'s audio fields are now `None` and `get_audio_features()` returns `{}`
until you pass `load_audio_features=True` to `SPORCDataset`. For the tutorial
workload that is the difference between 462 part files and 40 GB, and 346 and
27 GB.

### Search

The full-text index ships as two files. `metadata/turns_search.duckdb` (14 GB)
holds the inverted index and enough columns to identify a hit.
`metadata/turns_text.duckdb` holds the turn text and is optional. It is 33 GB
for text that is 13 GB as Parquet in `turns/text`, so take it only if you want
search to return text without leaving SQL.

- `fts` mode works with the index alone. Text is returned when the text database
  is present.
- `exact` and `regex` match against the text itself. With the text database they
  run in SQL; without it they fall back to scanning local Parquet.
- Ranked search now applies its limit before joining the text on, rather than
  joining all 185 million scored rows and then taking twenty. Measured at 24.3s
  before and 16.8s after.

### Subsets

`scripts/make_subset.py` writes the packed layout — parts, row groups, and its
own shard map — so a subset is a smaller dataset rather than a differently
shaped one, and the same client code reads both.

**Subsets built with sporc 1.0 do not open in 1.1.** They carry the old
per-podcast layout and no shard map, and raise `DatasetAccessError` on load.
Rebuild them with this version; `--podcast-ids` reproduces an existing
selection.

## 1.0.0

First Parquet-only release.
