# Changelog

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
