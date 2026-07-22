# Changelog

## 1.1.0

Supports SPoRC dataset version 1.1. **This release is required for that
dataset, and does not read the 1.0 layout.** If you need the old data, pin
`sporc<1.1`.

### The dataset is packed differently

Version 1.0 gave every podcast its own directory, which came to roughly 685,000
files. Downloading in bulk made hundreds of thousands of requests and ran into
Hugging Face's rate limits, failing with HTTP 429 partway through. The data now
sits in 545 files, with each podcast occupying exactly one row group and
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

Dataset 1.1 merged in 358,497 episodes that had been diarized but never joined
to the corpus, taking coverage from 372,604 episodes (33%) to 731,101 (65%).
Speaker labels for the new episodes are anonymous — `SPEAKER_00` and the like —
with no inferred names or roles, so name-based analyses see no benefit while
turn-structure analyses see nearly twice the data. `Episode.has_turn_data` is
still the way to tell a coverage gap from a genuinely turn-less episode.

### Turn changes

- `Turn.speakers_recomputed` says whether a turn's speaker labels came from the
  corrected matcher in dataset 1.1 or were carried over from 1.0 unchanged.
  Filter on it if your work needs a single consistent method.
- `Turn.word_count` prefers the count the dataset carries and falls back to
  splitting the text on whitespace when it does not.

  **The two are not the same measure.** The dataset's count is the number of
  aligned word tokens from diarization, which ran higher than a whitespace
  split on 55% of turns and by as much as several dozen words. 18,336,086 turns
  (9.9%, across 84,760 episodes) carry no count, because they were carried over
  from 1.0 unchanged and the word lists that defined it no longer exist. Those
  turns are exactly the ones with `speakers_recomputed = False` — the
  correspondence is exact across all 185,303,765 rows — so filter on that field
  if a consistent word count matters. Anything summing `word_count` over the
  whole corpus is mixing two definitions.
- Acoustic features gained standard deviations alongside the means:
  `mfcc1_sma3_stdev` and the rest.
- `Turn.word_count` no longer returns `NaN`. Joining the acoustic features on
  goes through pandas, which represents a missing integer as `float('nan')`;
  since NaN is not None the stored value was handed back untouched, and summing
  word counts over an affected episode produced NaN rather than a number.
- **A turn may now have an empty speaker list.** Where diarization produced no
  segments, the transcript arrives as one unattributed turn. Previously `Turn`
  rejected this and the backend discarded such rows, which would have made
  twelve episodes' text unreachable rather than merely unattributed. Code that
  assumes `turn.speaker[0]` exists needs a guard.

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
