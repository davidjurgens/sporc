---
language:
  - en
license: other
license_name: research-use-only
tags:
  - podcasts
  - transcripts
  - audio
  - prosody
  - NLP
  - speaker-diarization
  - sociolinguistics
  - parquet
task_categories:
  - text-classification
  - audio-classification
  - token-classification
pretty_name: "SPoRC: Structured Podcast Open Research Corpus"
size_categories:
  - 1M<n<10M
---

# SPoRC: the Structured Podcast Open Research Corpus (V 1.1)

SPoRC is a large multimodal dataset for studying the podcast ecosystem. It contains metadata, full transcripts, speaker-turn-level diarization, speaker-role labels, and acoustic features for over **1.1 million podcast episodes** across **228,000 podcasts**.

**Paper:** [Mapping the Podcast Ecosystem with the Structured Podcast Research Corpus](https://aclanthology.org/2025.acl-long.1222/) (ACL 2025)

**Upgrading from version 1.0?** Read [What changed since version 1.0](#what-changed-since-version-10) first. The file layout is different, speaker turns have been recomputed, and twice as many episodes now have them.

## Dataset Summary

| Statistic | Count |
|---|---|
| Podcasts | 228,099 |
| Episodes | 1,124,058 |
| Episodes with speaker turns | 731,113 (65%) |
| Speaker turns | 185,218,224 |
| Speaker name index entries | 921,287 |
| Apple Podcast categories covered | 20 main + subcategories |
| Languages | 60+ (primarily English) |

## What changed since version 1.0

Three things in this release will affect anyone who has already worked with SPoRC. Two of them concern speaker turns; the third is a corrected column. None of them changes the transcripts, which are the same text as before.

The files are also packed differently. Version 1.0 stored one directory per podcast, which came to about 685,000 files and meant that downloading the corpus made hundreds of thousands of separate requests — enough to hit Hugging Face's rate limits and fail with 429 errors partway through. The same data now sits in 534 files, each holding many podcasts, with an index at `metadata/shard_map.parquet` recording which file and which row group holds each podcast. Fetching a single podcast still costs one request; fetching everything costs a few hundred instead of hundreds of thousands.

### Speaker labels have been recomputed, and many of them changed

Turns are built by taking the transcript, which has a timestamp on every word, and the diarization output, which says which speaker was talking during which stretch of audio, and matching one against the other. A word spoken between 61.2 and 61.5 seconds belongs to whichever speaker held the floor then.

The code that did this matching in version 1.0 walked through the words and the speaker segments together, in order, assuming both lists ran front to back in time. The word lists do not always run front to back. In roughly a third of episodes some rows sit out of sequence, and when the matcher hit one of those it moved its position in the segment list past segments that were still open. Any word that came later and belonged to one of those segments came out with no speaker attached, or with the wrong one.

Long segments lost the most, since a long segment covers many words and every one of them was at risk. Passages where two people talk over each other were also badly affected, because those produce short interleaved segments that the matcher skipped past easily.

We rebuilt the turns using a matcher that keeps track of every segment still open at a given moment instead of a single position in the list. On episodes where both versions have turns, the old and new labels agree on about 84 percent of words. The other 16 percent are words the old code left unassigned or assigned to the wrong person.

Turn boundaries and turn counts have therefore moved. An episode with 40 turns in version 1.0 may have 45 now, because stretches previously merged into one speaker's turn are correctly split between two. Per-turn measurements will change accordingly: turn length, how often speakers alternate, how much speech overlaps. Conversational shows with several participants shift the most; scripted monologue barely moves.

84,760 episodes could not be rebuilt, a little under a quarter of those that had turns in version 1.0. The intermediate files their turns were originally derived from no longer exist, so there is nothing to redo the matching against. Rather than drop them and lose turn data people are using, we kept their version 1.0 turns as they were. Every turn carries a `speakers_recomputed` field so you can tell the two apart, and filter to only the rebuilt ones if your work needs a single consistent method.

### Many more episodes have turns now, but their speakers are anonymous

Version 1.0 had speaker turns for 372,604 episodes, about a third of the corpus. This release has them for 731,113, about two thirds.

The 358,509 additional episodes are not new recordings. Diarization had already been run on them; it had simply never been merged with the transcripts, so the results sat unused. This release does that merge.

For these episodes we know when each person spoke, but not who they are. Speakers are labelled `SPEAKER_00`, `SPEAKER_01`, and so on. Those labels only mean something inside a single episode: `SPEAKER_00` in one episode has no relationship to `SPEAKER_00` in another, and no relationship to any named person. The classifier that infers host and guest names from the transcript was not run over these episodes, so `inferred_speaker_name` and `inferred_speaker_role` are empty for them.

If you search by speaker name, or rely on knowing which turns belong to the host, you are working with the original set of episodes only. If you are studying turn-taking, interruption, speaking time, or anything else that depends on who spoke when rather than who they are, the newly added episodes are usable and roughly double how much material you have.

### unique_speaker_count in episode_metrics was wrong and is now fixed

In version 1.0 this column was built from each turn's position in the episode rather than from the speaker labels, so it counted turns. It equalled `total_turn_count` in 98.85 percent of episodes and ran as high as 7,683, which is not a plausible number of speakers for a podcast episode.

It now counts distinct speaker labels in the episode, which is what the name says. Typical values are in the low single digits. Any analysis that used it as a speaker count was measuring turn count instead, and any threshold built on it will need revisiting.

### Smaller changes

- Turn files carry counts, so counting no longer means tokenising the text yourself: `turns/metrics.word_count` for words, defined everywhere, and `turns/text.token_count` for the aligner's timestamped tokens, null for the 9.9% of turns carried over from 1.0. They differ by about 21%; see the schema note before summing either.
- Acoustic features now include a standard deviation as well as a mean for each measure, and live in their own `acoustics/` tree rather than beside the turn text, so they are only downloaded if you ask for them.
- `mp3_url` has been dropped from the turn files, where it repeated on every row. It is still on every episode, in `episodes/` and in `metadata/episode_catalog.parquet`.
- The search index is now two files. `metadata/turns_search.duckdb` holds the index and is 14 GB, down from 24.2 GB while covering 2.4x as many turns, because it no longer stores a second copy of the turn text. That text is in `metadata/turns_text.duckdb`, which is optional and large — 33 GB, against 13 GB for the same text as Parquet in `turns/text`. Take it if you want search to return the matching text without leaving SQL, or join back to `turns/text` on `(episode_id, turn_count)` yourself.

## Data Format

The dataset is stored as **Parquet files** with **zstd compression**. A `manifest.json` file at the root describes the layout and record counts.

### Directory Structure

```
├── manifest.json                              # Layout description and record counts
├── README.md                                  # This file
├── metadata/
│   ├── README.md                              # Catalogs, indexes, shard map, search DBs
│   ├── podcast_catalog.parquet                # One row per podcast (228K rows)
│   ├── episode_catalog.parquet                # One row per episode, no transcripts (1.1M rows)
│   ├── shard_map.parquet                      # podcast_id → which file and row group holds it
│   ├── category_index.parquet                 # category → podcast_id mapping (572K rows)
│   ├── hostname_index.parquet                 # hostname → podcast_id mapping (228K rows)
│   ├── speaker_name_index.parquet             # Speaker name lookup index (921K rows)
│   ├── episode_metrics.parquet                # Precomputed episode-level metrics (731K rows)
│   ├── turns_search.duckdb                    # Full-text search index over turns
│   └── turns_text.duckdb                      # Turn text for search (optional)
├── episodes/
│   ├── README.md                              # Episode records and transcripts
│   └── part-*.parquet                         # Full episode data including transcript (140 files)
├── turns/
│   ├── README.md                              # How the turn trees relate and join
│   ├── text/README.md                         # Turn schema, sentinels, empty text
│   ├── text/part-*.parquet                    # Turn text, timing, and speaker info (127 files)
│   ├── metrics/README.md                      # Per-turn measures, first-turn nulls
│   └── metrics/part-*.parquet                 # Turn-level computed metrics (127 files)
└── acoustics/
    ├── README.md                              # eGeMAPS measures and their units
    └── part-*.parquet                         # MFCCs, F0, formants per turn (140 files)
```

Each directory carries its own README with the full schema for the files in it,
the traps specific to those columns, and a runnable example that uses only
`pyarrow`, `pandas` or `duckdb`. Start there when working with one tree; this
file covers the layout as a whole.

### Finding one podcast's data

Each part file holds many podcasts, and **each podcast occupies exactly one row group**. `metadata/shard_map.parquet` records where:

| Column | Type | Description |
|---|---|---|
| `podcast_id` | string | Podcast identifier |
| `tree` | string | One of `episodes`, `turns_text`, `turns_metrics`, `acoustics` |
| `part` | string | File name within that tree |
| `row_group` | int64 | Row group index holding this podcast |
| `num_rows` | int64 | Rows in that row group |

Reading a single row group means one ranged read, so fetching one podcast costs about as much as it did when every podcast had its own file — without the hundreds of thousands of files.

```python
import pyarrow.parquet as pq
import pandas as pd

smap = pd.read_parquet("metadata/shard_map.parquet")
row = smap[(smap.podcast_id == "03b0f2a257fd") & (smap.tree == "turns_text")].iloc[0]
turns = pq.ParquetFile(f"turns/text/{row['part']}").read_row_group(row["row_group"])
```

Podcasts are ordered by primary category and then by id, so a category-scoped query touches a short run of consecutive files rather than all of them.

### ID Scheme

| ID | Derivation | Length | Unique across |
|---|---|---|---|
| `podcast_id` | `md5(rss_url)[:12]` | 12 hex chars | Globally unique |
| `episode_id` | `md5(mp3_url)[:16]` | 16 hex chars | Globally unique |

Episodes link to podcasts via `podcast_id`. Turns link to episodes via `episode_id`.

---

## Getting the Files

The dataset is gated: accept the terms on the dataset page and authenticate with `huggingface-cli login` before downloading. Everything below uses standard Hugging Face and Parquet tooling — no SPoRC-specific software is required.

### Just the catalogs (about 200 MB)

Enough to browse every podcast and episode, filter by category, language or date, and decide what you actually need.

```bash
huggingface-cli download blitt/SPoRC --repo-type dataset \
  --include "metadata/*.parquet" --exclude "metadata/turns_search.duckdb" \
  --local-dir SPoRC
```

### One tree (transcripts, or turns, or acoustics)

Each tree is independent. Take only the ones your work needs — acoustics in particular is bulky and rarely used.

```bash
# Transcripts only
huggingface-cli download blitt/SPoRC --repo-type dataset \
  --include "episodes/*" --local-dir SPoRC

# Speaker turns only
huggingface-cli download blitt/SPoRC --repo-type dataset \
  --include "turns/text/*" --local-dir SPoRC
```

### A few podcasts, without downloading a whole file

Because each podcast is exactly one row group, a single podcast can be read straight off the Hub with ranged requests. Nothing is written to disk and the rest of the part file is never transferred.

```python
import pandas as pd
import pyarrow.parquet as pq
from huggingface_hub import HfFileSystem, hf_hub_download

REPO = "blitt/SPoRC"

# The shard map is small; fetch it once.
smap = pd.read_parquet(hf_hub_download(
    REPO, "metadata/shard_map.parquet", repo_type="dataset"))

fs = HfFileSystem()

def read_podcast(podcast_id, tree, directory):
    row = smap[(smap.podcast_id == podcast_id) & (smap.tree == tree)].iloc[0]
    path = f"datasets/{REPO}/{directory}/{row['part']}"
    with fs.open(path, "rb") as fh:
        return pq.ParquetFile(fh).read_row_group(row["row_group"]).to_pandas()

turns = read_podcast("03b0f2a257fd", "turns_text", "turns/text")
episodes = read_podcast("03b0f2a257fd", "episodes", "episodes")
```

This is the access pattern the layout exists for. Version 1.0 gave every podcast its own file, which made this simple but meant ~685,000 files in the repository and rate-limit failures for anyone fetching in bulk.

### Everything

```bash
huggingface-cli download blitt/SPoRC --repo-type dataset --local-dir SPoRC
```

545 files and 94 GB in total: a few hundred requests rather than the hundreds of thousands version 1.0 required. Most of that is the two search databases, and both are optional.

| What you take | Size |
|---|---|
| Parquet only (`episodes`, `turns`, `acoustics`, `metadata/*.parquet`) | 46 GB |
| plus `metadata/turns_search.duckdb` | 61 GB |
| plus `metadata/turns_text.duckdb` | 94 GB |

`turns_text.duckdb` is 33 GB to hold text that takes 13 GB as Parquet in `turns/text`, because DuckDB's string compression is not zstd. Take it if you want full-text search to return the matching text without leaving SQL; otherwise join back to `turns/text` on `(episode_id, turn_count)`.

To skip both:

```bash
huggingface-cli download blitt/SPoRC --repo-type dataset --local-dir SPoRC \
  --exclude "metadata/*.duckdb"
```

---

## Schema Reference

### `metadata/podcast_catalog.parquet`

One row per podcast with aggregated statistics. This is the best starting point for browsing or filtering podcasts.

| Column | Type | Description |
|---|---|---|
| `podcast_id` | string | Unique podcast identifier |
| `rss_url` | string | RSS feed URL |
| `pod_title` | string | Podcast title |
| `pod_description` | string | Podcast description |
| `language` | string | Language code (e.g., `en`, `en-au`, `es`) |
| `explicit` | int64 | Explicit content flag (0 or 1) |
| `image_url` | string | Podcast cover image URL |
| `itunes_author` | string | iTunes author field |
| `episode_count` | int64 | Number of episodes in the dataset |
| `total_duration_seconds` | double | Sum of all episode durations |
| `primary_category` | string | Main Apple Podcast category |
| `all_categories` | list\<string\> | All assigned categories |
| `host_names` | list\<string\> | Predicted host names across episodes |
| `earliest_date` | string | Earliest episode date (ISO 8601) |
| `latest_date` | string | Latest episode date (ISO 8601) |

### `metadata/episode_catalog.parquet`

One row per episode with key metadata (no transcripts). Use this for filtering and discovery before loading full episode data.

| Column | Type | Description |
|---|---|---|
| `episode_id` | string | Unique episode identifier |
| `podcast_id` | string | Parent podcast identifier |
| `ep_title` | string | Episode title |
| `mp3_url` | string | Audio file URL |
| `duration_seconds` | double | Episode duration in seconds |
| `category1`–`category10` | string | Apple Podcast categories (up to 10) |
| `host_predicted_names` | list\<string\> | NER-predicted host names |
| `guest_predicted_names` | list\<string\> | NER-predicted guest names |
| `num_main_speakers` | int64 | Number of main speakers (see note below) |
| `language` | string | Language code |
| `explicit` | int64 | Explicit content flag (0 or 1) |
| `episode_date` | string | Publication date (millisecond timestamp as string) |
| `overlap_prop_duration` | double | Proportion of episode duration with overlapping speech |
| `avg_turn_duration` | double | Average speaker turn duration in seconds |
| `total_sp_labels` | int64 | Total number of distinct speaker labels |

`total_sp_labels`, `avg_turn_duration` and `overlap_prop_duration` are recomputed from the turns in this release, so they describe what is actually in the files. `total_sp_labels > 0` is the test for whether an episode has turns.

`num_main_speakers` counted speakers above a speaking-time threshold in version 1.0. The rule that produced it lived in the upstream pipeline and could not be reproduced exactly, so version 1.0's values are kept where they exist, and newly diarized episodes report every speaker detected.

### `episodes/part-*.parquet`

Full episode data including transcripts.

| Column | Type | Description |
|---|---|---|
| `episode_id` | string | Unique episode identifier |
| `podcast_id` | string | Parent podcast identifier |
| `ep_title` | string | Episode title |
| `ep_description` | string | Episode description |
| `mp3_url` | string | Audio file URL |
| `duration_seconds` | double | Episode duration in seconds |
| `transcript` | string | Full episode transcript |
| `rss_url` | string | Podcast RSS feed URL |
| `pod_title` | string | Podcast title |
| `pod_description` | string | Podcast description |
| `category1`–`category10` | string | Apple Podcast categories |
| `host_predicted_names` | list\<string\> | NER-predicted host names |
| `guest_predicted_names` | list\<string\> | NER-predicted guest names |
| `neither_predicted_names` | list\<string\> | Named speakers classified as neither host nor guest |
| `main_ep_speakers` | list\<string\> | Speaker labels with >5% speaking time |
| `host_speaker_labels` | string | JSON mapping of host names → speaker labels |
| `guest_speaker_labels` | string | JSON mapping of guest names → speaker labels |
| `num_main_speakers` | int64 | Number of main speakers |
| `overlap_prop_duration` | double | Overlap proportion by duration |
| `overlap_prop_turn_count` | double | Overlap proportion by turn count |
| `avg_turn_duration` | double | Average turn duration |
| `total_sp_labels` | int64 | Total distinct speaker labels |
| `language` | string | Language code |
| `explicit` | int64 | Explicit content flag |
| `image_url` | string | Episode/podcast image URL |
| `episode_date_localized` | string | Localized publication date |
| `oldest_episode_date` | string | Oldest episode date for the podcast |
| `last_update` | string | Last update timestamp |
| `created_on` | string | Creation timestamp |
| `itunes_author` | string | iTunes author |
| `itunes_owner_name` | string | iTunes owner name |
| `host` | string | Host field from RSS |

### `turns/text/part-*.parquet`

Speaker turn text, timing, and speaker information. One row per turn.

| Column | Type | Description |
|---|---|---|
| `episode_id` | string | Parent episode identifier |
| `podcast_id` | string | Parent podcast identifier |
| `speaker` | list\<string\> | Speaker label(s) for this turn (e.g., `["SPEAKER_03"]`); more than one means overlapping speech |
| `turn_text` | string | Text spoken in this turn |
| `start_time` | double | Turn start time in seconds |
| `end_time` | double | Turn end time in seconds |
| `duration` | double | Turn duration in seconds |
| `turn_count` | int32 | Sequential turn index within the episode (0-based) |
| `token_count` | int32 | **Timestamped tokens** the transcript aligned to this turn, punctuation counted separately — about 21% above a word count. **Null for the 18,250,545 turns (9.9%) carried over from version 1.0**, whose word lists no longer exist — exactly the rows with `speakers_recomputed = false` |
| `inferred_speaker_name` | string | Predicted speaker name, or `NO_INFERRED_SPEAKER` |
| `inferred_speaker_role` | string | `"host"`, `"guest"`, `"neither"`, or `NO_INFERRED_ROLE` |
| `speakers_recomputed` | bool | `true` if labels come from this release's matcher, `false` if carried over from version 1.0 |

### `acoustics/part-*.parquet`

Acoustic features per turn. Join with `turns/text` on `(episode_id, turn_count)`. Kept in a separate tree because they are bulky and rarely needed.

| Column | Type | Description |
|---|---|---|
| `episode_id` | string | Parent episode identifier |
| `podcast_id` | string | Parent podcast identifier |
| `turn_count` | int32 | Turn index (join key) |
| `mfcc1_sma3_mean` … `mfcc4_sma3_mean` | double | Mean of MFCC coefficients 1–4 |
| `mfcc1_sma3_stdev` … `mfcc4_sma3_stdev` | double | Standard deviation of the same, across the turn |
| `f0_semitone_from_27_5hz_sma3nz_mean` | double | Mean fundamental frequency in semitones (re 27.5 Hz) |
| `f0_semitone_from_27_5hz_sma3nz_stdev` | double | Standard deviation of the same |
| `f1_frequency_sma3nz_mean` | double | Mean 1st formant frequency |
| `f1_frequency_sma3nz_stdev` | double | Standard deviation of the same |

The mean columns keep the names they had in version 1.0. Standard deviations are new.

### `turns/metrics/part-*.parquet`

Precomputed turn-level metrics. Join with `turns/text` on `(episode_id, turn_count)`.

| Column | Type | Description |
|---|---|---|
| `episode_id` | string | Parent episode identifier |
| `turn_count` | int32 | Turn index (join key) |
| `word_count` | int32 | Whitespace-separated **words** in the turn. Defined for every row, and the measure `episode_metrics.total_word_count` sums |
| `words_per_second` | float | Speaking rate, from this word count |
| `gap_from_prev` | float | Silence gap from previous turn (seconds) |
| `overlap_with_prev` | float | Overlap with previous turn (seconds) |
| `discourse_marker_count` | int16 | Count of discourse markers (e.g., "um", "like", "you know") |
| `char_count` | int32 | Character count of turn text |

> **`token_count` is not a word count.** `turns/text.token_count` counts the
> timestamped tokens the transcript aligned to a turn, with punctuation as its
> own token, and runs about 21% above the number of words. Sum
> `turns/metrics.word_count` if you want words — it is what
> `episode_metrics.total_word_count` is built from and it is defined for every
> turn. Version 1.0 had neither column, so both are new here.

### `metadata/category_index.parquet`

Lookup table mapping categories to podcast IDs.

| Column | Type | Description |
|---|---|---|
| `category` | string | Apple Podcast category (lowercased) |
| `podcast_id` | string | Podcast identifier |

### `metadata/hostname_index.parquet`

Lookup table mapping RSS hostnames to podcast IDs.

| Column | Type | Description |
|---|---|---|
| `hostname` | string | RSS feed hostname |
| `podcast_id` | string | Podcast identifier |

### `metadata/speaker_name_index.parquet`

Index for searching by speaker name across the corpus. Built from the episode-level name predictions, so it covers only episodes where names were inferred — not the newly diarized ones.

| Column | Type | Description |
|---|---|---|
| `name_normalized` | string | Lowercased, whitespace-normalized speaker name |
| `name_original` | string | Original speaker name |
| `role` | string | Speaker role (`"host"`, `"guest"`, or `"neither"`) |
| `episode_id` | string | Episode identifier |
| `podcast_id` | string | Podcast identifier |

### `metadata/episode_metrics.parquet`

Precomputed episode-level aggregate metrics, for all 731,113 episodes with turn rows.

| Column | Type | Description |
|---|---|---|
| `episode_id` | string | Episode identifier |
| `podcast_id` | string | Podcast identifier |
| `total_word_count` | int32 | Total words in episode |
| `total_turn_count` | int32 | Total speaker turns |
| `unique_speaker_count` | int32 | Number of distinct speaker labels (see the note above — this was wrong in 1.0) |
| `avg_turn_duration` | float | Mean turn duration (seconds) |
| `median_turn_duration` | float | Median turn duration |
| `avg_words_per_second` | float | Mean speaking rate |
| `host_word_count` | int32 | Words spoken by host(s) |
| `guest_word_count` | int32 | Words spoken by guest(s) |
| `host_turn_proportion` | float | Proportion of turns by host |
| `host_word_proportion` | float | Proportion of words by host |
| `avg_gap_duration` | float | Mean silence between turns |
| `total_overlap_duration` | float | Total overlapping speech (seconds) |
| `discourse_marker_count` | int32 | Total discourse markers |
| `discourse_marker_rate` | float | Discourse markers per 1,000 words |
| `speaking_rate_host` | float | Host speaking rate (words/sec) |
| `speaking_rate_guest` | float | Guest speaking rate (words/sec) |

Host and guest columns are zero for the newly diarized episodes, which have no role labels.

---

## Using the `sporc` Python Package

Everything in this dataset is plain Parquet and can be used with pandas, DuckDB, Arrow, polars or R, as the next section shows. The [`sporc` package](https://pypi.org/project/sporc/) is a convenience layer on top of exactly those files: it resolves the shard map, issues the ranged reads, and wraps search and filtering in an API. Use it if it suits you, and use the files directly if it does not — neither path is second class, and nothing in the dataset is reachable only through the package.

```bash
pip install sporc
```

Version 1.1 of the dataset requires **sporc 1.1 or later**. Earlier versions expect the old per-podcast layout and will not find the files.

```python
from sporc import SPORCDataset

# Load the dataset
dataset = SPORCDataset()

# Search for a podcast
podcast = dataset.search_podcast("My Favorite Murder")

# Iterate episodes with lazy-loaded turns
for episode in podcast.episodes:
    print(episode.title, len(episode.turns), "turns")

# Full-text search across turns
results = dataset.search_turns("artificial intelligence", mode="fts")

# Search by speaker name
results = dataset.search_by_speaker_name("Ira Glass", role="host")

# KWIC concordance
results = dataset.concordance("like", context_words=5)
```

See the [sporc package documentation](https://github.com/blitt/sporc) for the full API.

---

## Working with the Parquet Files Directly

SPoRC is readable from any language or tool that supports Parquet. This section is self-contained: with the download commands above and the shard map, everything in the dataset is reachable without installing anything SPoRC-specific.

### Python (pandas / pyarrow)

```python
import pandas as pd
import pyarrow.parquet as pq

# Load the podcast catalog
podcasts = pd.read_parquet("metadata/podcast_catalog.parquet")
print(f"{len(podcasts)} podcasts")

# Filter to comedy podcasts with 50+ episodes
comedy = podcasts[
    (podcasts["primary_category"] == "comedy")
    & (podcasts["episode_count"] >= 50)
]

# Load the episode catalog (no transcripts — fast)
episodes = pd.read_parquet("metadata/episode_catalog.parquet")

# Get episodes for a specific podcast
pod_episodes = episodes[episodes["podcast_id"] == "03b0f2a257fd"]

# Read one podcast's turns via the shard map — one row group, one ranged read
smap = pd.read_parquet("metadata/shard_map.parquet")

def read_podcast(podcast_id, tree, directory):
    row = smap[(smap.podcast_id == podcast_id) & (smap.tree == tree)].iloc[0]
    return pq.ParquetFile(f"{directory}/{row['part']}").read_row_group(
        row["row_group"]).to_pandas()

turns = read_podcast("03b0f2a257fd", "turns_text", "turns/text")
audio = read_podcast("03b0f2a257fd", "acoustics", "acoustics")
turns_with_audio = turns.merge(audio, on=["episode_id", "turn_count"])
```

### Python (DuckDB) — query across the whole corpus

These read every part file, so budget minutes rather than seconds: the turn
trees are 185,218,224 rows and the last query below joins two of them. DuckDB
only reads the columns you name, which is most of what keeps this tractable, so
select narrowly. Where a question is really about a few podcasts, the shard map
route above is faster by orders of magnitude.

DuckDB will also want scratch space for the larger joins. Point it somewhere
with room, since the default may be a small `/tmp`:

```python
con.execute("SET temp_directory='/path/with/space'")
```

```python
import duckdb

con = duckdb.connect()

# Query across every part file
result = con.sql("""
    SELECT podcast_id, episode_id, turn_text, inferred_speaker_role, duration
    FROM read_parquet('turns/text/part-*.parquet')
    WHERE inferred_speaker_role = 'host'
      AND duration > 30
    LIMIT 100
""").df()

# Restrict to the turns recomputed in this release
consistent = con.sql("""
    SELECT episode_id, COUNT(*) AS turns
    FROM read_parquet('turns/text/part-*.parquet')
    WHERE speakers_recomputed
    GROUP BY episode_id
""").df()

# Search transcripts
matches = con.sql("""
    SELECT podcast_id, episode_id, ep_title, transcript
    FROM read_parquet('episodes/part-*.parquet')
    WHERE transcript ILIKE '%machine learning%'
    LIMIT 20
""").df()

# Aggregate speaking statistics across the corpus
stats = con.sql("""
    SELECT inferred_speaker_role,
           COUNT(*) AS turn_count,
           AVG(t.duration) AS avg_duration,
           AVG(m.words_per_second) AS avg_speaking_rate
    FROM read_parquet('turns/text/part-*.parquet') t
    JOIN read_parquet('turns/metrics/part-*.parquet') m
      ON t.episode_id = m.episode_id AND t.turn_count = m.turn_count
    GROUP BY inferred_speaker_role
""").df()
```

### Full-text search

`metadata/turns_search.duckdb` holds a BM25 index over every turn. It stores identifiers rather than the text, so pair it with `metadata/turns_text.duckdb` and let DuckDB join the two:

```python
import duckdb

con = duckdb.connect("metadata/turns_search.duckdb", read_only=True)
con.execute("LOAD fts")
con.execute("ATTACH 'metadata/turns_text.duckdb' AS txt (READ_ONLY)")

# Rank first, then attach the text to the rows that survive. Joining before the
# LIMIT joins all 185M scored rows to get twenty.
hits = con.sql("""
    WITH top AS (
        SELECT episode_id, podcast_id, turn_count, score FROM (
            SELECT *, fts_main_turns.match_bm25(row_id, 'artificial intelligence') AS score
            FROM turns
        ) WHERE score IS NOT NULL
        ORDER BY score DESC LIMIT 20
    )
    SELECT t.episode_id, t.turn_count, x.turn_text, t.score
    FROM top t JOIN txt.turn_text x USING (episode_id, turn_count)
    ORDER BY t.score DESC
""").df()
```

Without `turns_text.duckdb` the same query works if you drop the join: you get
turn identifiers and scores, and can fetch the text from `turns/text` using the
shard map. Substring and regex search need the text, so they require it.

### R

```r
library(arrow)

# Read metadata catalogs directly
podcasts <- read_parquet("metadata/podcast_catalog.parquet")
episodes <- read_parquet("metadata/episode_catalog.parquet")

# Open a whole tree as one dataset
turns <- open_dataset("turns/text")
host_turns <- turns |>
  filter(inferred_speaker_role == "host") |>
  select(podcast_id, episode_id, turn_text, duration) |>
  head(1000) |>
  collect()

# One podcast, via the shard map
smap <- read_parquet("metadata/shard_map.parquet")
loc <- subset(smap, podcast_id == "03b0f2a257fd" & tree == "turns_text")[1, ]
one <- read_parquet(file.path("turns/text", loc$part))
```

### Command Line (DuckDB CLI)

```bash
# Install: https://duckdb.org/docs/installation/
duckdb

# Count episodes per category
SELECT category1, COUNT(*) AS n
FROM read_parquet('metadata/episode_catalog.parquet')
WHERE category1 != ''
GROUP BY category1
ORDER BY n DESC;

# Find longest episodes
SELECT ep_title, duration_seconds / 3600.0 AS hours
FROM read_parquet('metadata/episode_catalog.parquet')
ORDER BY duration_seconds DESC
LIMIT 10;
```

---

## Key Concepts

### Speaker Labels and Roles

Each turn has a generic **speaker label** (e.g., `SPEAKER_00`, `SPEAKER_03`) assigned by the diarization model. These labels are consistent within an episode but not across episodes.

For episodes that had turns in version 1.0, a role-inference model assigns each speaker one of three **roles**:
- `host` — the podcast host
- `guest` — a guest on the episode
- `neither` — other speakers (e.g., advertisers, co-hosts in ambiguous cases)

An NER model predicts **speaker names** from introductions and context. These appear in `inferred_speaker_name` (per-turn) and `host_predicted_names` / `guest_predicted_names` (per-episode).

The episodes added in version 1.1 have speaker labels but no roles or names: those fields read `NO_INFERRED_ROLE` and `NO_INFERRED_SPEAKER`. Filter on `inferred_speaker_role != 'NO_INFERRED_ROLE'` if your analysis depends on knowing who is who.

The `host_speaker_labels` and `guest_speaker_labels` fields (JSON strings) map predicted names to their generic speaker labels, e.g., `{"John Smith": "SPEAKER_00"}`.

### Overlapping Speech

`speaker` is a list because diarization can mark two people as talking at once. A turn labelled `["SPEAKER_00", "SPEAKER_01"]` covers a stretch where both were active. This release recovers considerably more overlap than version 1.0, whose matcher tended to skip past the short interleaved segments that overlap produces.

### Diarization Quality Indicators

Not all episodes have high-quality diarization. Use these columns to filter:

- **`overlap_prop_duration`**: Proportion of episode duration where multiple speakers are marked as speaking simultaneously. High values (>0.15) may indicate diarization errors.
- **`overlap_prop_turn_count`**: Same concept measured by turn count.
- **`avg_turn_duration`**: Very high values may indicate under-segmented episodes.
- **`total_sp_labels`**: Number of unique speaker labels. Episodes with very high counts may have noisy diarization.

### Audio Features

Turn-level acoustic features are extracted using [openSMILE](https://audeering.github.io/opensmile/) with the eGeMAPSv2 feature set:

- **MFCCs 1–4**: Mel-frequency cepstral coefficients capturing spectral shape (voice quality, timbre)
- **F0 (fundamental frequency)**: Pitch in semitones relative to 27.5 Hz, useful for intonation and prosody analysis
- **F1 (first formant)**: Related to vowel height and openness, useful for phonetic and sociolinguistic analysis

`sma3` means the frame-level values were smoothed with a 3-frame moving average before being aggregated over the turn. Each feature has a mean and a standard deviation across the turn.

### Categories

Categories follow the [Apple Podcasts taxonomy](https://podcasters.apple.com/support/1691-apple-podcasts-categories): 20 main categories (Arts, Business, Comedy, Education, Fiction, Government, Health & Fitness, History, Kids & Family, Leisure, Music, News, Religion & Spirituality, Science, Society & Culture, Sports, Technology, True Crime, TV & Film) with subcategories. Each episode can have up to 10 categories stored in `category1` through `category10`.

---

## Citation

If you use SPoRC in your research, please cite:

```bibtex
@inproceedings{litterer-etal-2025-mapping,
    title = "Mapping the Podcast Ecosystem with the Structured Podcast Research Corpus",
    author = "Litterer, Benjamin Roger  and
      Jurgens, David  and
      Card, Dallas",
    booktitle = "Proceedings of the 63rd Annual Meeting of the Association for Computational Linguistics (Volume 1: Long Papers)",
    month = jul,
    year = "2025",
    address = "Vienna, Austria",
    publisher = "Association for Computational Linguistics",
    url = "https://aclanthology.org/2025.acl-long.1222/",
    doi = "10.18653/v1/2025.acl-long.1222",
    pages = "25132--25154",
}
```

---

## License and Terms of Use

This dataset is released for **research and educational purposes only**. By accessing the dataset, you agree to the terms of use. If you are a podcast creator and would like your content removed, please use the removal request form linked on the dataset page.
