# Working with the data

The corpus is ~57 GB and partitioned by podcast, so the podcast is the unit
of transfer. A median podcast is ~75 KB, so the one-time ~195 MB of
metadata catalogs dominates any study of fewer than a few thousand podcasts:

| Study size | Data fetched |
|---|---|
| 10 podcasts | ~750 KB |
| 200 podcasts | ~15 MB |
| 1,000 podcasts | ~75 MB |
| whole corpus | ~31 GB |

## Choosing how much to fetch

```python
# Default: catalogs now, per-podcast data on demand.
sporc = SPORCDataset()

# Fetch a known slice up front. Accepts podcast ids, podcast titles, episode
# ids, or a path to a .json / newline-delimited .txt file of them.
sporc = SPORCDataset(subset=["The NPR Politics Podcast", "Stuff You Should Know"])
sporc = SPORCDataset(subset="my_podcast_ids.txt")

# Pin a run to that slice: anything outside it raises DataNotLocalError
# rather than quietly downloading more.
sporc = SPORCDataset(subset="my_ids.txt", allow_downloads=False)

# Use a local copy of the layout; never downloads anything.
sporc = SPORCDataset(parquet_dir="/path/to/sporc_parquet")

# Download the whole corpus up front (~31 GB, ~685k files).
sporc = SPORCDataset(lazy=False)
```

`prefetch()` does the same job after construction, and reports what it resolved:

```python
sporc.prefetch(["The NPR Politics Podcast"])
# {'podcasts': 1, 'files': 4, 'unresolved': []}
```

## Selecting a subset efficiently

Selection is metadata-only. These methods run off the already-downloaded catalogs
and fetch nothing, so you can narrow to exactly the episodes you want before
pulling any data:

```python
hits = sporc.filter_episodes_by_metrics(min_word_count=5000, limit=200)
hits = sporc.search_by_speaker_name("Ira Glass", role="host")

sporc.prefetch({"episode_ids": [h["episode_id"] for h in hits]})
```

Two things are worth knowing:

- Pass `max_episodes` to `search_episodes`. Matching is metadata-only, but
  building each `Episode` reads that podcast's partition. `category="comedy"`
  matches 62,622 episodes across 14,668 podcasts, so `max_episodes=10` reads 10
  partitions instead.
- `filter_episodes_by_metrics` implies turn data. `episode_metrics` is derived
  from turns, so it only covers diarized episodes and never fetches one
  without them.

## Time span

SPoRC is a two-month snapshot: every episode was published between 1 May and
30 June 2020 (median 28 May). It is not a longitudinal archive, so "trends over
time" means trends across eight weeks. The window straddles a sharp,
dateable event, which makes before/after designs unusually clean.

## Turn coverage

Only 731,101 of 1,124,058 episodes (65%) were diarized into speaker turns.
The rest have a transcript but are marked `SPEAKER_DATA_UNAVAILABLE` upstream. An
empty `episode.turns` is therefore usually a gap in the corpus rather than a fact
about the episode:

```python
if episode.has_turn_data:
    analyze(episode.turns)
```

## Full-text search

`search_turns`, `search_episodes_by_text`, and `concordance` use the DuckDB
full-text index when present, and otherwise scan the turn partitions on disk:

| Local data | Scan (pyarrow) | 26 GB DuckDB index |
|---|---|---|
| 250 episodes (5 MB) | 0.37s | n/a — needs the full 26 GB |
| 1,000 episodes (17 MB) | **1.1s** | " |
| 5,000 episodes (83 MB) | 5.4s | " |
| all 78M turns | impractical | 1.7s open, ~5s/query (50s cold) |

The index is worth its 26 GB only for whole-corpus work. Below ~5,000 episodes,
scanning beats its cold start alone.

```python
sporc = SPORCDataset(include_search_db=True)   # whole corpus; pip install "sporc[duckdb]"

results = sporc.search_turns("artificial intelligence")      # fts (BM25)
results = sporc.search_turns("climate", mode="exact")        # substring
results = sporc.concordance("like", context_words=5)         # KWIC
```

Without the index, `mode="fts"` ranks by term frequency rather than BM25 and
covers only local data. The package warns and reports how many podcasts it scanned.

## Building teaching subsets

`scripts/make_subset.py` cuts a self-contained mini-SPoRC. It filters the catalogs
and episode partitions together, so counts, searches, and statistics are all
true of the subset:

```bash
# Ten disjoint 1k-episode subsets (~57 MB each)
for i in $(seq 1 10); do
  python scripts/make_subset.py --data-dir /path/to/sporc_parquet \
      --out subsets/subset_$i --episodes 1000 --seed $i \
      --exclude-used subsets/used.txt
done
```

Subsets are diarized-only by default, so every episode has turns; pass
`--include-undiarized` to mirror the real corpus's ~65% coverage. Learners point
at the directory and nothing downloads:

```python
sporc = SPORCDataset(parquet_dir="subsets/subset_1")
```

See the [Data loading & subsets guide](guides/loading.md) for the full set of
loading modes.
