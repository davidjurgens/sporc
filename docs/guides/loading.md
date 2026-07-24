# Data loading & subsets

The SPoRC corpus is ~57 GB and partitioned by podcast, so the podcast is the
unit of transfer. You almost never need all of it. By default the dataset loads
lazily, and you control how much is fetched: a fixed subset up front, the whole
corpus eagerly, or nothing beyond what's already local.

!!! note "The short version"
    - `SPORCDataset()`: lazy by default. Fetches the ~195 MB metadata catalogs
      now, and per-podcast data as you touch it.
    - `SPORCDataset(subset=[...])`: also fetches a fixed slice up front, so later
      access to it needs no network.
    - `SPORCDataset(subset=[...], allow_downloads=False)`: pins a run to that
      slice. Anything outside it raises rather than downloading.
    - `SPORCDataset(lazy=False)`: downloads the whole corpus up front.

## How loading works

Two facts shape everything below:

1. Data is partitioned by podcast. Working with a handful of podcasts costs
   a handful of small files, not the whole corpus. A median podcast is ~75 KB,
   so the one-time ~195 MB of metadata catalogs dominates any study of fewer
   than a few thousand podcasts.
2. Turn data is loaded on demand and only exists for ~65% of episodes. Of
   1,124,058 episodes, 731,101 were diarized into speaker turns. Accessing an
   episode's turns fetches and parses that podcast's turn partition the first
   time; episodes without turn data return an empty list. Always gate on
   `episode.has_turn_data`.

| Study size | Data fetched |
|---|---|
| 10 podcasts | ~750 KB (plus the ~195 MB catalogs) |
| 200 podcasts | ~15 MB |
| 1,000 podcasts | ~75 MB |
| whole corpus | ~31 GB |

## Constructor options

```python
SPORCDataset(
    parquet_dir=None,        # read a local layout; never downloads
    use_auth_token=None,     # HF token; None uses cached credentials
    cache_dir=None,          # HF cache location; None uses the default
    lazy=True,               # fetch per-podcast data on demand (default)
    subset=None,             # fetch a fixed slice up front
    allow_downloads=True,    # False forbids any further downloads
    include_search_db=False, # add the ~14 GB full-text search database
    include_turn_text=False, # add the ~19 GB turn-text database
    load_audio_features=False, # join acoustic features onto turns
    ignore_patterns=None,    # override download exclusions (non-lazy path)
)
```

The parameters that decide *how much data moves* are `lazy`, `subset`, and
`allow_downloads`. The rest are covered in the [API
reference](../reference/sporcdataset.md).

## Lazy loading (the default)

Lazy loading fetches only the metadata catalogs at construction, then pulls each
podcast's partition the first time you touch it. This keeps startup fast and
memory low, and downloads scale with what you actually use.

```python
from sporc import SPORCDataset

# Downloads ~195 MB of catalogs. No episode or turn data yet.
sporc = SPORCDataset()

# Metadata-only work is immediate.
podcast = sporc.search_podcast("How I Built This")
print(podcast.title, podcast.num_episodes, "episodes")

# Touching an episode's turns fetches that podcast's turn partition once.
for episode in podcast.episodes:
    if episode.has_turn_data:
        stats = episode.get_turn_statistics()
        print(f"{episode.title}: {stats['total_turns']} turns")
```

Turns load automatically the first time you access `episode.turns`,
`episode.get_all_turns()`, or `episode.get_turn_statistics()`. You can also
trigger the load explicitly, which is useful for warming a set of episodes
before a timed loop:

```python
# Load turns for one episode up front
episode = podcast.episodes[0]
sporc.load_turns_for_episode(episode)

# ...or for every episode in a podcast
sporc.load_turns_for_podcast(podcast)
```

!!! tip "Custom cache and credentials"
    ```python
    sporc = SPORCDataset(
        cache_dir="/path/to/cache",
        use_auth_token="hf_your_token_here",  # usually unnecessary after `hf auth login`
    )
    ```

## Fetching a subset up front

When you know which podcasts (or episodes) you need, pass `subset=` to fetch
them at construction. Later access to that slice then needs no network. `subset`
accepts a list of podcast ids or titles, a dict with `podcast_ids` /
`podcast_titles` / `episode_ids` keys, or a path to a `.json` or
newline-delimited `.txt` file holding either.

```python
# By title
sporc = SPORCDataset(subset=["The NPR Politics Podcast", "Stuff You Should Know"])

# From a file of ids or titles
sporc = SPORCDataset(subset="my_podcast_ids.txt")

# By explicit spec
sporc = SPORCDataset(subset={"podcast_titles": ["The Tim Ferriss Show"]})
```

`prefetch()` does the same job after construction and reports what it resolved:

```python
sporc = SPORCDataset()
result = sporc.prefetch(["The Moth", "How I Built This"])
print(result)  # {'podcasts': 2, 'files': ..., 'unresolved': []}
```

`unresolved` lists any titles/ids that didn't match the catalog, so a typo is
reported rather than silently fetched as a 404.

### Pinning a run to a subset

Combine `subset` with `allow_downloads=False` to guarantee a run touches nothing
beyond its slice. Anything outside the subset raises `DataNotLocalError` instead
of quietly downloading more. This helps with reproducible experiments and
metered connections.

```python
from sporc import SPORCDataset

sporc = SPORCDataset(
    subset="study_podcasts.txt",
    allow_downloads=False,
)
# Works: inside the subset.
podcast = sporc.search_podcast("The NPR Politics Podcast")

# Raises DataNotLocalError: outside the subset, and downloads are forbidden.
# other = sporc.search_podcast("Some Podcast Not In The Subset")
```

!!! note "`prefetch()` still runs under `allow_downloads=False`"
    Pinning a run to a slice means fetching *that slice* and nothing else, so
    `prefetch()` (and the `subset=` argument, which calls it) is allowed to
    download even when `allow_downloads=False`. The restriction applies to
    everything *after* the subset is in place.

## Eager download

Set `lazy=False` to download the whole corpus up front. Only do this when you
genuinely need all of it and have the disk space. It's ~31 GB across hundreds
of files. Every subsequent access is then local.

```python
# Downloads the full corpus (large; excludes the optional search databases
# unless you opt in with include_search_db / include_turn_text).
sporc = SPORCDataset(lazy=False)
```

To read a copy of the layout you already have on disk, pass `parquet_dir=`, which
never downloads anything:

```python
sporc = SPORCDataset(parquet_dir="/path/to/sporc_parquet")
```

## One-pass processing over the whole corpus

For a single sequential sweep, such as collecting a statistic or filtering to a
working set, iterate rather than materializing everything. `iterate_episodes()` and
`iterate_podcasts()` build one object at a time and let you cap the work with
`max_episodes` / `max_podcasts` and `sampling_mode` (`"first"` or `"random"`).

```python
sporc = SPORCDataset()

# Process a bounded, sequential slice.
long_episodes = []
for episode in sporc.iterate_episodes(max_episodes=5000):
    if episode.duration_minutes >= 30 and episode.num_main_speakers >= 2:
        long_episodes.append(episode)
print(f"Kept {len(long_episodes)} long multi-speaker episodes")
```

!!! warning "Bound whole-corpus iteration"
    `iterate_episodes()` with no cap builds every episode in turn, one partition
    read each, which on a lazy source downloads most of the corpus. Pass
    `max_episodes=` (or `max_podcasts=`), and use `sampling_mode="random"` for a
    representative slice. The same warning applies to `get_all_episodes()` and
    `get_all_podcasts()`, which build the entire corpus eagerly.

### Collecting statistics in one pass

```python
sporc = SPORCDataset()

totals = {"episodes": 0, "duration_hours": 0.0, "categories": {}}
for episode in sporc.iterate_episodes(max_episodes=10000):
    totals["episodes"] += 1
    totals["duration_hours"] += episode.duration_hours
    for category in episode.categories:
        totals["categories"][category] = totals["categories"].get(category, 0) + 1

print(f"Episodes seen: {totals['episodes']}")
print(f"Total duration: {totals['duration_hours']:.1f} hours")
top = sorted(totals["categories"].items(), key=lambda kv: kv[1], reverse=True)[:5]
print(f"Top categories: {dict(top)}")
```

For corpus-wide summary numbers you don't need to compute yourself, use the
precomputed catalog statistics:

```python
stats = sporc.get_dataset_statistics()
print(stats["total_podcasts"], "podcasts")
print(stats["total_episodes"], "episodes")
print(f"{stats['total_duration_hours']:.0f} hours")
print("Speaker distribution:", stats["speaker_distribution"])
```

## Choosing an approach

| You want to... | Use |
|---|---|
| Explore, then analyze a few known podcasts | Default `SPORCDataset()` (lazy) |
| Repeatedly search/analyze a fixed slice | `subset=[...]` |
| Guarantee a reproducible, self-contained run | `subset=[...], allow_downloads=False` |
| Sweep the whole corpus once, sequentially | `iterate_episodes(max_episodes=...)` |
| Work fully offline from a downloaded copy | `parquet_dir=...` |
| Keep everything local for many random-access passes | `lazy=False` (needs disk + bandwidth) |

## Best practices

1. Stay lazy unless you have a reason not to. The default fetches the least
   and starts the fastest.
2. Name your slice. If you know which podcasts you need, pass `subset=`; it
   turns repeated, per-touch downloads into one up-front fetch.
3. Pin reproducible runs with `allow_downloads=False` so an experiment can't
   silently grow.
4. Cap whole-corpus iteration with `max_episodes` / `max_podcasts`, and reach for
   `get_all_episodes()` only on an already-loaded subset.
5. Gate on `episode.has_turn_data` before reading turns, since a third of
   episodes have none.
6. Opt into the big extras deliberately. `include_search_db` (~14 GB) and
   `include_turn_text` (~19 GB) are only needed for full-text search;
   `load_audio_features` pulls a separate acoustics tree.

## See also

- [Working with the data](../data-access.md): the partitioning model and how
  much each study size costs.
- [Searching the corpus](searching.md): the search API these loading modes feed.
- [Quick start](../quickstart.md): the shortest end-to-end example.
- [API reference](../reference/sporcdataset.md): full `SPORCDataset` signatures.
