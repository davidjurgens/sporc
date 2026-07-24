# Quick start

Make sure you've [installed the package and authenticated](installation.md)
first. This example loads one real podcast — [The NPR Politics
Podcast](https://www.npr.org/podcasts/510310/npr-politics-podcast), which has 41
episodes in the corpus — and reads its episodes and turns.

```python
from sporc import SPORCDataset

# Downloads the metadata catalogs (~195 MB) on first use. Episode and turn data
# is fetched per podcast, as you touch it — not all 57 GB up front.
sporc = SPORCDataset()

# search_podcast matches by exact title first, then falls back to a substring
# match — so prefer the full, exact title (or a podcast_id) to avoid surprises.
podcast = sporc.search_podcast("The NPR Politics Podcast")
print(podcast.title, podcast.num_episodes, "episodes")

for episode in podcast.episodes:
    print(f"{episode.title} — {episode.duration_minutes:.0f} min")

    # Only ~65% of episodes were diarized into speaker turns; check first.
    if episode.has_turn_data:
        for turn in episode.turns[:3]:
            print(f"  [{turn.inferred_speaker_role}] {turn.text[:60]}...")
```

## Pin the download to one podcast

The example above downloads catalogs for the whole corpus but only fetches NPR
Politics' data. To make a run fully self-contained — fetch a known slice up
front, then touch nothing else — pass `subset`:

```python
sporc = SPORCDataset(subset=["The NPR Politics Podcast"])
podcast = sporc.search_podcast("The NPR Politics Podcast")
```

Add `allow_downloads=False` to *guarantee* nothing else is fetched: anything
outside the subset raises `DataNotLocalError` instead of quietly downloading.

## What to read next

- **[Working with the data](data-access.md)** — the corpus is partitioned by
  podcast; learn to fetch only the slice your study needs (and why the first
  ~195 MB dominates small studies).
- **[Searching the corpus](guides/searching.md)** — find podcasts and episodes by
  metadata, speaker, category, or full text.
- **[Tutorials](tutorials.md)** — eight end-to-end notebooks.

!!! warning "Two facts that shape every analysis"
    - **SPoRC is a two-month snapshot** (1 May – 30 June 2020). It's not a
      longitudinal archive.
    - **`len(episode.turns) == 0` is ambiguous** — it usually means the corpus has
      no turns for that episode, not that nobody spoke. Always gate on
      `episode.has_turn_data`.
