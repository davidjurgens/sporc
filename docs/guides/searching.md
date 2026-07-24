# Searching the corpus

SPoRC gives you several ways to find podcasts and episodes: structured metadata
filters, speaker-name lookups, and (optionally) full-text search over the turn
transcripts.

!!! note "Set-up"
    Every example assumes you've [installed the package and
    authenticated](../installation.md). The dataset loads lazily by default.
    Only the ~195 MB metadata catalogs are fetched up front, and per-podcast
    data is pulled as your searches touch it. See [Data loading &
    subsets](loading.md) for how to bound that.

```python
from sporc import SPORCDataset

sporc = SPORCDataset()
```

## Finding a podcast by name

`search_podcast` matches the exact title first, then falls back to a
substring match, so prefer the full, exact title.

```python
podcast = sporc.search_podcast("The NPR Politics Podcast")
print(podcast.title, "-", podcast.num_episodes, "episodes")
print("Primary category:", podcast.primary_category)
print("Hosts:", ", ".join(podcast.host_names))
```

To scan titles yourself (for example, every podcast whose title mentions a
word), iterate rather than materializing all 228,099 podcasts at once:

```python
# Bounded scan: look at the first 2,000 podcasts only.
matches = [
    p for p in sporc.iterate_podcasts(max_podcasts=2000)
    if "politics" in p.title.lower()
]
print(f"Found {len(matches)} podcasts with 'politics' in the title")
```

!!! warning "`search_podcast` raises if nothing matches"
    A missing podcast raises `NotFoundError` rather than returning `None`. Wrap
    lookups you're unsure about in `try` / `except NotFoundError`.

## Searching episodes by metadata

`search_episodes(max_episodes=None, sampling_mode="first", **criteria)` filters
the episode catalog. The supported criteria are:

| Criterion | Meaning |
|---|---|
| `min_duration`, `max_duration` | Episode length, in seconds |
| `min_speakers`, `max_speakers` | Number of diarized main speakers |
| `host_name` | Substring match against predicted host names |
| `guest_name` | Substring match against predicted guest names |
| `category` | Category or subcategory (substring, case-insensitive) |
| `subcategory` | Subcategory (substring, case-insensitive) |
| `language` | Language code, e.g. `"en"` |
| `podcast_name` | Substring match against the podcast title |
| `podcast_id` | Exact podcast id |
| `min_overlap_prop_duration`, `max_overlap_prop_duration` | Diarization overlap, as a proportion of duration |

!!! warning "Unknown criteria raise `TypeError`"
    Passing a criterion that isn't on this list (a typo, or an old parameter
    like `min_total_duration`) raises `TypeError` rather than silently
    returning the whole catalog. To cap the number of results, use
    `max_episodes=`, not `limit=`.

### By duration

Durations are in seconds.

```python
# Episodes longer than 30 minutes
long_episodes = sporc.search_episodes(min_duration=1800, max_episodes=200)
print(f"Found {len(long_episodes)} long episodes (capped at 200)")

# Between 10 and 30 minutes
medium_episodes = sporc.search_episodes(min_duration=600, max_duration=1800,
                                        max_episodes=200)

# Shorter than 10 minutes
short_episodes = sporc.search_episodes(max_duration=600, max_episodes=200)
```

!!! tip "Always cap broad searches"
    Building each `Episode` reads (and, on a lazy source, downloads) that
    podcast's partition. A bare `min_duration` filter matches hundreds of
    thousands of episodes across thousands of podcasts. Pass `max_episodes=` to
    bound the work, and `sampling_mode="random"` when you want a representative
    slice rather than the first N.

### By speaker count

`min_speakers` / `max_speakers` count the diarized main speakers, so they only
apply to the ~65% of episodes with turn data.

```python
# Exactly two speakers (a typical interview)
two_speaker = sporc.search_episodes(min_speakers=2, max_speakers=2,
                                     max_episodes=100)

# Three or more speakers (panels)
panels = sporc.search_episodes(min_speakers=3, max_episodes=100)

# Solo episodes
solo = sporc.search_episodes(min_speakers=1, max_speakers=1, max_episodes=100)
```

### By host or guest

`host_name` and `guest_name` take a single name string and match it as a
substring against the predicted names.

```python
# Episodes hosted by a given person
host_episodes = sporc.search_episodes(host_name="Guy Raz", max_episodes=100)

# Episodes featuring a given guest
guest_episodes = sporc.search_episodes(guest_name="Ira Glass", max_episodes=100)
```

For faster, index-backed name lookups that return lightweight dicts (and avoid
building full `Episode` objects), use the dedicated methods below in
[Searching by speaker](#searching-by-speaker).

### By category and subcategory

`category` matches against a podcast's categories and subcategories as a
case-insensitive substring, so `"Science"` also catches science subcategories.
Use `search_episodes_by_subcategory` (or the `subcategory=` criterion) when you
want to target a subcategory specifically.

```python
# All news episodes
news = sporc.search_episodes(category="News", max_episodes=200)

# Business episodes
business = sporc.search_episodes(category="Business", max_episodes=200)

# Target a subcategory directly
entrepreneurship = sporc.search_episodes_by_subcategory(
    "Entrepreneurship", max_episodes=200
)

# Podcasts (not episodes) that have episodes in a subcategory
science_podcasts = sporc.search_podcasts_by_subcategory("Astronomy")
```

See the [Categories guide](categories.md) for the full category taxonomy and
how main categories relate to subcategories.

### By language

```python
english = sporc.search_episodes(language="en", max_episodes=500)
spanish = sporc.search_episodes(language="es", max_episodes=500)
```

## Combining criteria

Criteria are combined with logical AND. Layer them to narrow a search.

```python
# Long, multi-speaker news episodes
long_multi_speaker = sporc.search_episodes(
    category="News",
    min_duration=1800,     # 30+ minutes
    min_speakers=3,        # 3+ speakers
    max_speakers=6,        # 6 or fewer
    max_episodes=100,
)

# Short solo culture episodes
short_solo = sporc.search_episodes(
    category="Society & Culture",
    max_duration=900,      # 15 minutes or less
    min_speakers=1,
    max_speakers=1,
    max_episodes=100,
)
```

### Filtering on diarization quality

`overlap_prop_duration` is the proportion of an episode's audio where speakers
overlap; lower is cleaner. Use the overlap criteria to keep only
well-separated conversations.

```python
# Episodes with clean diarization (under 5% overlap)
clean = sporc.search_episodes(
    max_overlap_prop_duration=0.05,
    max_episodes=200,
)

# Good-quality English interviews for conversation analysis
ca_ready = sporc.search_episodes(
    language="en",
    min_speakers=2,
    max_speakers=4,
    min_duration=1800,
    max_overlap_prop_duration=0.1,
    max_episodes=200,
)
```

## Searching by speaker

These methods answer from the shipped speaker/host/guest indexes without
building full `Episode` objects, so they're fast and don't trigger part-file
downloads. Each returns a list of dicts (with `episode_id`, `podcast_id`, and
name/role fields).

```python
# Episodes hosted by a person (host index only — no guest-mention noise)
hosted = sporc.search_by_host("Guy Raz", limit=50)

# Episodes where someone was diarized as a guest (a real appearance,
# not merely a mention)
appearances = sporc.search_by_guest("Malcolm Gladwell", limit=50)

# Any speaker by name, optionally restricted to a role
mentions = sporc.search_by_speaker_name("Terry Gross", role="host", limit=50)

for hit in hosted[:5]:
    print(hit["podcast_id"], hit["episode_id"], hit["name_original"])
```

Use `exact=True` on any of these to require a full case-insensitive match
instead of a substring.

!!! note "Appearance vs. mention"
    `search_by_guest` reports people who actually spoke (built from diarized
    guest labels). `search_by_speaker_name(..., role="guest")` also picks up
    people who were merely *named* in an episode. Prefer `search_by_guest` when
    you need genuine appearances.

## Full-text search

Searching the transcript text requires the full-text search database, which is
not downloaded by default. Enable it with `include_search_db=True` (this adds a
~14 GB download):

```python
sporc = SPORCDataset(include_search_db=True)

# BM25-ranked turn search
hits = sporc.search_turns("climate change", mode="fts", limit=50)
for hit in hits[:5]:
    print(hit)

# Episodes whose transcripts contain a phrase
episodes = sporc.search_episodes_by_text("supply chain", limit=50)

# Keyword-in-context concordance lines
lines = sporc.concordance("pandemic", context_words=8, limit=25)
```

`search_turns` and `search_episodes_by_text` accept `mode="fts"` (ranked),
`mode="exact"` (substring/ILIKE), or `mode="regex"`. Substring and regex modes
also read the turn-text database. Pass `include_turn_text=True` if you rely on
them heavily.

## Sampling results

`max_episodes` caps the result count; `sampling_mode` controls how the cap is
applied. `"first"` (default) takes them in catalog order, and `"random"` draws a
random sample.

```python
# First 25 long episodes (deterministic)
first_long = sporc.search_episodes(
    min_duration=1800, max_episodes=25, sampling_mode="first"
)

# A random 500-episode research sample
research_sample = sporc.search_episodes(
    max_episodes=500, sampling_mode="random"
)

# A random 100 well-separated news episodes
random_news = sporc.search_episodes(
    category="News",
    min_duration=1800,
    max_overlap_prop_duration=0.1,
    max_episodes=100,
    sampling_mode="random",
)
```

!!! tip "`random` samples before building"
    In `"random"` mode the candidate rows are shuffled *before* any `Episode` is
    built, so a random sample of 100 downloads ~100 podcasts' partitions, not
    the whole matching set.

## Analyzing results

Search returns a plain list of `Episode` objects, so ordinary Python does the
rest.

```python
results = sporc.search_episodes(
    category="Business",
    min_duration=1800,
    min_speakers=2,
    max_speakers=4,
    language="en",
    max_episodes=200,
)
print(f"Found {len(results)} matching episodes")

if results:
    durations = [ep.duration_minutes for ep in results]
    speaker_counts = [ep.num_main_speakers for ep in results]
    print(f"Average duration: {sum(durations) / len(durations):.1f} minutes")
    print(f"Average speakers: {sum(speaker_counts) / len(speaker_counts):.1f}")

    # Group by podcast
    by_podcast = {}
    for ep in results:
        by_podcast.setdefault(ep.podcast_title, []).append(ep)
    print(f"Episodes from {len(by_podcast)} different podcasts")

    # Longest first
    for ep in sorted(results, key=lambda e: e.duration_minutes, reverse=True)[:5]:
        print(f"  {ep.title}: {ep.duration_minutes:.1f} min")
```

!!! warning "Turn data is not guaranteed"
    Only ~65% of the 1,124,058 episodes (731,101) were diarized into speaker
    turns. Speaker-count and turn-level analysis only apply to those, so
    always gate on `episode.has_turn_data` before reading turns:

    ```python
    for ep in results:
        if ep.has_turn_data:
            stats = ep.get_turn_statistics()
            print(ep.title, stats["total_turns"], "turns")
    ```

## Error handling

```python
from sporc import SPORCDataset, SPORCError, NotFoundError

try:
    sporc = SPORCDataset()
    podcast = sporc.search_podcast("The Moth")
    results = sporc.search_episodes(category="Arts", min_duration=1800,
                                    max_episodes=100)
    print(f"{podcast.title}: search returned {len(results)} episodes")
except NotFoundError as e:
    print(f"Not found: {e}")
except TypeError as e:
    print(f"Invalid search criterion: {e}")
except SPORCError as e:
    print(f"SPoRC error: {e}")
```

## Tips for effective searching

1. Start simple, then add criteria one at a time.
2. Always cap broad searches with `max_episodes=`, since each result costs a
   partition read.
3. Prefer a subset when you'll search the same slice repeatedly; see
   [Data loading & subsets](loading.md).
4. Use the index-backed speaker methods (`search_by_host`, `search_by_guest`)
   for fast name lookups.
5. Add quality filters such as `max_overlap_prop_duration` for research-grade
   conversation data.
6. Gate on `has_turn_data` before any turn-level work.
7. Reach for full-text search only when you need it, since it's a large,
   opt-in download.

## See also

- [Data loading & subsets](loading.md): fetch only the slice your study needs.
- [Categories](categories.md): the category taxonomy behind `category=`.
- [Conversation analysis](conversation-analysis.md): working with turns once
  you've found your episodes.
- [API reference](../reference/sporcdataset.md): full `SPORCDataset` signatures.
