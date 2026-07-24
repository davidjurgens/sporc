# Podcast categories

SPoRC classifies every podcast with the official
[Apple Podcasts categories](https://podcasters.apple.com/support/1691-apple-podcasts-categories).
The scheme is a two-level hierarchy — a handful of **main categories**, each with
its own **subcategories** — and you can search and filter on either level.

!!! note "Categories live in the metadata catalog"
    Category assignments come from the podcast catalog that ships with the
    metadata (~195 MB), so category searches and the helper functions below work
    without downloading any episode data. Only building the matching `Episode`
    objects touches per-podcast partitions — see
    [Data loading & subsets](loading.md).

## Category structure

### Main categories

There are 19 main categories:

Arts, Business, Comedy, Education, Fiction, Government, Health & Fitness,
History, Kids & Family, Leisure, Music, News, Religion & Spirituality, Science,
Society & Culture, Sports, Technology, True Crime, TV & Film.

### Subcategories

Each main category has its own subcategories (110 categories in total). A few
representative examples:

| Main category | Subcategories |
|---|---|
| Arts | Books, Design, Fashion & Beauty, Food, Performing Arts, Visual Arts |
| Business | Careers, Entrepreneurship, Investing, Management, Marketing, Non-Profit |
| Education | Courses, How To, Language Learning, Self-Improvement |
| Health & Fitness | Alternative Health, Fitness, Medicine, Mental Health, Nutrition, Sexuality |
| News | Business News, Daily News, Entertainment News, News Commentary, Politics, Sports News, Tech News |
| Science | Astronomy, Chemistry, Earth Sciences, Life Sciences, Mathematics, Natural Sciences, Nature, Physics, Social Sciences |
| Society & Culture | Documentary, Personal Journals, Philosophy, Places & Travel, Relationships |

To enumerate the full hierarchy programmatically, use the helper functions in
[Category helper functions](#category-helper-functions) or read the exported
`APPLE_PODCAST_CATEGORIES` / `CATEGORY_HIERARCHY` mapping directly.

## Searching by category

### Basic category search

```python
from sporc import SPORCDataset

sporc = SPORCDataset()

# Search by main category. Bound the result set: a bare category search matches
# tens of thousands of episodes across thousands of podcasts, and building each
# one reads (on a lazy source, downloads) its podcast partition.
education = sporc.search_episodes(category="Education", max_episodes=200)
print(f"Found {len(education)} education episodes")

# Search by subcategory (any valid subcategory name works here too)
language_learning = sporc.search_episodes(
    category="Language Learning", max_episodes=200
)
print(f"Found {len(language_learning)} language-learning episodes")
```

!!! warning "Always pass `max_episodes` for broad searches"
    `search_episodes` matches rows in the metadata catalog, then builds an
    `Episode` for each hit. On a lazy Hub source every build is a download, so
    an unbounded category search can pull a large slice of the corpus. Pass
    `max_episodes` (and `sampling_mode="random"` for an unbiased sample) to keep
    the work proportional to what you actually need.

### Combining categories with other criteria

```python
# Long education episodes with good diarization quality
good_long_education = sporc.search_episodes(
    category="Education",
    min_duration=1800,               # 30+ minutes, in seconds
    max_overlap_prop_duration=0.1,   # low speaker overlap
    max_episodes=100,
)
print(f"Found {len(good_long_education)} episodes")
```

### Subcategory-specific searches

```python
# Dedicated subcategory search for episodes
language_episodes = sporc.search_episodes_by_subcategory(
    "Language Learning", max_episodes=100
)

# ...or for the podcasts that carry that subcategory
language_podcasts = sporc.search_podcasts_by_subcategory("Language Learning")
print(f"{len(language_podcasts)} podcasts include language-learning content")

# search_episodes_by_subcategory forwards any extra search criteria
long_multi_speaker = sporc.search_episodes_by_subcategory(
    "Language Learning",
    min_duration=1800,
    min_speakers=2,
    max_episodes=50,
)
```

## Category helper functions

SPoRC exports a set of pure helper functions and constants for working with the
category hierarchy. None of them touch the dataset, so they are cheap to call.

### Listing and looking up categories

```python
from sporc import (
    get_all_categories,
    get_main_categories,
    get_subcategories_list,
    get_subcategories,
    get_main_category,
)

get_all_categories()          # every main + subcategory name (110 total)
get_main_categories()         # the 19 main categories
get_subcategories_list()      # every subcategory across all main categories

# Subcategories of one main category
get_subcategories("Science")  # ['Astronomy', 'Chemistry', 'Earth Sciences', ...]

# The main category a subcategory belongs to
get_main_category("Astronomy")         # 'Science'
get_main_category("Language Learning") # 'Education'
```

### Validating category names

```python
from sporc import is_valid_category, is_main_category, is_subcategory

is_valid_category("Education")   # True
is_valid_category("Astronomy")   # True
is_valid_category("Invalid")     # False

is_main_category("Education")    # True
is_main_category("Astronomy")    # False

is_subcategory("Education")      # False
is_subcategory("Astronomy")      # True
```

### Subcategory utilities

```python
from sporc import (
    get_subcategories_by_main_category,
    get_subcategories_with_episodes,
    search_subcategories,
    get_popular_subcategories,
    get_subcategory_statistics,
)

# Subcategories of a main category (same as get_subcategories)
get_subcategories_by_main_category("Science")

# Sibling subcategories that share a subcategory's main category
get_subcategories_with_episodes("Astronomy")

# Fuzzy name search over subcategories
search_subcategories("tech")     # ['Tech News', ...]

# A curated list of commonly used subcategories
get_popular_subcategories()[:5]

# Summary counts for the hierarchy
stats = get_subcategory_statistics()
print(stats["total_subcategories"])
```

!!! tip "Constants are exported too"
    If you would rather work with the raw mapping, import
    `APPLE_PODCAST_CATEGORIES`, `ALL_CATEGORIES`, `MAIN_CATEGORIES`,
    `SUBCATEGORIES`, `CATEGORY_HIERARCHY`, or `SUBCATEGORY_TO_MAIN` from `sporc`.

## Categories on objects

Every `Podcast` and `Episode` exposes its own category assignments:

```python
podcast = sporc.search_podcast("Stuff You Should Know")

podcast.primary_category    # the podcast's top category
podcast.categories          # all category strings on the podcast
podcast.main_categories     # just the main categories
podcast.subcategories       # just the subcategories

episode = podcast.episodes[0]
episode.primary_category    # the episode's top category
episode.categories          # all category strings on the episode
```

!!! note "Podcast lookup matches the exact title first"
    `search_podcast` returns the podcast whose title matches exactly, falling
    back to a substring match if there is no exact hit. Use the full title —
    for example `"The NPR Politics Podcast"` or `"The Tim Ferriss Show"`.

## Category-based analysis

### Category distribution across the corpus

`get_dataset_statistics()` returns a `category_distribution` mapping category
names to episode counts.

```python
stats = sporc.get_dataset_statistics()

print("Top categories by episode count:")
for category, count in sorted(
    stats["category_distribution"].items(), key=lambda kv: kv[1], reverse=True
)[:10]:
    print(f"  {category}: {count}")
```

### Comparing categories

```python
def category_profile(category, max_episodes=200):
    """Summarize a random sample of episodes in a category."""
    episodes = sporc.search_episodes(
        category=category, max_episodes=max_episodes, sampling_mode="random"
    )
    if not episodes:
        return None
    return {
        "category": category,
        "sample_size": len(episodes),
        "avg_duration_min": sum(ep.duration_minutes for ep in episodes) / len(episodes),
        "avg_speakers": sum(ep.num_main_speakers for ep in episodes) / len(episodes),
        "avg_overlap": sum(ep.overlap_prop_duration for ep in episodes) / len(episodes),
    }

for category in ["Education", "Science", "Business", "News"]:
    profile = category_profile(category)
    if profile:
        print(
            f"{profile['category']}: {profile['sample_size']} episodes, "
            f"{profile['avg_duration_min']:.1f} min avg, "
            f"{profile['avg_speakers']:.1f} speakers, "
            f"overlap {profile['avg_overlap']:.3f}"
        )
```

!!! note "Overlap metrics are diarization-quality signals"
    `overlap_prop_duration` is the fraction of episode time with more than one
    speaker talking at once, and `overlap_prop_turn_count` is the fraction of
    overlapping turns. Lower is cleaner (0.05–0.1 is good; 0.2+ is noisy). These
    metrics only make sense for episodes with turn data — see
    [Conversation analysis](conversation-analysis.md).

## Working with a category slice

By default `SPORCDataset()` is **lazy**: it downloads the metadata catalogs up
front and fetches per-podcast partitions only as you touch them. That makes
one-off category searches cheap without any explicit subset step — just bound
them with `max_episodes`.

When you want to work with the same category slice repeatedly (or offline),
resolve the podcasts once and pin them with a `subset`:

```python
from sporc import SPORCDataset

# Find the podcasts in a subcategory, then pin them for a download-free session
scan = SPORCDataset()
podcasts = scan.search_podcasts_by_subcategory("Language Learning")
titles = [p.title for p in podcasts]

pinned = SPORCDataset(subset=titles, allow_downloads=False)
# Every search below is served from the pinned slice with no further downloads
episodes = pinned.search_episodes(category="Language Learning")
print(f"{len(episodes)} episodes in the pinned slice")
```

`allow_downloads=False` guarantees the run never fetches anything outside the
subset — a request for absent data raises instead of silently downloading. See
[Data loading & subsets](loading.md) for the full subset API.

## Best practices

- **Use exact, case-sensitive names.** `"Education"`, not `"education"`. Verify
  with `is_valid_category()` before searching.
- **Bound broad searches.** Always pass `max_episodes` for category searches,
  and `sampling_mode="random"` when you want a representative sample.
- **Search the right level.** A main category is broad; a subcategory is
  targeted. Both work with `search_episodes(category=...)`.
- **Pin recurring work.** Resolve a slice once and pass it as `subset` with
  `allow_downloads=False` for reproducible, offline analysis.

## See also

- [Searching the corpus](searching.md)
- [Data loading & subsets](loading.md)
- [Conversation analysis](conversation-analysis.md)
- [API reference](../reference/index.md)
