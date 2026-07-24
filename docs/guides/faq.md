# Frequently asked questions

Common questions about the SPoRC package, with solutions to issues people hit
most often.

## Installation and setup

### How do I install the package?

Install with pip:

```bash
pip install sporc
```

Before your first load you must also:

1. Accept the dataset terms on the
   [dataset card](https://huggingface.co/datasets/blitt/SPoRC) — the dataset is
   **gated**.
2. Authenticate locally so downloads can reach the gated files:

```bash
pip install huggingface_hub
hf auth login
```

Paste a token from
[huggingface.co/settings/tokens](https://huggingface.co/settings/tokens) when
prompted. See [Installation](../installation.md) for the full walkthrough.

!!! note "Older `huggingface_hub`"
    Before the CLI was renamed, this command was `huggingface-cli login`. Both
    write the same cached token, so either works.

### I get an authentication error when loading the dataset. What should I do?

This almost always means you are not authenticated with Hugging Face:

1. Install the hub: `pip install huggingface_hub`
2. Log in: `hf auth login`
3. Paste a token from
   [huggingface.co/settings/tokens](https://huggingface.co/settings/tokens)

If you would rather pass the token in code, use `use_auth_token`:

```python
from sporc import SPORCDataset

sporc = SPORCDataset(use_auth_token="hf_...")
```

### I get a "Dataset not found" or access error. What's wrong?

You have not accepted the dataset terms yet:

1. Visit [huggingface.co/datasets/blitt/SPoRC](https://huggingface.co/datasets/blitt/SPoRC)
2. Log in to your Hugging Face account
3. Click **Agree** to accept the terms of use
4. Load the dataset again

### The first load is slow. Is that normal?

Yes. The first load downloads the metadata catalogs (~195 MB) before anything
else, and then fetches per-podcast data as you touch it. Everything is cached
locally, so subsequent access to the same data is fast. See the next question
for how to keep downloads small.

## Loading and memory

### How much data does SPoRC download?

By default `SPORCDataset()` is **lazy**: it downloads only the ~195 MB metadata
catalogs up front and fetches each podcast's partition on demand as you touch
it. Working with a handful of podcasts costs a handful of small files rather
than the full ~57 GB corpus.

```python
from sporc import SPORCDataset

# Lazy (default): metadata now, per-podcast data on demand
sporc = SPORCDataset()
```

To control it:

- **Pin a slice.** Pass `subset=` to fetch a fixed set of podcasts or episodes
  up front so later access needs no network. It accepts a list of podcast ids or
  titles, a dict with `podcast_ids` / `podcast_titles` / `episode_ids`, or a path
  to a JSON / newline-delimited file.
- **Go fully offline.** Add `allow_downloads=False` so the run never fetches
  anything outside what is already local — absent data raises `DataNotLocalError`
  instead of downloading.
- **Download everything.** Pass `lazy=False` to pull the whole corpus up front.

```python
# Pin two podcasts and forbid any further downloads
sporc = SPORCDataset(
    subset=["The NPR Politics Podcast", "Stuff You Should Know"],
    allow_downloads=False,
)
```

See [Data loading & subsets](loading.md) for the complete subset API.

### How do I search for a specific podcast?

Use `search_podcast`. It matches the exact title first, then falls back to a
substring match:

```python
from sporc import SPORCDataset

sporc = SPORCDataset()
podcast = sporc.search_podcast("The Tim Ferriss Show")
if podcast:
    print(f"Found: {podcast.title} ({len(podcast.episodes)} episodes)")
```

### How do I find episodes longer than 30 minutes?

Use `search_episodes` with `min_duration` (seconds), and bound the results:

```python
long_episodes = sporc.search_episodes(min_duration=1800, max_episodes=100)
print(f"Found {len(long_episodes)} episodes")
```

!!! tip "Bound broad searches"
    `search_episodes` builds an `Episode` for every match, and on a lazy source
    each build downloads that podcast's partition. Pass `max_episodes` (and
    `sampling_mode="random"` for an unbiased sample) so the work stays
    proportional to what you need.

### How do I get all conversation turns from an episode?

```python
episode = podcast.episodes[0]
if episode.has_turn_data:
    for turn in episode.get_all_turns():
        print(f"{turn.primary_speaker}: {turn.text[:100]}...")
```

See the [empty turns](#an-episode-has-no-turns-is-that-a-bug) question for why
the `has_turn_data` guard matters.

### How do I analyze speaker participation?

```python
# Derive the set of speakers from the turns themselves
speakers = {turn.primary_speaker for turn in episode.get_all_turns()}
for speaker in speakers:
    speaker_turns = episode.get_turns_by_speaker(speaker)
    total_time = sum(t.duration for t in speaker_turns)
    print(f"{speaker}: {total_time / 60:.1f} minutes across {len(speaker_turns)} turns")
```

## Categories

### What categories are available?

SPoRC uses the official
[Apple Podcasts categories](https://podcasters.apple.com/support/1691-apple-podcasts-categories):
19 main categories with subcategories (110 categories in total). See the
[Categories guide](categories.md) for the full hierarchy and helper functions.

### How do I search by category?

```python
# Main category
education = sporc.search_episodes(category="Education", max_episodes=200)

# Subcategory (any valid category name works)
language = sporc.search_episodes(category="Language Learning", max_episodes=200)
```

### How do I search specifically by subcategory?

```python
# Dedicated helpers
episodes = sporc.search_episodes_by_subcategory("Language Learning", max_episodes=100)
podcasts = sporc.search_podcasts_by_subcategory("Language Learning")

# ...or the subcategory parameter on the general search
episodes = sporc.search_episodes(subcategory="Self-Improvement", max_episodes=100)
```

### How do I check whether a category name is valid?

```python
from sporc import is_valid_category, is_main_category, is_subcategory

is_valid_category("Education")     # True
is_valid_category("Invalid")       # False
is_main_category("Education")      # True
is_subcategory("Language Learning")# True
```

### My category search returns nothing. What's wrong?

Category names are **case-sensitive** and must match Apple's spelling exactly.

1. Use `"Education"`, not `"education"`.
2. Verify the name with `is_valid_category("Your Category")`.
3. If a narrow subcategory is empty, try its main category.

```python
from sporc import is_valid_category

name = "Your Category"
if is_valid_category(name):
    episodes = sporc.search_episodes(category=name, max_episodes=50)
    print(f"Found {len(episodes)} episodes")
else:
    print("Invalid category name")
```

## Sampling

### How do I limit the number of results?

Use `max_episodes` with `search_episodes`:

```python
first_50 = sporc.search_episodes(min_duration=1800, max_episodes=50, sampling_mode="first")
random_100 = sporc.search_episodes(max_episodes=100, sampling_mode="random")
```

### What sampling modes are available?

Two, via `sampling_mode`:

- **`"first"`** (default): the first N matches encountered — reproducible and
  fastest.
- **`"random"`**: an unbiased random sample of N matches, better for research.

```python
sporc.search_episodes(max_episodes=25, sampling_mode="first")
sporc.search_episodes(max_episodes=25, sampling_mode="random")
```

### When should I use random vs first sampling?

- **`"first"`** for reproducible development, testing, or when performance
  matters most.
- **`"random"`** for representative samples, statistical analysis, or to avoid
  ordering bias.

### Does sampling work with other search criteria?

Yes — it composes with everything:

```python
quality_education = sporc.search_episodes(
    category="Education",
    min_duration=1800,
    max_overlap_prop_duration=0.1,
    max_episodes=200,
    sampling_mode="random",
)
```

### How do I iterate a bounded number of episodes or podcasts?

```python
for episode in sporc.iterate_episodes(max_episodes=500, sampling_mode="first"):
    print(episode.title)

for podcast in sporc.iterate_podcasts(max_podcasts=100, sampling_mode="random"):
    print(podcast.title)
```

## Data quality and turns

### An episode has no turns. Is that a bug?

Usually not — turn coverage is **partial**. Of the 1,124,058 episodes in SPoRC,
about 65% (731,101) have been diarized into speaker turns; the rest carry a
transcript but no turns. An empty `turns` list is therefore normally a gap in
the corpus rather than a fact about the episode.

Gate on `has_turn_data` before drawing conclusions from empty turns:

```python
if episode.has_turn_data:
    turns = episode.get_all_turns()
    # ...analyze turns...
else:
    print("No turn data for this episode; skipping turn analysis")
```

!!! note "Roles are often unknown too"
    Speaker diarization does not always recover who is host vs. guest — roughly
    90% of turns carry `NO_INFERRED_ROLE`. Don't assume every turn has a resolved
    role; see [Conversation analysis](conversation-analysis.md).

### What do the overlap metrics mean?

They describe diarization quality:

- `overlap_prop_duration`: fraction of episode time with multiple speakers at
  once.
- `overlap_prop_turn_count`: fraction of turns that overlap another turn.

Lower is cleaner: 0.05–0.1 is good, 0.2+ is noisy. Filter on them in a search:

```python
good = sporc.search_episodes(
    max_overlap_prop_duration=0.1,
    max_overlap_prop_turn_count=0.2,
    max_episodes=100,
)
```

### Some turns are very short. Is that normal?

Yes. Podcasts are full of short interjections ("uh-huh", "right"), quick
replies, and overlapping speech. Filter for substantial turns by word count when
you need them:

```python
long_turns = episode.get_turns_by_min_length(10)  # turns of 10+ words
```

## Troubleshooting

### I get `ImportError: No module named 'datasets'`.

Install the runtime dependencies, or reinstall the package:

```bash
pip install datasets huggingface_hub
# or
pip install --force-reinstall sporc
```

### I hit a `PermissionError` on the cache directory.

This is a filesystem permission problem, not a package bug. Try:

1. Work inside a virtual environment.
2. Install with `pip install --user sporc`.
3. Point the cache somewhere you own with `cache_dir=` (below) or `HF_HOME`.
4. As a last resort, clear the cache: `rm -rf ~/.cache/huggingface/`.

### The cached data seems corrupted. How do I reset it?

Clear the Hugging Face cache and let SPoRC re-download what it needs:

```bash
rm -rf ~/.cache/huggingface/
```

### How do I point SPoRC at a specific cache location?

Pass `cache_dir`, or set the `HF_HOME` environment variable:

```python
from sporc import SPORCDataset

sporc = SPORCDataset(cache_dir="/path/to/cache")
```

```bash
export HF_HOME="/path/to/cache"
```

If you already have the partitioned Parquet layout on disk (for example an
exported subset), point `parquet_dir` at it and nothing is ever downloaded:

```python
sporc = SPORCDataset(parquet_dir="/path/to/parquet_layout")
```

## Advanced usage

### How do I analyze conversation across many episodes efficiently?

Pin the podcasts you care about with `subset` so the run stays bounded, then
iterate — gating on turn data as you go:

```python
from sporc import SPORCDataset

sporc = SPORCDataset(subset=["Stuff You Should Know", "How I Built This"])

all_turns = []
for episode in sporc.get_all_episodes():
    if episode.has_turn_data:
        all_turns.extend(episode.get_all_turns())

print(f"Collected {len(all_turns)} turns")
```

### How do I export conversation data for other tools?

Flatten turns into a DataFrame and write CSV:

```python
import pandas as pd

rows = []
for episode in sporc.get_all_episodes():
    if not episode.has_turn_data:
        continue
    for turn in episode.get_all_turns():
        rows.append({
            "episode_title": episode.title,
            "speaker": turn.primary_speaker,
            "start_time": turn.start_time,
            "end_time": turn.end_time,
            "duration": turn.duration,
            "word_count": turn.word_count,
            "text": turn.text,
        })

pd.DataFrame(rows).to_csv("conversation_data.csv", index=False)
```

### How do I find episodes with a specific host or guest?

Use the `host_name` / `guest_name` search criteria, or the dedicated lookup
helpers:

```python
by_host = sporc.search_episodes(host_name="Josh Clark", max_episodes=50)
podcasts = sporc.get_podcasts_by_host("Josh Clark")
```

### How do I extract simple features for machine learning?

```python
def features(episode):
    turns = episode.get_all_turns()
    total_words = sum(t.word_count for t in turns)
    return {
        "turn_count": len(turns),
        "avg_turn_duration": sum(t.duration for t in turns) / len(turns),
        "num_main_speakers": episode.num_main_speakers,
        "overlap_prop_duration": episode.overlap_prop_duration,
        "total_words": total_words,
        "words_per_minute": total_words / episode.duration_minutes,
    }

rows = [features(ep) for ep in sporc.get_all_episodes() if ep.has_turn_data]
```

## Getting help

- Read the guides: [Searching](searching.md),
  [Data loading & subsets](loading.md), [Categories](categories.md),
  [Conversation analysis](conversation-analysis.md).
- Browse the [API reference](../reference/index.md).
- Search or file issues at
  [github.com/davidjurgens/sporc/issues](https://github.com/davidjurgens/sporc/issues).

Still stuck? Confirm that (1) you have accepted the dataset terms on Hugging
Face, (2) your token is valid (`hf auth login`), and (3) your setup matches the
[Installation](../installation.md) guide.

## Citation

If you use SPoRC in your research, cite the ACL 2025 paper:
[*SPoRC: The Structured Podcast Open Research Corpus*](https://aclanthology.org/2025.acl-long.1222/).
