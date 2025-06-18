# SPORCDataset

The main class for interacting with the SPORC dataset. This class provides access to podcasts, episodes, and conversation data with various loading modes and search capabilities.

## Overview

The `SPORCDataset` class is the primary interface for working with the SPORC dataset. It supports three different loading modes:

- **Memory Mode**: Loads the entire dataset into memory for fast access
- **Streaming Mode**: Loads data on-demand for memory efficiency
- **Selective Mode**: Loads a filtered subset into memory for balanced performance

## Initialization

### Basic Usage

```python
from sporc import SPORCDataset

# Memory mode (default) - loads entire dataset into memory
sporc = SPORCDataset()

# Streaming mode - loads data on-demand
sporc = SPORCDataset(streaming=True)

# Selective mode - load filtered subset into memory
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(categories=['education'])
```

### Parameters

- `streaming` (bool, optional): If `True`, uses streaming mode. Default is `False`.
- `cache_dir` (str, optional): Directory to cache the dataset. Default is Hugging Face cache directory.
- `token` (str, optional): Hugging Face token for authentication. If not provided, uses cached credentials.

## Methods

### Search Methods

#### `search_podcast(name: str) -> Podcast`

Find a podcast by name (exact match).

```python
# Find a specific podcast
podcast = sporc.search_podcast("SingOut SpeakOut")
print(f"Found podcast: {podcast.title}")
print(f"Number of episodes: {len(podcast.episodes)}")
```

**Parameters:**
- `name` (str): Exact podcast name to search for

**Returns:**
- `Podcast` object if found, `None` if not found

**Raises:**
- `SPORCError`: If search fails or multiple matches found

#### `search_episodes(**criteria) -> List[Episode]`

Search for episodes matching specific criteria.

```python
# Search by duration
long_episodes = sporc.search_episodes(min_duration=1800)  # 30+ minutes

# Search by speaker count
two_speaker_episodes = sporc.search_episodes(min_speakers=2, max_speakers=2)

# Search by host
simon_episodes = sporc.search_episodes(host_name="Simon Shapiro")

# Search by category
education_episodes = sporc.search_episodes(category="education")

# Complex search
complex_results = sporc.search_episodes(
    min_duration=600,      # 10+ minutes
    max_speakers=3,        # 3 or fewer speakers
    category="education",  # Education category
    language="en"          # English language
)
```

**Supported Criteria:**
- `min_duration` (int): Minimum episode duration in seconds
- `max_duration` (int): Maximum episode duration in seconds
- `min_speakers` (int): Minimum number of speakers
- `max_speakers` (int): Maximum number of speakers
- `host_name` (str): Host name to search for
- `guest_name` (str): Guest name to search for
- `category` (str): Podcast category
- `language` (str): Language code (e.g., "en", "es")
- `min_total_duration` (float): Minimum total podcast duration in hours
- `max_overlap_prop_duration` (float): Maximum overlap proportion by duration
- `max_overlap_prop_turn_count` (float): Maximum overlap proportion by turn count

**Returns:**
- `List[Episode]`: List of matching episodes

### Access Methods

#### `get_all_podcasts() -> List[Podcast]`

Get all podcasts in the dataset.

```python
all_podcasts = sporc.get_all_podcasts()
print(f"Total podcasts: {len(all_podcasts)}")

for podcast in all_podcasts[:5]:  # First 5 podcasts
    print(f"- {podcast.title} ({len(podcast.episodes)} episodes)")
```

**Returns:**
- `List[Podcast]`: All podcasts in the dataset

**Note:** In streaming mode, this requires iterating through the entire dataset.

#### `get_all_episodes() -> List[Episode]`

Get all episodes in the dataset.

```python
all_episodes = sporc.get_all_episodes()
print(f"Total episodes: {len(all_episodes)}")

# Get some statistics
durations = [ep.duration_seconds for ep in all_episodes]
print(f"Average duration: {sum(durations) / len(durations):.1f} seconds")
```

**Returns:**
- `List[Episode]`: All episodes in the dataset

**Note:** In streaming mode, this requires iterating through the entire dataset.

### Iteration Methods (Streaming Mode Only)

#### `iterate_podcasts() -> Iterator[Podcast]`

Iterate over podcasts without loading them all into memory.

```python
# In streaming mode
sporc = SPORCDataset(streaming=True)

for podcast in sporc.iterate_podcasts():
    print(f"Processing: {podcast.title}")
    # Process each podcast individually
    if len(podcast.episodes) > 10:
        print(f"  Has {len(podcast.episodes)} episodes")
```

**Returns:**
- `Iterator[Podcast]`: Iterator over podcasts

#### `iterate_episodes() -> Iterator[Episode]`

Iterate over episodes without loading them all into memory.

```python
# In streaming mode
sporc = SPORCDataset(streaming=True)

for episode in sporc.iterate_episodes():
    print(f"Processing: {episode.title}")
    # Process each episode individually
    if episode.duration_seconds > 3600:  # 1+ hour
        print(f"  Long episode: {episode.duration_seconds / 3600:.1f} hours")
```

**Returns:**
- `Iterator[Episode]`: Iterator over episodes

### Selective Loading Methods (Streaming Mode Only)

#### `load_podcast_subset(**criteria) -> None`

Load a filtered subset of podcasts into memory for fast access.

```python
# Initialize streaming mode
sporc = SPORCDataset(streaming=True)

# Load only education podcasts
sporc.load_podcast_subset(categories=['education'])
print(f"Loaded {len(sporc)} episodes from education podcasts")

# Now you have fast access to education content
education_podcasts = sporc.get_all_podcasts()
long_education_episodes = sporc.search_episodes(min_duration=1800)
```

**Supported Criteria:**
- `categories` (List[str]): List of podcast categories to include
- `hosts` (List[str]): List of host names to include
- `min_episodes` (int): Minimum number of episodes per podcast
- `min_total_duration` (float): Minimum total podcast duration in hours
- `language` (str): Language code to filter by
- `max_podcasts` (int): Maximum number of podcasts to load

**Note:** This method modifies the dataset object to contain only the filtered subset.

### Statistics Methods

#### `get_dataset_statistics() -> Dict[str, Any]`

Get comprehensive statistics about the dataset.

```python
stats = sporc.get_dataset_statistics()
print(f"Total podcasts: {stats['total_podcasts']}")
print(f"Total episodes: {stats['total_episodes']}")
print(f"Total duration: {stats['total_duration_hours']:.1f} hours")
print(f"Average episode duration: {stats['avg_episode_duration_minutes']:.1f} minutes")
print(f"Categories: {stats['categories']}")
```

**Returns:**
- `Dict[str, Any]`: Dictionary containing various dataset statistics

**Available Statistics:**
- `total_podcasts`: Number of podcasts
- `total_episodes`: Number of episodes
- `total_duration_hours`: Total duration in hours
- `avg_episode_duration_minutes`: Average episode duration in minutes
- `categories`: List of podcast categories
- `languages`: List of languages
- `speaker_count_distribution`: Distribution of speaker counts
- `duration_distribution`: Distribution of episode durations

## Properties

### `len(sporc) -> int`

Get the number of episodes in the dataset.

```python
print(f"Dataset contains {len(sporc)} episodes")
```

**Note:** In streaming mode, this raises a `RuntimeError` unless a subset has been loaded.

### `streaming -> bool`

Check if the dataset is in streaming mode.

```python
if sporc.streaming:
    print("Dataset is in streaming mode")
else:
    print("Dataset is in memory mode")
```

## Mode Comparison

### Memory Mode vs Streaming Mode

| Feature | Memory Mode | Streaming Mode |
|---------|-------------|----------------|
| Initial loading | Slow | Fast |
| Memory usage | High (~2-4GB) | Low (~50-100MB) |
| Search speed | Fast (O(1)) | Slow (O(n)) |
| Access speed | Instant | Variable |
| `len()` support | Yes | No |
| Multiple iterations | Yes | No |
| Statistics | Instant | Requires iteration |

### When to Use Each Mode

**Use Memory Mode when:**
- You have sufficient RAM (8GB+ recommended)
- You need fast access to multiple episodes
- You want to perform complex searches frequently
- You need to iterate over data multiple times
- You're working with smaller subsets of the dataset

**Use Streaming Mode when:**
- You have limited RAM (< 8GB)
- You're processing the entire dataset sequentially
- You only need to access a few episodes
- You're doing one-pass analysis
- You're working on systems with memory constraints

**Use Selective Mode when:**
- You want to work with a specific genre, host, or category
- You have limited RAM but need fast access to a subset
- You want to perform frequent searches on a filtered dataset
- You know your filtering criteria in advance
- You want the best balance of memory efficiency and performance

## Error Handling

The class includes comprehensive error handling:

```python
from sporc import SPORCDataset, SPORCError

try:
    sporc = SPORCDataset()
    podcast = sporc.search_podcast("Nonexistent Podcast")
    if podcast is None:
        print("Podcast not found")
except SPORCError as e:
    print(f"Error: {e}")
```

## Performance Tips

1. **Use selective loading** for focused analysis
2. **Cache results** when performing repeated searches
3. **Use streaming mode** for large-scale processing
4. **Filter early** to reduce memory usage
5. **Use appropriate data structures** for your analysis needs

## Examples

### Basic Dataset Exploration

```python
from sporc import SPORCDataset

# Load dataset
sporc = SPORCDataset()

# Get basic statistics
stats = sporc.get_dataset_statistics()
print(f"Dataset contains {stats['total_episodes']} episodes")

# Explore categories
for category in stats['categories']:
    episodes = sporc.search_episodes(category=category)
    print(f"{category}: {len(episodes)} episodes")
```

### Advanced Search and Analysis

```python
# Find long episodes with multiple speakers
complex_episodes = sporc.search_episodes(
    min_duration=3600,    # 1+ hour
    min_speakers=3,       # 3+ speakers
    category="education"  # Education category
)

print(f"Found {len(complex_episodes)} long, multi-speaker education episodes")

# Analyze conversation patterns
for episode in complex_episodes[:5]:
    turns = episode.get_all_turns()
    long_turns = [t for t in turns if t.duration > 60]
    print(f"{episode.title}: {len(long_turns)} turns longer than 1 minute")
```

### Streaming Mode Processing

```python
# Process large dataset efficiently
sporc = SPORCDataset(streaming=True)

total_duration = 0
episode_count = 0

for episode in sporc.iterate_episodes():
    total_duration += episode.duration_seconds
    episode_count += 1

    if episode_count % 100 == 0:
        print(f"Processed {episode_count} episodes")

print(f"Total duration: {total_duration / 3600:.1f} hours")
```