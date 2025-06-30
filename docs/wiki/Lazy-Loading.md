# Lazy Loading

The SPORC dataset supports lazy loading of turn data, allowing you to work with podcast and episode metadata first, then load turn data only when needed. This is especially useful for large datasets where loading all turn data at once would consume significant memory.

## Overview

Lazy loading provides two main benefits:

1. **Memory Efficiency**: Only load turn data for episodes you actually need
2. **Fast Startup**: Begin working with the dataset immediately without waiting for all turn data to load

## Basic Usage

### Enabling Lazy Loading

To enable lazy loading, set `load_turns_eagerly=False` when creating the dataset:

```python
from sporc import SPORCDataset

# Initialize with lazy loading
sporc = SPORCDataset(
    local_data_dir="data",
    load_turns_eagerly=False,  # Enable lazy loading
    streaming=False
)
```

### Loading Turns Explicitly

With lazy loading enabled, you need to explicitly load turns when you need them:

```python
# Get episodes (turns not loaded yet)
episodes = sporc.search_episodes(min_duration=1800)

# Load turns for a specific episode
sporc.load_turns_for_episode(episodes[0])

# Now you can access turns
print(f"Episode has {len(episodes[0])} turns")
```

## Efficient Indexed Loading

For local files, SPORC uses an efficient indexing system that maps episode URLs to file offsets in the turn data file. This allows random access to specific episodes without reading the entire turn file.

### How It Works

1. **Index Building**: When lazy loading is enabled for local files, SPORC builds an index mapping episode URLs to file offsets
2. **Random Access**: When loading turns for specific episodes, SPORC seeks to the exact file offsets and reads only the relevant data
3. **Performance**: This approach is much faster than reading the entire turn file, especially for large datasets

### Index Management

```python
# Check index status
index_status = sporc.get_index_status()
print(f"Index built: {index_status['index_built']}")
print(f"Episodes indexed: {index_status['episodes_indexed']}")

# Build index asynchronously (if not already built)
sporc.build_turn_index_async()

# Use dataset while index builds in background
episodes = sporc.search_episodes(min_duration=1800)
for episode in episodes:
    print(f"Episode: {episode.title}")
```

## Loading Methods

### Individual Episode Loading

Load turns for a single episode:

```python
episode = sporc.search_episodes(min_duration=1800)[0]
sporc.load_turns_for_episode(episode)

# Now episode has turns loaded
print(f"Episode has {len(episode)} turns")
```

### Batch Episode Loading

Load turns for multiple episodes efficiently:

```python
episodes = sporc.search_episodes(min_duration=1800)[:5]
sporc.preload_turns_for_episodes(episodes)

# All episodes now have turns loaded
for episode in episodes:
    print(f"{episode.title}: {len(episode)} turns")
```

### Podcast Loading

Load turns for all episodes in a podcast:

```python
podcast = sporc.search_podcast("Example Podcast")
sporc.load_turns_for_podcast(podcast)

# All episodes in the podcast now have turns loaded
total_turns = sum(len(episode) for episode in podcast.episodes)
print(f"Podcast has {total_turns} total turns")
```

## Memory Usage Comparison

Lazy loading provides significant memory savings:

```python
import psutil
import os

def print_memory_usage(label):
    process = psutil.Process(os.getpid())
    memory_mb = process.memory_info().rss / 1024 / 1024
    print(f"{label}: {memory_mb:.1f} MB")

# Eager loading (loads all turns)
sporc_eager = SPORCDataset(load_turns_eagerly=True)
print_memory_usage("Eager loading")

# Lazy loading (loads only metadata + index)
sporc_lazy = SPORCDataset(load_turns_eagerly=False)
print_memory_usage("Lazy loading (metadata + index)")

# Load turns for specific episodes
episodes = sporc_lazy.search_episodes(min_duration=1800)[:3]
sporc_lazy.preload_turns_for_episodes(episodes)
print_memory_usage("Lazy loading (3 episodes)")
```

## Use Cases

### Large Dataset Exploration

```python
# Initialize with lazy loading for large dataset
sporc = SPORCDataset(load_turns_eagerly=False)

# Explore metadata quickly
all_episodes = sporc.get_all_episodes()
print(f"Dataset has {len(all_episodes)} episodes")

# Filter episodes by criteria
long_episodes = [ep for ep in all_episodes if ep.duration_minutes > 60]
interview_episodes = [ep for ep in all_episodes if ep.num_guests > 0]

print(f"Long episodes: {len(long_episodes)}")
print(f"Interview episodes: {len(interview_episodes)}")

# Load turns only for episodes you want to analyze
episodes_to_analyze = long_episodes[:10]
sporc.preload_turns_for_episodes(episodes_to_analyze)

# Perform analysis
for episode in episodes_to_analyze:
    stats = episode.get_turn_statistics()
    print(f"{episode.title}: {stats['total_turns']} turns")
```

### Selective Analysis

```python
# Load dataset with lazy loading
sporc = SPORCDataset(load_turns_eagerly=False)

# Find specific episodes
target_episodes = []
for episode in sporc.get_all_episodes():
    if "interview" in episode.title.lower() and episode.duration_minutes > 45:
        target_episodes.append(episode)

print(f"Found {len(target_episodes)} target episodes")

# Load turns only for target episodes
sporc.preload_turns_for_episodes(target_episodes)

# Analyze target episodes
for episode in target_episodes:
    print(f"\n{episode.title}")
    print(f"Duration: {episode.duration_minutes:.1f} minutes")
    print(f"Turns: {len(episode)}")
    print(f"Words: {episode.get_turn_statistics()['total_words']}")
```

### Interactive Workflows

```python
# Start with lazy loading for interactive exploration
sporc = SPORCDataset(load_turns_eagerly=False)

# Explore episodes
episodes = sporc.search_episodes(min_duration=1800)
for i, episode in enumerate(episodes[:5]):
    print(f"{i+1}. {episode.title} ({episode.duration_minutes:.1f} min)")

# User selects episodes to analyze
selected_indices = [0, 2, 4]  # User selection
selected_episodes = [episodes[i] for i in selected_indices]

# Load turns for selected episodes
sporc.preload_turns_for_episodes(selected_episodes)

# Perform analysis
for episode in selected_episodes:
    print(f"\nAnalyzing: {episode.title}")
    # ... perform analysis ...
```

## Performance Considerations

### Index Building

- **First Time**: Building the index takes time but only happens once
- **Subsequent Runs**: Index is loaded from disk, making startup very fast
- **Background Building**: Use `build_turn_index_async()` to build index in background

### Loading Performance

- **Individual Loading**: Good for loading a few episodes
- **Batch Loading**: More efficient for loading multiple episodes
- **Indexed Access**: Much faster than reading entire turn file

### Memory Usage

- **Metadata Only**: Very low memory usage
- **Index**: Small memory overhead for file offset mapping
- **Loaded Turns**: Memory usage scales with number of episodes loaded

## Best Practices

1. **Use Lazy Loading for Large Datasets**: Always use lazy loading when working with large datasets
2. **Batch Loading**: Use `preload_turns_for_episodes()` for loading multiple episodes
3. **Index Management**: Let the index build automatically or use background building
4. **Memory Monitoring**: Monitor memory usage when loading many episodes
5. **Selective Loading**: Only load turns for episodes you actually need to analyze

## Limitations

- **Local Files Only**: Indexed loading is only available for local files
- **Hugging Face Datasets**: Fall back to in-memory storage for Hugging Face datasets
- **Index File**: Requires additional disk space for the index file
- **First Run**: Initial index building takes time

## Example Workflow

Here's a complete workflow for analyzing a large dataset efficiently:

```python
from sporc import SPORCDataset
import time

# 1. Initialize with lazy loading
print("Initializing dataset...")
sporc = SPORCDataset(load_turns_eagerly=False)
print("✓ Dataset initialized")

# 2. Explore metadata
print("\nExploring dataset...")
all_episodes = sporc.get_all_episodes()
print(f"Total episodes: {len(all_episodes)}")

# 3. Filter episodes
long_episodes = [ep for ep in all_episodes if ep.duration_minutes > 60]
print(f"Long episodes: {len(long_episodes)}")

# 4. Select episodes for analysis
episodes_to_analyze = long_episodes[:10]
print(f"Selected {len(episodes_to_analyze)} episodes for analysis")

# 5. Load turns efficiently
print("\nLoading turns...")
start_time = time.time()
sporc.preload_turns_for_episodes(episodes_to_analyze)
load_time = time.time() - start_time
print(f"✓ Loaded turns in {load_time:.2f} seconds")

# 6. Perform analysis
print("\nPerforming analysis...")
for episode in episodes_to_analyze:
    stats = episode.get_turn_statistics()
    print(f"  {episode.title}: {stats['total_turns']} turns, {stats['total_words']} words")

print("\n✓ Analysis complete!")
```

This approach allows you to work efficiently with large datasets while maintaining control over memory usage and loading times.