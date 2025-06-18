# Streaming Mode

The SPORC package supports streaming mode for memory-efficient processing of large datasets. This page explains when and how to use streaming mode effectively.

## Overview

Streaming mode loads data on-demand rather than loading the entire dataset into memory at once. This allows you to process datasets that are larger than your available RAM.

## When to Use Streaming Mode

### Use Streaming Mode When:

- **Limited RAM**: You have less than 8GB of available RAM
- **Large Datasets**: You're working with datasets larger than your available memory
- **Sequential Processing**: You're processing episodes one at a time in order
- **One-Pass Analysis**: You only need to iterate through the data once
- **Memory Constraints**: You're working on systems with strict memory limits
- **Batch Processing**: You're processing data in batches and don't need random access

### Avoid Streaming Mode When:

- **Frequent Searches**: You need to perform many search operations
- **Random Access**: You need to access episodes in random order
- **Multiple Iterations**: You need to iterate over the same data multiple times
- **Interactive Analysis**: You're doing exploratory data analysis
- **Small Datasets**: The dataset fits comfortably in your available memory

## Memory vs Streaming Mode Comparison

| Aspect | Memory Mode | Streaming Mode |
|--------|-------------|----------------|
| **Initial Load Time** | Slower (loads all data) | Fast (loads metadata only) |
| **Memory Usage** | High (2-4GB typical) | Low (50-100MB initial) |
| **Search Speed** | Fast (O(1) for indexed data) | Slow (O(n) - must iterate) |
| **Random Access** | Instant | Not available |
| **Multiple Iterations** | Yes | No (must reload) |
| **Dataset Size** | Limited by RAM | Limited by storage only |
| **Statistics** | Instant | Requires full iteration |

## Basic Usage

### Initializing Streaming Mode

```python
from sporc import SPORCDataset

# Initialize in streaming mode
sporc = SPORCDataset(streaming=True)

# With custom configuration
sporc = SPORCDataset(
    streaming=True,
    cache_dir="/path/to/cache",
    use_auth_token="your_token_here"
)
```

### Iterating Over Episodes

```python
sporc = SPORCDataset(streaming=True)

# Process episodes one at a time
for episode in sporc.iterate_episodes():
    print(f"Processing: {episode.title}")
    print(f"Duration: {episode.duration_minutes:.1f} minutes")

    # Process episode data
    turns = episode.get_all_turns()
    print(f"Number of turns: {len(turns)}")

    # Memory is automatically freed after each iteration
    print("---")
```

### Iterating Over Podcasts

```python
sporc = SPORCDataset(streaming=True)

# Process podcasts one at a time
for podcast in sporc.iterate_podcasts():
    print(f"Podcast: {podcast.title}")
    print(f"Episodes: {podcast.num_episodes}")

    for episode in podcast.episodes:
        print(f"  - {episode.title}")

    # Memory is freed after processing each podcast
    print("---")
```

## Search Operations in Streaming Mode

### Basic Search

Search operations work in streaming mode but require iterating through the dataset:

```python
sporc = SPORCDataset(streaming=True)

# Search for long episodes (slower than memory mode)
long_episodes = sporc.search_episodes(min_duration=1800)  # 30+ minutes
print(f"Found {len(long_episodes)} long episodes")

# Search for education episodes
education_episodes = sporc.search_episodes(category="education")
print(f"Found {len(education_episodes)} education episodes")
```

### Efficient Filtering During Iteration

Instead of using search, filter during iteration for better performance:

```python
sporc = SPORCDataset(streaming=True)

long_episodes = []
education_episodes = []

for episode in sporc.iterate_episodes():
    # Filter for long episodes
    if episode.duration_minutes >= 30:
        long_episodes.append(episode)

    # Filter for education episodes
    if "education" in [cat.lower() for cat in episode.categories]:
        education_episodes.append(episode)

print(f"Found {len(long_episodes)} long episodes")
print(f"Found {len(education_episodes)} education episodes")
```

## Memory-Efficient Processing Patterns

### Pattern 1: Process in Batches

```python
sporc = SPORCDataset(streaming=True)

batch_size = 10
episode_count = 0
category_counts = {}

for episode in sporc.iterate_episodes():
    episode_count += 1

    # Process episode
    for category in episode.categories:
        category_counts[category] = category_counts.get(category, 0) + 1

    # Print progress every batch_size episodes
    if episode_count % batch_size == 0:
        print(f"Processed {episode_count} episodes")
        print(f"Memory usage: {get_memory_usage():.1f} MB")
        print(f"Top categories: {dict(sorted(category_counts.items(), key=lambda x: x[1], reverse=True)[:3])}")
        print()

print(f"Final results: {episode_count} episodes processed")
```

### Pattern 2: Filter and Process

```python
sporc = SPORCDataset(streaming=True)

long_episode_count = 0
total_long_duration = 0

for episode in sporc.iterate_episodes():
    # Only process episodes that meet criteria
    if episode.duration_minutes >= 30 and episode.num_main_speakers >= 2:
        long_episode_count += 1
        total_long_duration += episode.duration_minutes

        # Process only qualifying episodes
        print(f"Long episode {long_episode_count}: {episode.title}")
        print(f"Duration: {episode.duration_minutes:.1f} minutes")
        print(f"Speakers: {episode.num_main_speakers}")
        print()

print(f"Found {long_episode_count} qualifying episodes")
print(f"Total duration: {total_long_duration:.1f} minutes")
```

### Pattern 3: Collect Statistics

```python
sporc = SPORCDataset(streaming=True)

stats = {
    'total_episodes': 0,
    'total_duration': 0,
    'category_counts': {},
    'speaker_counts': {},
    'episode_types': {'solo': 0, 'interview': 0, 'panel': 0}
}

for episode in sporc.iterate_episodes():
    stats['total_episodes'] += 1
    stats['total_duration'] += episode.duration_seconds

    # Category statistics
    for category in episode.categories:
        stats['category_counts'][category] = stats['category_counts'].get(category, 0) + 1

    # Speaker statistics
    speaker_count = episode.num_main_speakers
    stats['speaker_counts'][speaker_count] = stats['speaker_counts'].get(speaker_count, 0) + 1

    # Episode type statistics
    if episode.is_solo:
        stats['episode_types']['solo'] += 1
    elif episode.is_interview:
        stats['episode_types']['interview'] += 1
    else:
        stats['episode_types']['panel'] += 1

print(f"Total episodes: {stats['total_episodes']}")
print(f"Total duration: {stats['total_duration']/3600:.1f} hours")
print(f"Category distribution: {stats['category_counts']}")
print(f"Speaker distribution: {stats['speaker_counts']}")
print(f"Episode types: {stats['episode_types']}")
```

## Performance Monitoring

### Memory Usage Monitoring

```python
import psutil
import os

def get_memory_usage():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

sporc = SPORCDataset(streaming=True)

print(f"Initial memory: {get_memory_usage():.1f} MB")

episode_count = 0
for episode in sporc.iterate_episodes():
    episode_count += 1

    if episode_count % 10 == 0:
        print(f"Episode {episode_count}: {get_memory_usage():.1f} MB")

    # Process episode...

print(f"Final memory: {get_memory_usage():.1f} MB")
```

### Processing Speed Monitoring

```python
import time

sporc = SPORCDataset(streaming=True)

start_time = time.time()
episode_count = 0

for episode in sporc.iterate_episodes():
    episode_count += 1

    if episode_count % 100 == 0:
        elapsed = time.time() - start_time
        rate = episode_count / elapsed
        print(f"Processed {episode_count} episodes at {rate:.1f} episodes/second")

total_time = time.time() - start_time
print(f"Total time: {total_time:.1f} seconds")
print(f"Average rate: {episode_count/total_time:.1f} episodes/second")
```

## Limitations and Considerations

### Limitations

1. **No Random Access**: You cannot access episodes by index
2. **No Length**: `len(sporc)` raises a RuntimeError
3. **Single Iteration**: You cannot iterate over the same data multiple times
4. **Slower Searches**: Search operations require full iteration
5. **Statistics Cost**: Getting statistics requires full iteration

### Memory Considerations

- **Episode Memory**: Each episode typically uses 100-500MB of memory
- **Turn Data**: Loading turn data for an episode increases memory usage
- **Garbage Collection**: Memory is freed after processing each episode
- **Batch Processing**: Process in small batches to control memory usage

### Performance Considerations

- **Network**: Streaming mode may require more network requests
- **Cache**: Data is cached locally after first access
- **CPU**: Processing is CPU-bound rather than I/O-bound
- **Concurrency**: Streaming mode is not designed for concurrent access

## Error Handling

### Common Streaming Mode Errors

```python
sporc = SPORCDataset(streaming=True)

try:
    # This will raise an error
    episode_count = len(sporc)
except RuntimeError as e:
    print(f"Error: {e}")
    print("Use iterate_episodes() to count episodes")

try:
    # This will also raise an error
    episodes = sporc.get_all_episodes()
except RuntimeError as e:
    print(f"Error: {e}")
    print("Use iterate_episodes() instead")

# Correct way to count episodes
episode_count = 0
for episode in sporc.iterate_episodes():
    episode_count += 1
print(f"Total episodes: {episode_count}")
```

### Memory Error Handling

```python
import gc

sporc = SPORCDataset(streaming=True)

try:
    for episode in sporc.iterate_episodes():
        # Process episode...

        # Force garbage collection if needed
        if episode_count % 50 == 0:
            gc.collect()

except MemoryError:
    print("Out of memory! Consider processing in smaller batches")
    # Force garbage collection
    gc.collect()
```

## Best Practices

### Do's

- ✅ Use streaming mode for large datasets
- ✅ Process episodes sequentially
- ✅ Filter during iteration when possible
- ✅ Monitor memory usage
- ✅ Process in batches for large datasets
- ✅ Use garbage collection when needed

### Don'ts

- ❌ Don't use streaming mode for frequent searches
- ❌ Don't try to access episodes randomly
- ❌ Don't iterate over the same data multiple times
- ❌ Don't load all episodes into memory in streaming mode
- ❌ Don't use streaming mode for interactive analysis

## Example: Complete Streaming Workflow

```python
from sporc import SPORCDataset
import time
import psutil
import os

def streaming_analysis():
    """Complete example of streaming analysis."""

    def get_memory_usage():
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024

    print("Starting streaming analysis...")
    print(f"Initial memory: {get_memory_usage():.1f} MB")

    # Initialize streaming mode
    sporc = SPORCDataset(streaming=True)

    # Initialize statistics
    stats = {
        'total_episodes': 0,
        'total_duration': 0,
        'long_episodes': 0,
        'education_episodes': 0,
        'category_counts': {},
        'start_time': time.time()
    }

    # Process episodes
    for episode in sporc.iterate_episodes():
        stats['total_episodes'] += 1
        stats['total_duration'] += episode.duration_seconds

        # Count long episodes
        if episode.duration_minutes >= 30:
            stats['long_episodes'] += 1

        # Count education episodes
        if "education" in [cat.lower() for cat in episode.categories]:
            stats['education_episodes'] += 1

        # Category statistics
        for category in episode.categories:
            stats['category_counts'][category] = stats['category_counts'].get(category, 0) + 1

        # Progress reporting
        if stats['total_episodes'] % 100 == 0:
            elapsed = time.time() - stats['start_time']
            rate = stats['total_episodes'] / elapsed
            memory = get_memory_usage()
            print(f"Processed {stats['total_episodes']} episodes at {rate:.1f}/s, {memory:.1f}MB")

    # Final results
    total_time = time.time() - stats['start_time']
    print(f"\nAnalysis completed in {total_time:.1f} seconds")
    print(f"Final memory usage: {get_memory_usage():.1f} MB")
    print(f"Results:")
    print(f"  Total episodes: {stats['total_episodes']}")
    print(f"  Total duration: {stats['total_duration']/3600:.1f} hours")
    print(f"  Long episodes: {stats['long_episodes']}")
    print(f"  Education episodes: {stats['education_episodes']}")
    print(f"  Top categories: {dict(sorted(stats['category_counts'].items(), key=lambda x: x[1], reverse=True)[:5])}")

if __name__ == "__main__":
    streaming_analysis()
```

This example demonstrates a complete streaming workflow that efficiently processes the entire dataset while monitoring memory usage and performance.