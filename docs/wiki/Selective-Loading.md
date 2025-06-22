# Selective Loading

Selective loading is a powerful feature that allows you to filter and load specific podcast subsets into memory for O(1) operations. This provides the best balance between memory efficiency and performance.

## Overview

Selective loading works by:
1. Starting in streaming mode (low memory usage)
2. Filtering podcasts based on your criteria during the initial loading phase
3. Loading only the filtered subset into memory
4. Providing O(1) access to the selected podcasts and episodes

This approach gives you:
- **Memory efficiency**: Only loads the data you need
- **Fast access**: O(1) operations on the loaded subset
- **Flexibility**: Multiple filtering criteria
- **Performance**: Best of both streaming and memory modes

## When to Use Selective Loading

### Use Selective Loading When:

- ✅ You want to work with a specific genre, host, or category
- ✅ You have limited RAM but need fast access to a subset
- ✅ You want to perform frequent searches on a filtered dataset
- ✅ You know your filtering criteria in advance
- ✅ You want the best balance of memory efficiency and performance
- ✅ You're doing research on specific podcast types
- ✅ You need to analyze patterns within a subset

### Avoid Selective Loading When:

- ❌ You need access to the entire dataset
- ❌ You don't know your filtering criteria in advance
- ❌ You're doing exploratory analysis across all podcasts
- ❌ You have sufficient RAM for the full dataset
- ❌ You only need to process data once sequentially

## Basic Usage

### Initialization

```python
from sporc import SPORCDataset

# Initialize in streaming mode
sporc = SPORCDataset(streaming=True)

# Load filtered subset
sporc.load_podcast_subset(categories=['education'])
```

### Available Filtering Criteria

```python
sporc = SPORCDataset(streaming=True)

# Filter by podcast names (exact or partial matches)
sporc.load_podcast_subset(podcast_names=['SingOut SpeakOut', 'Brazen Education'])

# Filter by categories
sporc.load_podcast_subset(categories=['education', 'science'])

# Filter by hosts
sporc.load_podcast_subset(hosts=['Simon Shapiro', 'John Doe'])

# Filter by episode count
sporc.load_podcast_subset(min_episodes=10, max_episodes=100)

# Filter by total duration (in hours)
sporc.load_podcast_subset(min_total_duration=5.0, max_total_duration=50.0)

# Filter by language
sporc.load_podcast_subset(language='en')

# Filter by explicit content
sporc.load_podcast_subset(explicit=False)

# Complex filtering (combine multiple criteria)
sporc.load_podcast_subset(
    categories=['education', 'science'],
    min_episodes=5,
    min_total_duration=2.0,
    language='en',
    explicit=False
)

# Sampling: Limit results and control sampling mode
# Load first 100 education podcasts
sporc.load_podcast_subset(
    categories=['education'],
    max_podcasts=100,
    sampling_mode="first"
)

# Load random 50 podcasts with at least 5 episodes
sporc.load_podcast_subset(
    min_episodes=5,
    max_podcasts=50,
    sampling_mode="random"
)

# Load first 1000 episodes from education podcasts
sporc.load_podcast_subset(
    categories=['education'],
    max_episodes=1000,
    sampling_mode="first"
)
```

## Filtering Examples

### Load by Category

```python
# Load only education podcasts
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(categories=['education'])

print(f"Loaded {len(sporc)} episodes from education podcasts")

# Now you have fast access to education content
education_podcasts = sporc.get_all_podcasts()
for podcast in education_podcasts:
    print(f"Education podcast: {podcast.title}")

# Fast search within the subset
long_education_episodes = sporc.search_episodes(min_duration=1800)  # 30+ minutes
print(f"Found {len(long_education_episodes)} long education episodes")
```

### Load by Host

```python
# Load podcasts by specific hosts
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(hosts=['Simon Shapiro', 'John Doe'])

print(f"Loaded {len(sporc)} episodes from selected hosts")

# Fast access to episodes from these hosts
host_podcasts = sporc.get_all_podcasts()
for podcast in host_podcasts:
    print(f"Host podcast: {podcast.title}")
    print(f"Hosts: {podcast.host_names}")
```

### Load by Episode Count

```python
# Load podcasts with substantial episode counts
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(min_episodes=10)

print(f"Loaded {len(sporc)} episodes from established podcasts")

# Work with established podcasts
established_podcasts = sporc.get_all_podcasts()
for podcast in established_podcasts:
    print(f"Established podcast: {podcast.title} ({podcast.num_episodes} episodes)")
```

### Load by Duration

```python
# Load podcasts with substantial content
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(min_total_duration=5.0)  # 5+ hours

print(f"Loaded {len(sporc)} episodes from substantial podcasts")

# Work with substantial podcasts
substantial_podcasts = sporc.get_all_podcasts()
for podcast in substantial_podcasts:
    print(f"Substantial podcast: {podcast.title} ({podcast.total_duration_hours:.1f} hours)")
```

### Complex Filtering

```python
# Load substantial English education/science podcasts
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(
    categories=['education', 'science'],
    min_episodes=5,
    min_total_duration=2.0,  # 2+ hours
    language='en'
)

print(f"Loaded {len(sporc)} episodes from curated podcasts")

# Now you have fast access to a curated subset
curated_episodes = sporc.get_all_episodes()
curated_podcasts = sporc.get_all_podcasts()

print(f"Curated podcasts: {[p.title for p in curated_podcasts]}")
```

### Sampling with Selective Loading

Selective loading supports sampling to limit the number of results:

```python
# Load first 100 education podcasts
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(
    categories=['education'],
    max_podcasts=100,
    sampling_mode="first"
)

print(f"Loaded first {len(sporc)} education episodes")

# Load random 50 substantial podcasts
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(
    min_episodes=10,
    max_podcasts=50,
    sampling_mode="random"
)

print(f"Loaded random {len(sporc)} episodes from substantial podcasts")

# Load first 1000 episodes from science podcasts
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(
    categories=['science'],
    max_episodes=1000,
    sampling_mode="first"
)

print(f"Loaded first {len(sporc)} science episodes")
```

### Research Sampling

```python
# Get a representative sample for research
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(
    categories=['education', 'science'],
    max_podcasts=500,
    sampling_mode="random"
)

print(f"Loaded random sample of {len(sporc)} episodes for research")

# Analyze the sample
sample_stats = sporc.get_dataset_statistics()
print(f"Sample duration: {sample_stats['total_duration_hours']:.1f} hours")
print(f"Sample categories: {dict(sorted(sample_stats['category_distribution'].items(), key=lambda x: x[1], reverse=True)[:5])}")
```

### Development and Testing

```python
# Load a small subset for development
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(
    max_podcasts=10,
    sampling_mode="first"
)

print(f"Loaded {len(sporc)} episodes for development")

# Test your code on the small subset
for episode in sporc.get_all_episodes():
    print(f"Testing with: {episode.title}")
    # Your analysis code here
```

## Performance Benefits

### Comparison with Other Modes

| Operation | Memory Mode | Streaming Mode | Selective Mode |
|-----------|-------------|----------------|----------------|
| **Initial Load** | Slow (loads all) | Fast (metadata only) | Fast (metadata only) |
| **Filtering** | Instant (O(1)) | Slow (O(n)) | Medium (O(n) one-time) |
| **Search** | Fast (O(1)) | Slow (O(n)) | Fast (O(1)) |
| **Memory Usage** | High (2-4GB) | Low (50-100MB) | Medium (100MB-2GB) |
| **Multiple Searches** | Fast | Slow (repeats O(n)) | Fast |

### Performance Example

```python
import time

# Test streaming mode search
print("Streaming Mode Search (O(n) operations):")
start_time = time.time()

sporc_streaming = SPORCDataset(streaming=True)
education_episodes = sporc_streaming.search_episodes(category="education")
search_time = time.time() - start_time

print(f"Search time: {search_time:.2f} seconds")
print(f"Found {len(education_episodes)} education episodes")

# Search again (will be slow again)
start_time = time.time()
long_education_episodes = sporc_streaming.search_episodes(
    category="education",
    min_duration=1800
)
second_search_time = time.time() - start_time

print(f"Second search time: {second_search_time:.2f} seconds")

print()

# Test selective mode search
print("Selective Mode Search (O(1) operations after loading):")
start_time = time.time()

sporc_selective = SPORCDataset(streaming=True)
sporc_selective.load_podcast_subset(categories=['education'])
load_time = time.time() - start_time

print(f"Load time: {load_time:.2f} seconds")
print(f"Loaded {len(sporc_selective)} education episodes")

# Fast search within subset
start_time = time.time()
education_episodes = sporc_selective.search_episodes(category="education")
search_time = time.time() - start_time

print(f"Search time: {search_time:.4f} seconds (O(1))")

# Another fast search
start_time = time.time()
long_education_episodes = sporc_selective.search_episodes(
    category="education",
    min_duration=1800
)
second_search_time = time.time() - start_time

print(f"Second search time: {second_search_time:.4f} seconds (O(1))")

print()
print(f"Selective mode provides {search_time/second_search_time:.1f}x faster searches")
print(f"after initial loading cost of {load_time:.2f}s")
```

## Use Cases

### Research on Specific Genres

```python
# Research on education podcasts
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(
    categories=['education'],
    min_episodes=5,
    language='en'
)

print(f"Loaded {len(sporc)} episodes for education research")

# Analyze episode types
solo_episodes = sporc.search_episodes(max_speakers=1)
interview_episodes = sporc.search_episodes(min_speakers=2, max_speakers=2)
panel_episodes = sporc.search_episodes(min_speakers=3)

print(f"Solo episodes: {len(solo_episodes)}")
print(f"Interview episodes: {len(interview_episodes)}")
print(f"Panel episodes: {len(panel_episodes)}")

# Analyze duration patterns
short_episodes = sporc.search_episodes(max_duration=900)  # < 15 minutes
medium_episodes = sporc.search_episodes(min_duration=900, max_duration=2700)  # 15-45 minutes
long_episodes = sporc.search_episodes(min_duration=2700)  # > 45 minutes

print(f"Short episodes: {len(short_episodes)}")
print(f"Medium episodes: {len(medium_episodes)}")
print(f"Long episodes: {len(long_episodes)}")
```

### Host Analysis

```python
# Analyze specific hosts
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(
    hosts=['Simon Shapiro', 'John Doe', 'Jane Smith'],
    min_episodes=3
)

print(f"Loaded {len(sporc)} episodes from selected hosts")

# Analyze host-specific patterns
for podcast in sporc.get_all_podcasts():
    print(f"Host: {podcast.host_names}")
    print(f"Podcast: {podcast.title}")
    print(f"Episodes: {podcast.num_episodes}")
    print(f"Total duration: {podcast.total_duration_hours:.1f} hours")
    print(f"Average episode length: {podcast.avg_episode_duration_minutes:.1f} minutes")
    print()
```

### Content Analysis

```python
# Analyze substantial content
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(
    min_total_duration=10.0,  # 10+ hours of content
    min_episodes=20
)

print(f"Loaded {len(sporc)} episodes from substantial podcasts")

# Analyze content patterns
stats = sporc.get_dataset_statistics()
print(f"Total duration: {stats['total_duration_hours']:.1f} hours")
print(f"Average episode length: {stats['avg_episode_duration_minutes']:.1f} minutes")
print(f"Top categories: {dict(sorted(stats['category_distribution'].items(), key=lambda x: x[1], reverse=True)[:5])}")

# Analyze episode types
episode_types = stats['episode_types']
print(f"Episode types: {episode_types}")
```

### Conversation Analysis

```python
# Load education podcasts for conversation analysis
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(
    categories=['education'],
    min_episodes=5
)

print(f"Loaded {len(sporc)} episodes for conversation analysis")

# Analyze conversation patterns
total_turns = 0
total_words = 0
turn_durations = []

for episode in sporc.get_all_episodes():
    turns = episode.get_all_turns()
    total_turns += len(turns)

    for turn in turns:
        total_words += turn.word_count
        turn_durations.append(turn.duration)

print(f"Total turns: {total_turns}")
print(f"Total words: {total_words}")
print(f"Average turn duration: {sum(turn_durations)/len(turn_durations):.1f} seconds")
print(f"Average words per turn: {total_words/total_turns:.1f}")
```

## Best Practices

### Choose Appropriate Filtering Criteria

```python
# Good: Specific and focused
sporc.load_podcast_subset(
    categories=['education'],
    min_episodes=5,
    language='en'
)

# Avoid: Too broad (defeats purpose of selective loading)
sporc.load_podcast_subset(categories=['education', 'science', 'technology', 'business', 'health'])

# Avoid: Too narrow (might not find any podcasts)
sporc.load_podcast_subset(
    categories=['very_specific_category'],
    hosts=['very_specific_host'],
    min_episodes=100
)
```

### Monitor Memory Usage

```python
import psutil
import os

def get_memory_usage():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

print(f"Initial memory: {get_memory_usage():.1f} MB")

sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(categories=['education'])

print(f"After loading: {get_memory_usage():.1f} MB")
print(f"Loaded {len(sporc)} episodes")
```

### Use Multiple Subsets for Different Analyses

```python
# Load different subsets for different analyses
sporc_education = SPORCDataset(streaming=True)
sporc_education.load_podcast_subset(categories=['education'])

sporc_science = SPORCDataset(streaming=True)
sporc_science.load_podcast_subset(categories=['science'])

sporc_hosts = SPORCDataset(streaming=True)
sporc_hosts.load_podcast_subset(hosts=['Simon Shapiro'])

# Compare different subsets
print(f"Education episodes: {len(sporc_education)}")
print(f"Science episodes: {len(sporc_science)}")
print(f"Simon's episodes: {len(sporc_hosts)}")
```

### Combine with Other Analysis Techniques

```python
# Load subset for detailed analysis
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(
    categories=['education'],
    min_episodes=5
)

# Use fast access for detailed analysis
for episode in sporc.get_all_episodes():
    # Analyze conversation turns
    turns = episode.get_turns_by_time_range(0, 300)  # First 5 minutes
    host_turns = episode.get_host_turns()
    guest_turns = episode.get_guest_turns()

    # Analyze patterns
    print(f"Episode: {episode.title}")
    print(f"  Early turns: {len(turns)}")
    print(f"  Host turns: {len(host_turns)}")
    print(f"  Guest turns: {len(guest_turns)}")
```

## Limitations and Considerations

### Limitations

1. **One-time filtering**: You cannot change filtering criteria after loading
2. **Subset-only access**: You can only access podcasts in the loaded subset
3. **Initial cost**: First filtering requires O(n) iteration
4. **Memory usage**: Still requires memory proportional to subset size

### Memory Considerations

- **Episode memory**: Each episode typically uses 100-500MB of memory
- **Turn data**: Loading turn data increases memory usage
- **Subset size**: Memory usage scales with the size of your filtered subset
- **Multiple instances**: Each SPORCDataset instance uses separate memory

### Performance Considerations

- **Filtering cost**: Initial filtering is O(n) but only happens once
- **Search speed**: All searches after loading are O(1)
- **Memory vs speed**: Trade-off between memory usage and search speed
- **Cache efficiency**: Data is cached locally after first access

## Error Handling

### Common Errors

```python
sporc = SPORCDataset(streaming=True)

try:
    # Load subset
    sporc.load_podcast_subset(categories=['education'])
    print(f"Loaded {len(sporc)} episodes")

    # Fast access works
    episodes = sporc.get_all_episodes()
    print(f"Retrieved {len(episodes)} episodes")

    # But you can only access the loaded subset
    podcast = sporc.search_podcast("Podcast Not In Subset")
except NotFoundError as e:
    print(f"Podcast not found in loaded subset: {e}")

# You can load a different subset
sporc.load_podcast_subset(categories=['science'])
print(f"Now loaded {len(sporc)} science episodes")
```

### Memory Error Handling

```python
import gc

try:
    sporc = SPORCDataset(streaming=True)
    sporc.load_podcast_subset(categories=['education'])

    # Process data...

except MemoryError:
    print("Out of memory! Consider:")
    print("1. Using more specific filtering criteria")
    print("2. Processing in smaller batches")
    print("3. Using streaming mode instead")

    # Force garbage collection
    gc.collect()
```

## Complete Example

Here's a complete example demonstrating selective loading:

```python
from sporc import SPORCDataset, SPORCError
import time
import psutil
import os

def get_memory_usage():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def analyze_education_podcasts():
    """Complete example of selective loading for education podcast analysis."""

    print("=== Education Podcast Analysis ===\n")
    print(f"Initial memory: {get_memory_usage():.1f} MB")

    # Initialize and load subset
    start_time = time.time()
    sporc = SPORCDataset(streaming=True)
    sporc.load_podcast_subset(
        categories=['education'],
        min_episodes=5,
        language='en'
    )
    load_time = time.time() - start_time

    print(f"Load time: {load_time:.2f} seconds")
    print(f"Memory after loading: {get_memory_usage():.1f} MB")
    print(f"Loaded {len(sporc)} episodes from {len(sporc.get_all_podcasts())} podcasts")

    # Fast analysis on the subset
    print("\n=== Fast Analysis ===")

    # Episode type analysis
    solo_episodes = sporc.search_episodes(max_speakers=1)
    interview_episodes = sporc.search_episodes(min_speakers=2, max_speakers=2)
    panel_episodes = sporc.search_episodes(min_speakers=3)

    print(f"Solo episodes: {len(solo_episodes)}")
    print(f"Interview episodes: {len(interview_episodes)}")
    print(f"Panel episodes: {len(panel_episodes)}")

    # Duration analysis
    short_episodes = sporc.search_episodes(max_duration=900)
    medium_episodes = sporc.search_episodes(min_duration=900, max_duration=2700)
    long_episodes = sporc.search_episodes(min_duration=2700)

    print(f"Short episodes (<15min): {len(short_episodes)}")
    print(f"Medium episodes (15-45min): {len(medium_episodes)}")
    print(f"Long episodes (>45min): {len(long_episodes)}")

    # Statistics
    stats = sporc.get_dataset_statistics()
    print(f"\nTotal duration: {stats['total_duration_hours']:.1f} hours")
    print(f"Average episode length: {stats['avg_episode_duration_minutes']:.1f} minutes")

    # Podcast details
    print(f"\n=== Podcast Details ===")
    for podcast in sporc.get_all_podcasts()[:5]:  # First 5 podcasts
        print(f"Podcast: {podcast.title}")
        print(f"  Episodes: {podcast.num_episodes}")
        print(f"  Duration: {podcast.total_duration_hours:.1f} hours")
        print(f"  Hosts: {', '.join(podcast.host_names)}")
        print()

    print(f"Final memory: {get_memory_usage():.1f} MB")
    print("=== Analysis Complete ===")

if __name__ == "__main__":
    try:
        analyze_education_podcasts()
    except SPORCError as e:
        print(f"SPORC Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
```

This example demonstrates the complete workflow of selective loading, from initialization to detailed analysis, with memory monitoring and error handling.

Note: In streaming mode, `len(sporc)` returns 1,134,058 (the total number of episodes) unless a subset has been loaded, in which case it returns the size of the subset.