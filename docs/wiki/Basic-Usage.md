# Basic Usage

This guide covers the fundamental usage patterns for the SPORC package. You'll learn how to load the dataset, search for content, and perform basic analysis.

## Getting Started

### Importing the Package

```python
from sporc import SPORCDataset, SPORCError
```

### Loading the Dataset

The first step is to initialize the SPORC dataset. The package supports three modes:

#### Memory Mode (Default)
```python
# Load all data into memory for fast access
sporc = SPORCDataset()

# With custom cache directory
sporc = SPORCDataset(cache_dir="/path/to/cache")

# With custom authentication token
sporc = SPORCDataset(use_auth_token="your_token_here")
```

#### Streaming Mode
```python
# Load data on-demand to reduce memory usage
sporc = SPORCDataset(streaming=True)

# With custom cache directory and authentication
sporc = SPORCDataset(
    streaming=True,
    cache_dir="/path/to/cache",
    use_auth_token="your_token_here"
)
```

#### Selective Mode
```python
# Load data on-demand, then filter and load specific subset into memory
sporc = SPORCDataset(streaming=True)

# Load only education podcasts
sporc.load_podcast_subset(categories=['education'])

# Load podcasts by specific hosts
sporc.load_podcast_subset(hosts=['Simon Shapiro', 'John Doe'])

# Load substantial podcasts (10+ episodes)
sporc.load_podcast_subset(min_episodes=10)

# Complex filtering
sporc.load_podcast_subset(
    categories=['education', 'science'],
    min_episodes=5,
    min_total_duration=2.0,  # 2+ hours
    language='en'
)
```

**Note**: The dataset will be downloaded automatically on first use. This may take some time depending on your internet connection.

### Choosing Between Memory, Streaming, and Selective Mode

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

## Working with Podcasts

### Finding a Podcast

Search for a specific podcast by name:

```python
# Exact match
podcast = sporc.search_podcast("SingOut SpeakOut")

# Case-insensitive search
podcast = sporc.search_podcast("singout speakout")

# Partial match
podcast = sporc.search_podcast("SingOut")
```

### Getting Podcast Information

```python
podcast = sporc.search_podcast("Example Podcast")

print(f"Title: {podcast.title}")
print(f"Description: {podcast.description}")
print(f"Number of episodes: {podcast.num_episodes}")
print(f"Total duration: {podcast.total_duration_hours:.1f} hours")
print(f"Hosts: {podcast.host_names}")
print(f"Categories: {podcast.categories}")
```

### Iterating Through Episodes

```python
podcast = sporc.search_podcast("Example Podcast")

for episode in podcast.episodes:
    print(f"Episode: {episode.title}")
    print(f"Duration: {episode.duration_minutes:.1f} minutes")
    print(f"Date: {episode.episode_date}")
    print("---")
```

### Streaming Mode: Iterating Over Podcasts

In streaming mode, you can iterate over podcasts one at a time:

```python
sporc = SPORCDataset(streaming=True)

for podcast in sporc.iterate_podcasts():
    print(f"Podcast: {podcast.title}")
    print(f"Episodes: {podcast.num_episodes}")

    for episode in podcast.episodes:
        print(f"  - {episode.title}")

    # Memory is freed after processing each podcast
    print("---")
```

### Selective Mode: Loading Filtered Subsets

In selective mode, you can load specific podcast subsets and then have O(1) access:

```python
# Initialize streaming mode
sporc = SPORCDataset(streaming=True)

# Load education podcasts
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

## Working with Episodes

### Getting Episode Information

```python
episode = podcast.episodes[0]

print(f"Title: {episode.title}")
print(f"Description: {episode.description}")
print(f"Duration: {episode.duration_minutes:.1f} minutes")
print(f"Hosts: {episode.host_names}")
print(f"Guests: {episode.guest_names}")
print(f"Categories: {episode.categories}")
print(f"Main speakers: {episode.num_main_speakers}")
```

### Episode Type Classification

```python
episode = podcast.episodes[0]

print(f"Is solo episode: {episode.is_solo}")
print(f"Is interview: {episode.is_interview}")
print(f"Is panel discussion: {episode.is_panel}")
print(f"Is long-form: {episode.is_long_form}")
print(f"Has guests: {episode.has_guests}")
```

### Streaming Mode: Iterating Over Episodes

In streaming mode, you can iterate over episodes one at a time:

```python
sporc = SPORCDataset(streaming=True)

for episode in sporc.iterate_episodes():
    print(f"Episode: {episode.title}")
    print(f"Duration: {episode.duration_minutes:.1f} minutes")
    print(f"Speakers: {episode.num_main_speakers}")

    # Process episode...
    # Memory is freed after processing each episode
    print("---")
```

### Selective Mode: Fast Access to Filtered Episodes

In selective mode, you can load filtered episodes and have fast access:

```python
sporc = SPORCDataset(streaming=True)

# Load substantial episodes (30+ minutes)
sporc.load_podcast_subset(min_total_duration=5.0)  # 5+ hours per podcast

# Fast access to all loaded episodes
all_episodes = sporc.get_all_episodes()
print(f"Loaded {len(all_episodes)} substantial episodes")

# Fast search within the subset
long_episodes = sporc.search_episodes(min_duration=3600)  # 1+ hour episodes
print(f"Found {len(long_episodes)} very long episodes")
```

## Searching for Content

### Searching Episodes by Criteria

```python
# Search by duration
long_episodes = sporc.search_episodes(min_duration=1800)  # 30+ minutes
short_episodes = sporc.search_episodes(max_duration=600)  # 10 minutes or less

# Search by speaker count
solo_episodes = sporc.search_episodes(max_speakers=1)
multi_speaker_episodes = sporc.search_episodes(min_speakers=3)

# Search by host
simon_episodes = sporc.search_episodes(host_name="Simon Shapiro")

# Search by category
education_episodes = sporc.search_episodes(category="education")
music_episodes = sporc.search_episodes(category="music")

# Combined search
long_interviews = sporc.search_episodes(
    min_duration=1800,  # 30+ minutes
    min_speakers=2,     # At least 2 speakers
    category="education"
)
```

**Note**: In streaming mode, search operations require iterating through the dataset and may be slower than in memory mode. In selective mode, searches are fast O(1) operations on the loaded subset.

### Getting All Content

```python
# Get all podcasts
all_podcasts = sporc.get_all_podcasts()
print(f"Total podcasts: {len(all_podcasts)}")

# Get all episodes
all_episodes = sporc.get_all_episodes()
print(f"Total episodes: {len(all_episodes)}")
```

**Note**: In streaming mode, these operations load all data into memory, which may defeat the purpose of using streaming mode. Consider using `iterate_podcasts()` or `iterate_episodes()` instead. In selective mode, these operations are fast and only return the loaded subset.

## Selective Loading

### Overview

Selective loading allows you to filter podcasts during the initial loading phase and then have O(1) access to the selected subset. This is useful when you want to work with a specific genre, host, or other criteria without loading the entire dataset.

### Available Filtering Criteria

```python
sporc = SPORCDataset(streaming=True)

# Filter by podcast names
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

# Complex filtering
sporc.load_podcast_subset(
    categories=['education', 'science'],
    min_episodes=5,
    min_total_duration=2.0,
    language='en',
    explicit=False
)
```

### Performance Benefits

Selective loading provides the best of both worlds:

```python
# Initialize streaming mode
sporc = SPORCDataset(streaming=True)

# Load education podcasts (O(n) one-time cost)
sporc.load_podcast_subset(categories=['education'])
print(f"Loaded {len(sporc)} education episodes")

# Now all operations are O(1) on the subset
education_podcasts = sporc.get_all_podcasts()  # Fast
long_episodes = sporc.search_episodes(min_duration=1800)  # Fast
stats = sporc.get_dataset_statistics()  # Fast
```

### Use Cases

**Research on specific genres:**
```python
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(categories=['education'])
# Now you can efficiently analyze education podcasts
```

**Host analysis:**
```python
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(hosts=['Simon Shapiro'])
# Now you can analyze Simon's podcasting patterns
```

**Content analysis:**
```python
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(min_episodes=20, min_total_duration=10.0)
# Now you can analyze substantial, established podcasts
```

## Working with Conversation Turns

### Getting All Turns

```python
episode = podcast.episodes[0]
turns = episode.get_all_turns()

print(f"Total turns: {len(turns)}")

for turn in turns[:5]:  # First 5 turns
    print(f"Speaker: {turn.primary_speaker}")
    print(f"Duration: {turn.duration:.1f} seconds")
    print(f"Words: {turn.word_count}")
    print(f"Text: {turn.text[:100]}...")
    print("---")
```

### Filtering Turns by Time

```python
# Get turns from first 5 minutes
early_turns = episode.get_turns_by_time_range(0, 300)

# Get turns from last 10 minutes
late_turns = episode.get_turns_by_time_range(
    episode.duration_seconds - 600,
    episode.duration_seconds
)

# Get turns from specific time range
middle_turns = episode.get_turns_by_time_range(600, 1200)  # 10-20 minutes
```

### Filtering Turns by Speaker

```python
# Get all turns by a specific speaker
speaker_turns = episode.get_turns_by_speaker("SPEAKER_00")

# Get all turns by host
host_turns = episode.get_host_turns()

# Get all turns by guests
guest_turns = episode.get_guest_turns()
```

### Filtering Turns by Length

```python
# Get turns with at least 50 words
long_turns = episode.get_turns_by_min_length(50)

# Get turns with at least 100 words
very_long_turns = episode.get_turns_by_min_length(100)
```

## Basic Statistics

### Dataset Statistics

```python
stats = sporc.get_dataset_statistics()

print(f"Total podcasts: {stats['total_podcasts']}")
print(f"Total episodes: {stats['total_episodes']}")
print(f"Total duration: {stats['total_duration_hours']:.1f} hours")
print(f"Average episode length: {stats['avg_episode_duration_minutes']:.1f} minutes")

print("\nTop categories:")
for category, count in sorted(stats['category_distribution'].items(), key=lambda x: x[1], reverse=True)[:5]:
    print(f"  {category}: {count} episodes")
```

**Note**: In streaming mode, calculating statistics requires iterating through the entire dataset and may take longer than in memory mode. In selective mode, statistics are calculated on the loaded subset and are fast.

### Podcast Statistics

```python
podcast = sporc.search_podcast("Example Podcast")
stats = podcast.get_episode_statistics()

print(f"Number of episodes: {stats['num_episodes']}")
print(f"Total duration: {stats['total_duration_hours']:.1f} hours")
print(f"Average episode length: {stats['avg_episode_duration_minutes']:.1f} minutes")
print(f"Episode types: {stats['episode_types']}")
```

### Episode Statistics

```python
episode = podcast.episodes[0]
stats = episode.get_turn_statistics()

print(f"Total turns: {stats['total_turns']}")
print(f"Total words: {stats['total_words']}")
print(f"Average turn duration: {stats['avg_turn_duration']:.1f} seconds")
print(f"Average words per turn: {stats['avg_words_per_turn']:.1f}")
```

## Memory-Efficient Processing Patterns

### Processing in Batches

```python
sporc = SPORCDataset(streaming=True)

batch_size = 10
episode_count = 0

for episode in sporc.iterate_episodes():
    episode_count += 1

    # Process episode
    print(f"Processing episode {episode_count}: {episode.title}")

    # Print progress every batch_size episodes
    if episode_count % batch_size == 0:
        print(f"Processed {episode_count} episodes")
        # Memory is automatically freed after each episode
```

### Filtering During Iteration

```python
sporc = SPORCDataset(streaming=True)

long_episode_count = 0

for episode in sporc.iterate_episodes():
    # Filter for long episodes only
    if episode.duration_minutes >= 30:
        long_episode_count += 1
        print(f"Long episode {long_episode_count}: {episode.title}")

        # Process only long episodes
        # This saves memory by not processing short episodes
```

### Selective Loading for Efficient Processing

```python
# Load only the podcasts you need
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(categories=['education'], min_episodes=5)

# Now you can process efficiently with fast access
for episode in sporc.get_all_episodes():
    # Process episode with O(1) access
    print(f"Processing: {episode.title}")
    turns = episode.get_all_turns()
    # Process turns...
```

### Collecting Statistics During Iteration

```python
sporc = SPORCDataset(streaming=True)

category_counts = {}
total_duration = 0

for episode in sporc.iterate_episodes():
    # Update statistics
    for category in episode.categories:
        category_counts[category] = category_counts.get(category, 0) + 1

    total_duration += episode.duration_seconds

print(f"Total duration: {total_duration/3600:.1f} hours")
print(f"Category distribution: {category_counts}")
```

## Error Handling

### Basic Error Handling

```python
from sporc import SPORCDataset, SPORCError, NotFoundError

try:
    sporc = SPORCDataset()
    podcast = sporc.search_podcast("Nonexistent Podcast")
except NotFoundError as e:
    print(f"Podcast not found: {e}")
except SPORCError as e:
    print(f"SPORC error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

### Common Error Scenarios

```python
try:
    # This might fail if authentication is not set up
    sporc = SPORCDataset()
except SPORCError as e:
    if "authentication" in str(e).lower():
        print("Please set up Hugging Face authentication")
    elif "terms" in str(e).lower():
        print("Please accept the dataset terms on Hugging Face")
    else:
        print(f"Dataset error: {e}")
```

### Streaming Mode Specific Errors

```python
sporc = SPORCDataset(streaming=True)

try:
    # This will raise an error in streaming mode
    episode_count = len(sporc)
except RuntimeError as e:
    print(f"Error: {e}")
    print("Use iterate_episodes() instead of len() in streaming mode")

# Correct way to count episodes in streaming mode
episode_count = 0
for episode in sporc.iterate_episodes():
    episode_count += 1
print(f"Total episodes: {episode_count}")
```

### Selective Mode Specific Errors

```python
sporc = SPORCDataset(streaming=True)

# This will work fine
sporc.load_podcast_subset(categories=['education'])
print(f"Loaded {len(sporc)} episodes")

# This will also work fine (fast access)
episodes = sporc.get_all_episodes()
print(f"Retrieved {len(episodes)} episodes")

# But you can only access the loaded subset
try:
    podcast = sporc.search_podcast("Podcast Not In Subset")
except NotFoundError as e:
    print(f"Podcast not found in loaded subset: {e}")
```

## Complete Example

Here's a complete example that demonstrates the basic workflow:

```python
from sporc import SPORCDataset, SPORCError

def analyze_podcast(podcast_name, use_streaming=False, use_selective=False, categories=None):
    """Analyze a podcast and its episodes."""
    try:
        # Load dataset
        if use_selective:
            sporc = SPORCDataset(streaming=True)
            if categories:
                sporc.load_podcast_subset(categories=categories)
            else:
                sporc.load_podcast_subset(podcast_names=[podcast_name])
        else:
            sporc = SPORCDataset(streaming=use_streaming)

        # Find podcast
        podcast = sporc.search_podcast(podcast_name)

        print(f"Podcast: {podcast.title}")
        print(f"Episodes: {podcast.num_episodes}")
        print(f"Total duration: {podcast.total_duration_hours:.1f} hours")
        print(f"Hosts: {', '.join(podcast.host_names)}")
        print()

        # Analyze each episode
        for i, episode in enumerate(podcast.episodes[:3]):  # First 3 episodes
            print(f"Episode {i+1}: {episode.title}")
            print(f"  Duration: {episode.duration_minutes:.1f} minutes")
            print(f"  Speakers: {episode.num_main_speakers}")
            print(f"  Type: {'Solo' if episode.is_solo else 'Interview' if episode.is_interview else 'Panel'}")

            # Get turn statistics
            turns = episode.get_all_turns()
            if turns:
                print(f"  Turns: {len(turns)}")
                print(f"  Total words: {sum(turn.word_count for turn in turns)}")

            print()

    except SPORCError as e:
        print(f"Error: {e}")

# Run the analysis in different modes
analyze_podcast("SingOut SpeakOut", use_streaming=False)  # Memory mode
analyze_podcast("SingOut SpeakOut", use_streaming=True)   # Streaming mode
analyze_podcast("SingOut SpeakOut", use_selective=True, categories=['education'])  # Selective mode
```

## Next Steps

Now that you understand the basics, you can:

1. Explore [Search Examples](Search-Examples) for more advanced search techniques
2. Learn about [Conversation Analysis](Conversation-Analysis) for deeper turn analysis
3. Check out [Data Quality](Data-Quality) to understand how to assess dataset quality
4. Try the [Examples](Examples) for more complex use cases
5. Read about [Streaming Mode](Streaming-Mode) for memory-efficient processing
6. Learn about [Selective Loading](Selective-Loading) for filtered subset processing