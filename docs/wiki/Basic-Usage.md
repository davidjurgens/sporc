# Basic Usage

This guide covers the fundamental usage patterns for the SPORC package. You'll learn how to load the dataset, search for content, and perform basic analysis.

## Getting Started

### Importing the Package

```python
from sporc import SPORCDataset, SPORCError
```

### Loading the Dataset

The first step is to initialize the SPORC dataset:

```python
# Basic initialization
sporc = SPORCDataset()

# With custom cache directory
sporc = SPORCDataset(cache_dir="/path/to/cache")

# With custom authentication token
sporc = SPORCDataset(use_auth_token="your_token_here")
```

**Note**: The dataset will be downloaded automatically on first use. This may take some time depending on your internet connection.

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

### Getting All Content

```python
# Get all podcasts
all_podcasts = sporc.get_all_podcasts()
print(f"Total podcasts: {len(all_podcasts)}")

# Get all episodes
all_episodes = sporc.get_all_episodes()
print(f"Total episodes: {len(all_episodes)}")
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

## Complete Example

Here's a complete example that demonstrates the basic workflow:

```python
from sporc import SPORCDataset, SPORCError

def analyze_podcast(podcast_name):
    """Analyze a podcast and its episodes."""
    try:
        # Load dataset
        sporc = SPORCDataset()

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

# Run the analysis
analyze_podcast("SingOut SpeakOut")
```

## Next Steps

Now that you understand the basics, you can:

1. Explore [Search Examples](Search-Examples) for more advanced search techniques
2. Learn about [Conversation Analysis](Conversation-Analysis) for deeper turn analysis
3. Check out [Data Quality](Data-Quality) to understand how to assess dataset quality
4. Try the [Examples](Examples) for more complex use cases