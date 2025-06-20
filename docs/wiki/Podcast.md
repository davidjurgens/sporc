# Podcast

The `Podcast` class represents a podcast with its episodes and metadata. It provides access to podcast-level information and all associated episodes.

## Overview

A `Podcast` object contains metadata about a podcast (title, description, category, etc.) and provides access to all episodes belonging to that podcast. This class is typically obtained from the `SPORCDataset` class through search methods.

## Creating Podcast Objects

Podcast objects are typically created by the `SPORCDataset` class:

```python
from sporc import SPORCDataset

# Initialize dataset
sporc = SPORCDataset()

# Get a podcast by name
podcast = sporc.search_podcast("SingOut SpeakOut")

# Get all podcasts
all_podcasts = sporc.get_all_podcasts()
podcast = all_podcasts[0]
```

## Properties

### Basic Information

#### `title: str`

The title of the podcast.

```python
print(f"Podcast: {podcast.title}")
```

#### `description: str`

The description of the podcast.

```python
print(f"Description: {podcast.description}")
```

#### `category: str`

The primary category of the podcast.

```python
print(f"Category: {podcast.category}")
```

#### `language: str`

The language of the podcast (e.g., "en", "es").

```python
print(f"Language: {podcast.language}")
```

### Episode Information

#### `episodes: List[Episode]`

List of all episodes belonging to this podcast.

```python
print(f"Number of episodes: {len(podcast.episodes)}")

for episode in podcast.episodes:
    print(f"- {episode.title} ({episode.duration_seconds / 60:.1f} minutes)")
```

#### `episode_count: int`

The number of episodes in this podcast.

```python
print(f"This podcast has {podcast.episode_count} episodes")
```

### Speaker Information

#### `host_names: List[str]`

List of predicted host names for this podcast.

```python
print(f"Hosts: {podcast.host_names}")
```

#### `main_speakers: List[str]`

List of main speaker labels used in this podcast.

```python
print(f"Main speakers: {podcast.main_speakers}")
```

### Duration Information

#### `total_duration_seconds: float`

Total duration of all episodes in seconds.

```python
total_hours = podcast.total_duration_seconds / 3600
print(f"Total content: {total_hours:.1f} hours")
```

#### `total_duration_hours: float`

Total duration of all episodes in hours.

```python
print(f"Total content: {podcast.total_duration_hours:.1f} hours")
```

#### `average_episode_duration_minutes: float`

Average duration of episodes in minutes.

```python
print(f"Average episode length: {podcast.average_episode_duration_minutes:.1f} minutes")
```

### Quality Indicators

#### `overlap_prop_duration: float`

Proportion of overlapping speech by duration across all episodes.

```python
print(f"Overlap proportion (duration): {podcast.overlap_prop_duration:.3f}")
```

#### `overlap_prop_turn_count: float`

Proportion of overlapping speech by turn count across all episodes.

```python
print(f"Overlap proportion (turns): {podcast.overlap_prop_turn_count:.3f}")
```

#### `avg_turn_duration: float`

Average turn duration across all episodes.

```python
print(f"Average turn duration: {podcast.avg_turn_duration:.1f} seconds")
```

## Methods

### Episode Access

#### `get_episode_by_title(title: str) -> Optional[Episode]`

Find a specific episode by title.

```python
episode = podcast.get_episode_by_title("Episode 1: Introduction")
if episode:
    print(f"Found episode: {episode.title}")
else:
    print("Episode not found")
```

**Parameters:**
- `title` (str): Exact episode title to search for

**Returns:**
- `Episode` object if found, `None` if not found

#### `get_episodes_by_duration(min_duration: float = None, max_duration: float = None) -> List[Episode]`

Get episodes within a specific duration range.

```python
# Episodes longer than 30 minutes
long_episodes = podcast.get_episodes_by_duration(min_duration=1800)

# Episodes between 10-30 minutes
medium_episodes = podcast.get_episodes_by_duration(min_duration=600, max_duration=1800)

# Episodes shorter than 10 minutes
short_episodes = podcast.get_episodes_by_duration(max_duration=600)
```

**Parameters:**
- `min_duration` (float, optional): Minimum duration in seconds
- `max_duration` (float, optional): Maximum duration in seconds

**Returns:**
- `List[Episode]`: Episodes matching the duration criteria

#### `get_episodes_by_speaker_count(min_speakers: int = None, max_speakers: int = None) -> List[Episode]`

Get episodes with a specific number of speakers.

```python
# Episodes with exactly 2 speakers
two_speaker_episodes = podcast.get_episodes_by_speaker_count(min_speakers=2, max_speakers=2)

# Episodes with 3 or more speakers
multi_speaker_episodes = podcast.get_episodes_by_speaker_count(min_speakers=3)
```

**Parameters:**
- `min_speakers` (int, optional): Minimum number of speakers
- `max_speakers` (int, optional): Maximum number of speakers

**Returns:**
- `List[Episode]`: Episodes matching the speaker count criteria

### Statistics

#### `get_statistics() -> Dict[str, Any]`

Get comprehensive statistics about this podcast.

```python
stats = podcast.get_statistics()
print(f"Total episodes: {stats['episode_count']}")
print(f"Total duration: {stats['total_duration_hours']:.1f} hours")
print(f"Average episode length: {stats['avg_episode_duration_minutes']:.1f} minutes")
print(f"Hosts: {stats['host_names']}")
print(f"Categories: {stats['categories']}")
```

**Returns:**
- `Dict[str, Any]`: Dictionary containing podcast statistics

**Available Statistics:**
- `episode_count`: Number of episodes
- `total_duration_seconds`: Total duration in seconds
- `total_duration_hours`: Total duration in hours
- `avg_episode_duration_minutes`: Average episode duration in minutes
- `host_names`: List of host names
- `categories`: List of categories
- `languages`: List of languages
- `speaker_count_distribution`: Distribution of speaker counts
- `duration_distribution`: Distribution of episode durations

### Conversation Analysis

#### `get_all_turns() -> List[Turn]`

Get all conversation turns from all episodes.

```python
all_turns = podcast.get_all_turns()
print(f"Total turns: {len(all_turns)}")

# Analyze turn lengths
long_turns = [t for t in all_turns if t.duration > 60]
print(f"Turns longer than 1 minute: {len(long_turns)}")
```

**Returns:**
- `List[Turn]`: All conversation turns from all episodes

#### `get_turns_by_speaker(speaker_name: str) -> List[Turn]`

Get all turns by a specific speaker across all episodes.

```python
# Get all turns by the main host
host_turns = podcast.get_turns_by_speaker("SPEAKER_00")
print(f"Host turns: {len(host_turns)}")

# Calculate total speaking time for host
host_speaking_time = sum(t.duration for t in host_turns)
print(f"Host speaking time: {host_speaking_time / 3600:.1f} hours")
```

**Parameters:**
- `speaker_name` (str): Speaker label to search for

**Returns:**
- `List[Turn]`: All turns by the specified speaker

#### `get_turns_by_time_range(start_time: float, end_time: float) -> List[Turn]`

Get all turns within a specific time range across all episodes.

```python
# Get all turns from the first 5 minutes of each episode
early_turns = podcast.get_turns_by_time_range(0, 300)
print(f"Early turns: {len(early_turns)}")
```

**Parameters:**
- `start_time` (float): Start time in seconds
- `end_time` (float): End time in seconds

**Returns:**
- `List[Turn]`: All turns within the specified time range

### Container-like Behavior

The `Podcast` class supports container-like behavior for accessing episodes, similar to Python lists. Episodes are accessed in chronological order.

#### `len(podcast) -> int`

Get the number of episodes in the podcast.

```python
episode_count = len(podcast)
print(f"Podcast has {episode_count} episodes")
```

**Returns:**
- `int`: Number of episodes in the podcast

#### `podcast[index] -> Episode`

Get an episode by index (0-based, chronological order).

```python
# Get first episode
first_episode = podcast[0]
print(f"First episode: {first_episode.title}")

# Get last episode
last_episode = podcast[-1]
print(f"Last episode: {last_episode.title}")

# Get specific episode
episode_5 = podcast[5]
print(f"Episode 5: {episode_5.title}")
```

**Parameters:**
- `index` (int): Episode index (supports negative indexing)

**Returns:**
- `Episode`: The episode at the specified index

**Raises:**
- `IndexError`: If index is out of range

#### `for episode in podcast:`

Iterate over all episodes in chronological order.

```python
# Iterate over all episodes
for i, episode in enumerate(podcast):
    print(f"Episode {i}: {episode.title} ({episode.duration_minutes:.1f} min)")

# List comprehension
long_episodes = [ep for ep in podcast if ep.duration_minutes > 60]
print(f"Found {len(long_episodes)} episodes longer than 1 hour")

# Sum operations
total_duration = sum(ep.duration_seconds for ep in podcast)
print(f"Total podcast duration: {total_duration / 3600:.1f} hours")
```

### Container Usage Examples

```python
# Check if podcast has episodes
if len(podcast) > 0:
    print(f"Podcast has {len(podcast)} episodes")

    # Access first and last episodes
    first_episode = podcast[0]
    last_episode = podcast[-1]

    print(f"First episode: {first_episode.title}")
    print(f"Last episode: {last_episode.title}")

# Iterate with index
for i, episode in enumerate(podcast):
    if episode.duration_minutes > 60:  # Long episodes
        print(f"Long episode {i}: {episode.title} ({episode.duration_minutes:.1f} min)")

# Filter episodes
interview_episodes = [ep for ep in podcast if ep.is_interview]
solo_episodes = [ep for ep in podcast if ep.is_solo]

print(f"Interview episodes: {len(interview_episodes)}")
print(f"Solo episodes: {len(solo_episodes)}")

# Statistical operations
episode_durations = [ep.duration_minutes for ep in podcast]
avg_duration = sum(episode_durations) / len(episode_durations)
max_duration = max(episode_durations)

print(f"Average episode duration: {avg_duration:.1f} minutes")
print(f"Longest episode: {max_duration:.1f} minutes")

# Find episodes by criteria
recent_episodes = [ep for ep in podcast if ep.episode_date and ep.episode_date.year >= 2023]
print(f"Recent episodes (2023+): {len(recent_episodes)}")

# Access specific episodes
if len(podcast) >= 3:
    third_episode = podcast[2]
    print(f"Third episode: {third_episode.title}")

    # Get episodes from the middle
    mid_point = len(podcast) // 2
    middle_episode = podcast[mid_point]
    print(f"Middle episode: {middle_episode.title}")
```

## Usage Examples

### Basic Podcast Information

```python
from sporc import SPORCDataset

# Load dataset and get a podcast
sporc = SPORCDataset()
podcast = sporc.search_podcast("Example Podcast")

# Display basic information
print(f"Podcast: {podcast.title}")
print(f"Category: {podcast.category}")
print(f"Language: {podcast.language}")
print(f"Description: {podcast.description}")
print(f"Hosts: {podcast.host_names}")
print(f"Episodes: {podcast.episode_count}")
print(f"Total duration: {podcast.total_duration_hours:.1f} hours")
```

### Episode Analysis

```python
# Get episodes by different criteria
long_episodes = podcast.get_episodes_by_duration(min_duration=1800)  # 30+ minutes
two_speaker_episodes = podcast.get_episodes_by_speaker_count(min_speakers=2, max_speakers=2)

print(f"Long episodes: {len(long_episodes)}")
print(f"Two-speaker episodes: {len(two_speaker_episodes)}")

# Analyze episode distribution
for episode in podcast.episodes:
    print(f"{episode.title}: {episode.duration_seconds / 60:.1f} min, {len(episode.main_speakers)} speakers")
```

### Conversation Analysis

```python
# Get all conversation turns
all_turns = podcast.get_all_turns()
print(f"Total conversation turns: {len(all_turns)}")

# Analyze turn lengths
turn_durations = [t.duration for t in all_turns]
avg_turn_duration = sum(turn_durations) / len(turn_durations)
print(f"Average turn duration: {avg_turn_duration:.1f} seconds")

# Find longest turns
longest_turns = sorted(all_turns, key=lambda t: t.duration, reverse=True)[:5]
for turn in longest_turns:
    print(f"Long turn by {turn.speaker}: {turn.duration:.1f} seconds")
```

### Speaker Analysis

```python
# Analyze speaker participation
for speaker in podcast.main_speakers:
    speaker_turns = podcast.get_turns_by_speaker(speaker)
    total_speaking_time = sum(t.duration for t in speaker_turns)
    print(f"{speaker}: {len(speaker_turns)} turns, {total_speaking_time / 60:.1f} minutes")
```

### Quality Assessment

```python
# Check podcast quality indicators
print(f"Overlap proportion (duration): {podcast.overlap_prop_duration:.3f}")
print(f"Overlap proportion (turns): {podcast.overlap_prop_turn_count:.3f}")
print(f"Average turn duration: {podcast.avg_turn_duration:.1f} seconds")

# Filter for high-quality episodes
good_quality_episodes = [
    ep for ep in podcast.episodes
    if ep.overlap_prop_duration < 0.1 and ep.overlap_prop_turn_count < 0.2
]
print(f"High-quality episodes: {len(good_quality_episodes)}")
```

### Statistics and Reporting

```python
# Get comprehensive statistics
stats = podcast.get_statistics()
print("Podcast Statistics:")
print(f"  Episodes: {stats['episode_count']}")
print(f"  Total duration: {stats['total_duration_hours']:.1f} hours")
print(f"  Average episode length: {stats['avg_episode_duration_minutes']:.1f} minutes")
print(f"  Hosts: {stats['host_names']}")
print(f"  Categories: {stats['categories']}")
print(f"  Languages: {stats['languages']}")

# Duration distribution
duration_dist = stats['duration_distribution']
print("Duration Distribution:")
for duration_range, count in duration_dist.items():
    print(f"  {duration_range}: {count} episodes")
```

## Error Handling

```python
from sporc import SPORCError

try:
    podcast = sporc.search_podcast("Nonexistent Podcast")
    if podcast is None:
        print("Podcast not found")
    else:
        # Work with the podcast
        print(f"Found: {podcast.title}")
except SPORCError as e:
    print(f"Error: {e}")
```

## Performance Considerations

1. **Episode access** is O(1) for indexed episodes
2. **Turn analysis** requires iterating through all episodes
3. **Statistics calculation** may be cached for better performance
4. **Large podcasts** with many episodes may require significant memory
5. **Use filtering** to work with subsets when appropriate