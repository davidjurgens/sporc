# Episode

The `Episode` class represents a single podcast episode with its metadata, transcript, and conversation turns. It provides access to episode-level information and conversation analysis capabilities.

## Overview

An `Episode` object contains metadata about a specific episode (title, duration, hosts, etc.) and provides access to the full transcript and conversation turns. This class is typically obtained from a `Podcast` object or through search methods on the `SPORCDataset`.

## Creating Episode Objects

Episode objects are typically created by accessing episodes from a `Podcast` or through dataset search:

```python
from sporc import SPORCDataset

# Initialize dataset
sporc = SPORCDataset()

# Get episode through podcast
podcast = sporc.search_podcast("Example Podcast")
episode = podcast.episodes[0]

# Get episode through search
episodes = sporc.search_episodes(min_duration=1800)  # 30+ minute episodes
episode = episodes[0]
```

## Properties

### Basic Information

#### `title: str`

The title of the episode.

```python
print(f"Episode: {episode.title}")
```

#### `description: str`

The description of the episode.

```python
print(f"Description: {episode.description}")
```

#### `duration_seconds: float`

The duration of the episode in seconds.

```python
duration_minutes = episode.duration_seconds / 60
print(f"Duration: {duration_minutes:.1f} minutes")
```

#### `duration_minutes: float`

The duration of the episode in minutes.

```python
print(f"Duration: {episode.duration_minutes:.1f} minutes")
```

#### `duration_hours: float`

The duration of the episode in hours.

```python
print(f"Duration: {episode.duration_hours:.2f} hours")
```

### Speaker Information

#### `host_names: List[str]`

List of predicted host names for this episode.

```python
print(f"Hosts: {episode.host_names}")
```

#### `guest_names: List[str]`

List of predicted guest names for this episode.

```python
print(f"Guests: {episode.guest_names}")
```

#### `main_speakers: List[str]`

List of main speaker labels used in this episode.

```python
print(f"Main speakers: {episode.main_speakers}")
```

#### `speaker_count: int`

The number of speakers in this episode.

```python
print(f"Number of speakers: {episode.speaker_count}")
```

### Transcript Information

#### `transcript: str`

The full transcript of the episode.

```python
print(f"Transcript length: {len(episode.transcript)} characters")
print(f"First 200 characters: {episode.transcript[:200]}...")
```

#### `word_count: int`

The number of words in the transcript.

```python
print(f"Word count: {episode.word_count}")
```

### Quality Indicators

#### `overlap_prop_duration: float`

Proportion of overlapping speech by duration.

```python
print(f"Overlap proportion (duration): {episode.overlap_prop_duration:.3f}")
```

#### `overlap_prop_turn_count: float`

Proportion of overlapping speech by turn count.

```python
print(f"Overlap proportion (turns): {episode.overlap_prop_turn_count:.3f}")
```

#### `avg_turn_duration: float`

Average turn duration in seconds.

```python
print(f"Average turn duration: {episode.avg_turn_duration:.1f} seconds")
```

#### `total_speaker_labels: int`

Total number of speaker labels in the episode.

```python
print(f"Total speaker labels: {episode.total_speaker_labels}")
```

## Methods

### Turn Access

#### `get_all_turns() -> List[Turn]`

Get all conversation turns in the episode.

```python
all_turns = episode.get_all_turns()
print(f"Total turns: {len(all_turns)}")

for turn in all_turns[:5]:  # First 5 turns
    print(f"Speaker {turn.speaker}: {turn.text[:50]}...")
```

**Returns:**
- `List[Turn]`: All conversation turns in the episode

#### `get_turns_by_speaker(speaker_name: str) -> List[Turn]`

Get all turns by a specific speaker.

```python
# Get all turns by the main host
host_turns = episode.get_turns_by_speaker("SPEAKER_00")
print(f"Host turns: {len(host_turns)}")

# Calculate total speaking time for host
host_speaking_time = sum(t.duration for t in host_turns)
print(f"Host speaking time: {host_speaking_time / 60:.1f} minutes")
```

**Parameters:**
- `speaker_name` (str): Speaker label to search for

**Returns:**
- `List[Turn]`: All turns by the specified speaker

#### `get_turns_by_time_range(start_time: float, end_time: float) -> List[Turn]`

Get all turns within a specific time range.

```python
# Get turns from the first 5 minutes
early_turns = episode.get_turns_by_time_range(0, 300)
print(f"Early turns: {len(early_turns)}")

# Get turns from the last 10 minutes
late_turns = episode.get_turns_by_time_range(
    episode.duration_seconds - 600,
    episode.duration_seconds
)
print(f"Late turns: {len(late_turns)}")
```

**Parameters:**
- `start_time` (float): Start time in seconds
- `end_time` (float): End time in seconds

**Returns:**
- `List[Turn]`: All turns within the specified time range

#### `get_turns_by_min_length(min_length: int) -> List[Turn]`

Get turns longer than a specified minimum length.

```python
# Get turns longer than 30 seconds
long_turns = episode.get_turns_by_min_length(30)
print(f"Long turns: {len(long_turns)}")

# Get turns longer than 2 minutes
very_long_turns = episode.get_turns_by_min_length(120)
print(f"Very long turns: {len(very_long_turns)}")
```

**Parameters:**
- `min_length` (int): Minimum turn length in seconds

**Returns:**
- `List[Turn]`: All turns longer than the specified minimum

### Analysis Methods

#### `get_speaker_statistics() -> Dict[str, Any]`

Get statistics about speaker participation.

```python
stats = episode.get_speaker_statistics()
print(f"Total speakers: {stats['total_speakers']}")
print(f"Total turns: {stats['total_turns']}")
print(f"Average turn duration: {stats['avg_turn_duration']:.1f} seconds")

# Speaker participation
for speaker, data in stats['speaker_participation'].items():
    print(f"{speaker}: {data['turn_count']} turns, {data['total_duration']:.1f} seconds")
```

**Returns:**
- `Dict[str, Any]`: Dictionary containing speaker statistics

**Available Statistics:**
- `total_speakers`: Number of speakers
- `total_turns`: Total number of turns
- `avg_turn_duration`: Average turn duration
- `speaker_participation`: Dictionary with per-speaker statistics

#### `get_conversation_flow() -> List[Dict[str, Any]]`

Get the conversation flow with timing information.

```python
flow = episode.get_conversation_flow()
print(f"Conversation flow: {len(flow)} segments")

for segment in flow[:5]:  # First 5 segments
    print(f"Time {segment['start_time']:.1f}s: {segment['speaker']} - {segment['text'][:50]}...")
```

**Returns:**
- `List[Dict[str, Any]]`: List of conversation segments with timing

#### `get_speaker_transitions() -> List[Tuple[str, str]]`

Get the sequence of speaker transitions.

```python
transitions = episode.get_speaker_transitions()
print(f"Speaker transitions: {len(transitions)}")

# Analyze common transitions
from collections import Counter
transition_counts = Counter(transitions)
most_common = transition_counts.most_common(5)
for (speaker1, speaker2), count in most_common:
    print(f"{speaker1} -> {speaker2}: {count} times")
```

**Returns:**
- `List[Tuple[str, str]]`: List of speaker transition pairs

### Quality Assessment

#### `get_quality_metrics() -> Dict[str, float]`

Get comprehensive quality metrics for the episode.

```python
metrics = episode.get_quality_metrics()
print(f"Overlap proportion (duration): {metrics['overlap_prop_duration']:.3f}")
print(f"Overlap proportion (turns): {metrics['overlap_prop_turn_count']:.3f}")
print(f"Average turn duration: {metrics['avg_turn_duration']:.1f} seconds")
print(f"Turn count: {metrics['turn_count']}")
print(f"Word count: {metrics['word_count']}")
print(f"Words per minute: {metrics['words_per_minute']:.1f}")
```

**Returns:**
- `Dict[str, float]`: Dictionary containing quality metrics

### Container-like Behavior

The `Episode` class supports container-like behavior for accessing turns, similar to Python lists. **Note:** Turns must be loaded first using `load_turns()`.

#### `len(episode) -> int`

Get the number of turns in the episode.

```python
# Load turns first
episode.load_turns(turns_data)

# Get turn count
turn_count = len(episode)
print(f"Episode has {turn_count} turns")
```

**Returns:**
- `int`: Number of turns in the episode

**Raises:**
- `RuntimeError`: If turns are not loaded

#### `episode[index] -> Turn`

Get a turn by index (0-based).

```python
# Get first turn
first_turn = episode[0]
print(f"First turn: {first_turn.text[:50]}...")

# Get last turn
last_turn = episode[-1]
print(f"Last turn: {last_turn.text[:50]}...")

# Get specific turn
turn_5 = episode[5]
print(f"Turn 5: {turn_5.text[:50]}...")
```

**Parameters:**
- `index` (int): Turn index (supports negative indexing)

**Returns:**
- `Turn`: The turn at the specified index

**Raises:**
- `RuntimeError`: If turns are not loaded
- `IndexError`: If index is out of range

#### `for turn in episode:`

Iterate over all turns in the episode.

```python
# Iterate over all turns
for i, turn in enumerate(episode):
    print(f"Turn {i}: {turn.speaker} - {turn.text[:50]}...")

# List comprehension
long_turns = [turn for turn in episode if turn.duration > 30]
print(f"Found {len(long_turns)} turns longer than 30 seconds")

# Sum operations
total_duration = sum(turn.duration for turn in episode)
print(f"Total speaking time: {total_duration / 60:.1f} minutes")
```

**Raises:**
- `RuntimeError`: If turns are not loaded

### Container Usage Examples

```python
# Check if episode has turns
if len(episode) > 0:
    print(f"Episode has {len(episode)} turns")

    # Access first and last turns
    first_turn = episode[0]
    last_turn = episode[-1]

    print(f"First turn by {first_turn.speaker}: {first_turn.text[:50]}...")
    print(f"Last turn by {last_turn.speaker}: {last_turn.text[:50]}...")

# Iterate with index
for i, turn in enumerate(episode):
    if turn.duration > 60:  # Long turns
        print(f"Long turn {i}: {turn.speaker} ({turn.duration:.1f}s)")

# Filter turns
host_turns = [turn for turn in episode if turn.inferred_speaker_role == 'host']
guest_turns = [turn for turn in episode if turn.inferred_speaker_role == 'guest']

print(f"Host turns: {len(host_turns)}")
print(f"Guest turns: {len(guest_turns)}")

# Statistical operations
turn_durations = [turn.duration for turn in episode]
avg_duration = sum(turn_durations) / len(turn_durations)
max_duration = max(turn_durations)

print(f"Average turn duration: {avg_duration:.1f} seconds")
print(f"Longest turn: {max_duration:.1f} seconds")
```

## Usage Examples

### Basic Episode Information

```python
from sporc import SPORCDataset

# Load dataset and get an episode
sporc = SPORCDataset()
episodes = sporc.search_episodes(min_duration=1800)  # 30+ minute episodes
episode = episodes[0]

# Display basic information
print(f"Episode: {episode.title}")
print(f"Duration: {episode.duration_minutes:.1f} minutes")
print(f"Hosts: {episode.host_names}")
print(f"Guests: {episode.guest_names}")
print(f"Speakers: {episode.speaker_count}")
print(f"Word count: {episode.word_count}")
```

### Conversation Analysis

```python
# Get all conversation turns
all_turns = episode.get_all_turns()
print(f"Total conversation turns: {len(all_turns)}")

# Analyze turn lengths
turn_durations = [t.duration for t in all_turns]
avg_turn_duration = sum(turn_durations) / len(turn_durations)
print(f"Average turn duration: {avg_turn_duration:.1f} seconds")

# Find longest turns
longest_turns = sorted(all_turns, key=lambda t: t.duration, reverse=True)[:5]
for turn in longest_turns:
    print(f"Long turn by {turn.speaker}: {turn.duration:.1f} seconds")
    print(f"  Text: {turn.text[:100]}...")
```

### Speaker Analysis

```python
# Analyze each speaker's participation
for speaker in episode.main_speakers:
    speaker_turns = episode.get_turns_by_speaker(speaker)
    total_speaking_time = sum(t.duration for t in speaker_turns)
    avg_turn_length = total_speaking_time / len(speaker_turns) if speaker_turns else 0

    print(f"{speaker}:")
    print(f"  Turns: {len(speaker_turns)}")
    print(f"  Total speaking time: {total_speaking_time / 60:.1f} minutes")
    print(f"  Average turn length: {avg_turn_length:.1f} seconds")
    print(f"  Percentage of episode: {(total_speaking_time / episode.duration_seconds) * 100:.1f}%")
```

### Time-Based Analysis

```python
# Analyze conversation patterns over time
early_turns = episode.get_turns_by_time_range(0, 300)  # First 5 minutes
middle_turns = episode.get_turns_by_time_range(300, 600)  # 5-10 minutes
late_turns = episode.get_turns_by_time_range(episode.duration_seconds - 300, episode.duration_seconds)  # Last 5 minutes

print(f"Early turns: {len(early_turns)}")
print(f"Middle turns: {len(middle_turns)}")
print(f"Late turns: {len(late_turns)}")

# Compare speaking patterns
for period, turns in [("Early", early_turns), ("Middle", middle_turns), ("Late", late_turns)]:
    if turns:
        avg_duration = sum(t.duration for t in turns) / len(turns)
        print(f"{period} average turn duration: {avg_duration:.1f} seconds")
```

### Content Analysis

```python
# Analyze transcript content
print(f"Transcript length: {len(episode.transcript)} characters")
print(f"Word count: {episode.word_count}")
print(f"Words per minute: {episode.word_count / episode.duration_minutes:.1f}")

# Get long turns for content analysis
long_turns = episode.get_turns_by_min_length(60)  # 1+ minute turns
print(f"Long turns (1+ minute): {len(long_turns)}")

for turn in long_turns[:3]:  # First 3 long turns
    print(f"\nLong turn by {turn.speaker} ({turn.duration:.1f} seconds):")
    print(f"Text: {turn.text[:200]}...")
```

### Quality Assessment

```python
# Get quality metrics
metrics = episode.get_quality_metrics()
print("Quality Metrics:")
print(f"  Overlap proportion (duration): {metrics['overlap_prop_duration']:.3f}")
print(f"  Overlap proportion (turns): {metrics['overlap_prop_turn_count']:.3f}")
print(f"  Average turn duration: {metrics['avg_turn_duration']:.1f} seconds")
print(f"  Turn count: {metrics['turn_count']}")
print(f"  Word count: {metrics['word_count']}")
print(f"  Words per minute: {metrics['words_per_minute']:.1f}")

# Assess quality
if metrics['overlap_prop_duration'] < 0.1:
    print("✓ Good diarization quality (low overlap)")
else:
    print("⚠ Moderate diarization quality (some overlap)")

if metrics['avg_turn_duration'] > 10:
    print("✓ Good conversation flow (substantial turns)")
else:
    print("⚠ Short conversation turns")
```

### Conversation Flow Analysis

```python
# Get conversation flow
flow = episode.get_conversation_flow()
print(f"Conversation flow: {len(flow)} segments")

# Analyze speaker transitions
transitions = episode.get_speaker_transitions()
print(f"Speaker transitions: {len(transitions)}")

# Find most common transitions
from collections import Counter
transition_counts = Counter(transitions)
most_common = transition_counts.most_common(5)
print("Most common speaker transitions:")
for (speaker1, speaker2), count in most_common:
    print(f"  {speaker1} -> {speaker2}: {count} times")
```

### Statistical Analysis

```python
# Get speaker statistics
stats = episode.get_speaker_statistics()
print("Speaker Statistics:")
print(f"  Total speakers: {stats['total_speakers']}")
print(f"  Total turns: {stats['total_turns']}")
print(f"  Average turn duration: {stats['avg_turn_duration']:.1f} seconds")

print("\nSpeaker Participation:")
for speaker, data in stats['speaker_participation'].items():
    percentage = (data['total_duration'] / episode.duration_seconds) * 100
    print(f"  {speaker}: {data['turn_count']} turns, {data['total_duration']:.1f}s ({percentage:.1f}%)")
```

## Error Handling

```python
from sporc import SPORCError

try:
    # Work with episode
    turns = episode.get_all_turns()
    print(f"Found {len(turns)} turns")
except SPORCError as e:
    print(f"Error: {e}")
except AttributeError as e:
    print(f"Episode data incomplete: {e}")
```

## Performance Considerations

1. **Turn access** is O(n) where n is the number of turns
2. **Time-based queries** are efficient with indexed data
3. **Speaker-based queries** may require full iteration
4. **Large episodes** with many turns may require significant memory
5. **Transcript access** loads the full text into memory
6. **Use filtering** to work with subsets when appropriate