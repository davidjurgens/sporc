# Turn

The `Turn` class represents a single conversation turn in a podcast episode. It contains information about who spoke, what they said, when they spoke, and for how long.

## Overview

A `Turn` object represents a continuous segment of speech by a single speaker. It includes the spoken text, timing information, speaker identification, and inferred metadata about the speaker's role and identity.

## Creating Turn Objects

Turn objects are typically created by accessing turns from an `Episode`:

```python
from sporc import SPORCDataset

# Initialize dataset and get an episode
sporc = SPORCDataset()
episodes = sporc.search_episodes(min_duration=1800)  # 30+ minute episodes
episode = episodes[0]

# Get all turns from the episode
all_turns = episode.get_all_turns()
turn = all_turns[0]  # First turn

# Get turns by speaker
host_turns = episode.get_turns_by_speaker("SPEAKER_00")
turn = host_turns[0]  # First turn by the host

# Get turns by time range
early_turns = episode.get_turns_by_time_range(0, 300)  # First 5 minutes
turn = early_turns[0]  # First turn in the time range
```

## Properties

### Basic Information

#### `speaker: str`

The speaker label for this turn (e.g., "SPEAKER_00", "SPEAKER_01").

```python
print(f"Speaker: {turn.speaker}")
```

#### `text: str`

The spoken text in this turn.

```python
print(f"Text: {turn.text}")
print(f"Text length: {len(turn.text)} characters")
```

### Timing Information

#### `start_time: float`

The start time of the turn in seconds from the beginning of the episode.

```python
start_minutes = turn.start_time / 60
print(f"Start time: {start_minutes:.1f} minutes into the episode")
```

#### `end_time: float`

The end time of the turn in seconds from the beginning of the episode.

```python
end_minutes = turn.end_time / 60
print(f"End time: {end_minutes:.1f} minutes into the episode")
```

#### `duration: float`

The duration of the turn in seconds.

```python
print(f"Duration: {turn.duration:.1f} seconds")
print(f"Duration: {turn.duration / 60:.1f} minutes")
```

### Inferred Information

#### `inferred_role: str`

The inferred role of the speaker (e.g., "host", "guest", "co-host").

```python
print(f"Inferred role: {turn.inferred_role}")
```

#### `inferred_name: str`

The inferred name of the speaker.

```python
print(f"Inferred name: {turn.inferred_name}")
```

### Content Analysis

#### `word_count: int`

The number of words in the turn.

```python
print(f"Word count: {turn.word_count}")
```

#### `words_per_minute: float`

The speaking rate in words per minute.

```python
print(f"Speaking rate: {turn.words_per_minute:.1f} words per minute")
```

## Methods

### Content Analysis

#### `get_keywords() -> List[str]`

Extract keywords from the turn text.

```python
keywords = turn.get_keywords()
print(f"Keywords: {keywords}")
```

**Returns:**
- `List[str]`: List of extracted keywords

#### `get_sentiment() -> Dict[str, float]`

Analyze the sentiment of the turn text.

```python
sentiment = turn.get_sentiment()
print(f"Sentiment: {sentiment}")
print(f"Positive: {sentiment['positive']:.3f}")
print(f"Negative: {sentiment['negative']:.3f}")
print(f"Neutral: {sentiment['neutral']:.3f}")
```

**Returns:**
- `Dict[str, float]`: Dictionary with sentiment scores

#### `get_topics() -> List[str]`

Extract topics from the turn text.

```python
topics = turn.get_topics()
print(f"Topics: {topics}")
```

**Returns:**
- `List[str]`: List of extracted topics

### Formatting

#### `format_timestamp() -> str`

Format the turn timing as a readable timestamp.

```python
timestamp = turn.format_timestamp()
print(f"Timestamp: {timestamp}")
# Output: "12:34 - 12:45 (11 seconds)"
```

**Returns:**
- `str`: Formatted timestamp string

#### `format_duration() -> str`

Format the duration in a human-readable format.

```python
duration_str = turn.format_duration()
print(f"Duration: {duration_str}")
# Output: "2 minutes 15 seconds" or "45 seconds"
```

**Returns:**
- `str`: Formatted duration string

### Validation

#### `is_valid() -> bool`

Check if the turn data is valid and complete.

```python
if turn.is_valid():
    print("Turn data is valid")
else:
    print("Turn data is incomplete or invalid")
```

**Returns:**
- `bool`: True if turn data is valid, False otherwise

## Usage Examples

### Basic Turn Information

```python
from sporc import SPORCDataset

# Load dataset and get a turn
sporc = SPORCDataset()
episodes = sporc.search_episodes(min_duration=1800)  # 30+ minute episodes
episode = episodes[0]
turns = episode.get_all_turns()
turn = turns[0]

# Display basic information
print(f"Speaker: {turn.speaker}")
print(f"Start time: {turn.start_time:.1f} seconds")
print(f"End time: {turn.end_time:.1f} seconds")
print(f"Duration: {turn.duration:.1f} seconds")
print(f"Text: {turn.text}")
print(f"Word count: {turn.word_count}")
print(f"Speaking rate: {turn.words_per_minute:.1f} words per minute")
```

### Timing Analysis

```python
# Analyze turn timing
print(f"Turn starts at: {turn.start_time / 60:.1f} minutes")
print(f"Turn ends at: {turn.end_time / 60:.1f} minutes")
print(f"Turn duration: {turn.duration / 60:.1f} minutes")

# Format timing information
timestamp = turn.format_timestamp()
duration_str = turn.format_duration()
print(f"Timestamp: {timestamp}")
print(f"Duration: {duration_str}")

# Check if turn is in a specific time range
if 300 <= turn.start_time <= 600:  # 5-10 minutes into episode
    print("This turn occurs in the 5-10 minute range")
```

### Content Analysis

```python
# Analyze turn content
print(f"Text length: {len(turn.text)} characters")
print(f"Word count: {turn.word_count}")
print(f"Average word length: {len(turn.text.replace(' ', '')) / turn.word_count:.1f} characters")

# Check for long turns
if turn.duration > 60:  # Longer than 1 minute
    print(f"Long turn by {turn.speaker}: {turn.duration:.1f} seconds")
    print(f"Text preview: {turn.text[:100]}...")

# Check speaking rate
if turn.words_per_minute > 200:
    print(f"Fast speaker: {turn.words_per_minute:.1f} words per minute")
elif turn.words_per_minute < 100:
    print(f"Slow speaker: {turn.words_per_minute:.1f} words per minute")
else:
    print(f"Normal speaking rate: {turn.words_per_minute:.1f} words per minute")
```

### Speaker Analysis

```python
# Analyze speaker information
print(f"Speaker label: {turn.speaker}")
print(f"Inferred role: {turn.inferred_role}")
print(f"Inferred name: {turn.inferred_name}")

# Check if this is a host turn
if turn.inferred_role == "host":
    print("This is a host turn")
elif turn.inferred_role == "guest":
    print("This is a guest turn")
else:
    print(f"This is a {turn.inferred_role} turn")

# Analyze speaker patterns
if turn.speaker == "SPEAKER_00":
    print("This is the main host speaking")
elif turn.speaker == "SPEAKER_01":
    print("This is a co-host or guest speaking")
```

### Turn Comparison

```python
# Compare multiple turns
turns = episode.get_all_turns()[:5]  # First 5 turns

print("Turn Comparison:")
for i, t in enumerate(turns):
    print(f"Turn {i+1}:")
    print(f"  Speaker: {t.speaker}")
    print(f"  Duration: {t.duration:.1f} seconds")
    print(f"  Word count: {t.word_count}")
    print(f"  Speaking rate: {t.words_per_minute:.1f} wpm")
    print(f"  Text preview: {t.text[:50]}...")
    print()

# Find the longest turn
longest_turn = max(turns, key=lambda t: t.duration)
print(f"Longest turn: {longest_turn.speaker} ({longest_turn.duration:.1f} seconds)")

# Find the turn with most words
most_words_turn = max(turns, key=lambda t: t.word_count)
print(f"Most words: {most_words_turn.speaker} ({most_words_turn.word_count} words)")
```

### Conversation Flow Analysis

```python
# Analyze conversation flow
turns = episode.get_all_turns()

# Find turns that start conversations
conversation_starts = []
for i, turn in enumerate(turns):
    if i == 0 or turn.start_time - turns[i-1].end_time > 5:  # 5+ second gap
        conversation_starts.append(turn)

print(f"Conversation starts: {len(conversation_starts)}")

for turn in conversation_starts[:3]:
    print(f"Conversation start by {turn.speaker}: {turn.text[:100]}...")

# Find overlapping turns (if any)
overlapping_turns = []
for i, turn in enumerate(turns):
    if i > 0 and turn.start_time < turns[i-1].end_time:
        overlap = turns[i-1].end_time - turn.start_time
        overlapping_turns.append((turns[i-1], turn, overlap))

print(f"Overlapping turns: {len(overlapping_turns)}")
for prev_turn, curr_turn, overlap in overlapping_turns[:3]:
    print(f"Overlap: {prev_turn.speaker} -> {curr_turn.speaker} ({overlap:.1f}s)")
```

### Quality Assessment

```python
# Assess turn quality
print("Turn Quality Assessment:")

# Check if turn is valid
if turn.is_valid():
    print("✓ Turn data is valid")
else:
    print("⚠ Turn data is incomplete")

# Check for very short turns
if turn.duration < 1:
    print("⚠ Very short turn (less than 1 second)")
elif turn.duration < 5:
    print("⚠ Short turn (less than 5 seconds)")
else:
    print("✓ Reasonable turn length")

# Check for very long turns
if turn.duration > 300:  # 5+ minutes
    print("⚠ Very long turn (more than 5 minutes)")
elif turn.duration > 120:  # 2+ minutes
    print("⚠ Long turn (more than 2 minutes)")
else:
    print("✓ Reasonable turn length")

# Check speaking rate
if turn.words_per_minute > 250:
    print("⚠ Very fast speaking rate")
elif turn.words_per_minute < 50:
    print("⚠ Very slow speaking rate")
else:
    print("✓ Normal speaking rate")

# Check text quality
if len(turn.text.strip()) == 0:
    print("⚠ Empty turn text")
elif len(turn.text) < 10:
    print("⚠ Very short turn text")
else:
    print("✓ Good turn text")
```

### Advanced Analysis

```python
# Analyze turn patterns across the episode
turns = episode.get_all_turns()

# Calculate statistics
durations = [t.duration for t in turns]
word_counts = [t.word_count for t in turns]
speaking_rates = [t.words_per_minute for t in turns]

print("Episode Turn Statistics:")
print(f"Total turns: {len(turns)}")
print(f"Average duration: {sum(durations) / len(durations):.1f} seconds")
print(f"Average word count: {sum(word_counts) / len(word_counts):.1f} words")
print(f"Average speaking rate: {sum(speaking_rates) / len(speaking_rates):.1f} wpm")

# Find outliers
long_turns = [t for t in turns if t.duration > 120]  # 2+ minutes
short_turns = [t for t in turns if t.duration < 5]   # Less than 5 seconds
fast_turns = [t for t in turns if t.words_per_minute > 200]
slow_turns = [t for t in turns if t.words_per_minute < 100]

print(f"Long turns (>2min): {len(long_turns)}")
print(f"Short turns (<5s): {len(short_turns)}")
print(f"Fast turns (>200wpm): {len(fast_turns)}")
print(f"Slow turns (<100wpm): {len(slow_turns)}")

# Analyze speaker distribution
speaker_counts = {}
for turn in turns:
    speaker_counts[turn.speaker] = speaker_counts.get(turn.speaker, 0) + 1

print("Speaker Turn Distribution:")
for speaker, count in sorted(speaker_counts.items()):
    percentage = (count / len(turns)) * 100
    print(f"  {speaker}: {count} turns ({percentage:.1f}%)")
```

## Error Handling

```python
from sporc import SPORCError

try:
    # Work with turn
    if turn.is_valid():
        print(f"Turn by {turn.speaker}: {turn.text[:50]}...")
    else:
        print("Turn data is invalid")
except SPORCError as e:
    print(f"Error: {e}")
except AttributeError as e:
    print(f"Turn data incomplete: {e}")
```

## Performance Considerations

1. **Text access** loads the full text into memory
2. **Content analysis** methods may be computationally expensive
3. **Large turns** with long text may require significant memory
4. **Batch processing** of turns is more efficient than individual analysis
5. **Caching results** can improve performance for repeated analysis
6. **Use filtering** to work with relevant turns only