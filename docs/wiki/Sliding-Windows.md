# Sliding Windows

The SPORC package provides powerful sliding window functionality for analyzing podcast conversations in manageable chunks with configurable overlap. This feature is essential for processing large episodes, maintaining conversation context, and performing temporal analysis.

## Overview

Sliding windows allow you to process conversation turns in fixed-size chunks while maintaining overlap between consecutive windows. This approach is particularly useful for:

- **Large Episode Processing**: Break down long episodes into manageable segments
- **Context Preservation**: Maintain conversation context across window boundaries
- **Temporal Analysis**: Analyze conversation patterns over specific time periods
- **Batch Processing**: Process conversations in chunks for memory efficiency
- **Speaker Interaction Analysis**: Study how speakers interact within conversation segments

## Core Concepts

### TurnWindow Class

The `TurnWindow` class represents a window of conversation turns with rich metadata:

```python
from sporc.episode import TurnWindow

# A TurnWindow contains:
# - turns: List of Turn objects in this window
# - window_index: Position of this window (0-based)
# - start_index/end_index: Turn indices in the episode
# - total_windows: Total number of windows
# - overlap_size: Number of overlapping turns with previous window
```

### Window Properties

Each window provides comprehensive information:

```python
window = episode.sliding_window(window_size=10, overlap=2).__next__()

# Basic properties
print(f"Size: {window.size} turns")
print(f"Time range: {window.time_range[0]:.1f}s - {window.time_range[1]:.1f}s")
print(f"Duration: {window.duration:.1f}s")

# Position information
print(f"Is first: {window.is_first}")
print(f"Is last: {window.is_last}")
print(f"Has overlap: {window.has_overlap}")

# Turn analysis
print(f"New turns: {len(window.new_turns)}")
print(f"Overlap turns: {len(window.overlap_turns)}")
```

## Basic Usage

### Turn-Based Sliding Windows

Create windows based on the number of turns:

```python
from sporc import SPORCDataset

# Load dataset with lazy loading
dataset = SPORCDataset(load_turns_eagerly=False)

# Get a podcast and load turns
podcast = dataset.get_podcast_by_title("The Joe Rogan Experience")
episode = podcast.episodes[0]
dataset.load_turns_for_episode(episode)

# Create sliding windows with 10 turns each and 2 turns overlap
for window in episode.sliding_window(window_size=10, overlap=2):
    print(f"Window {window.window_index + 1}/{window.total_windows}")
    print(f"  Turns: {window.start_index}-{window.end_index} ({window.size} turns)")
    print(f"  Time range: {window.time_range[0]:.1f}s - {window.time_range[1]:.1f}s")
    print(f"  New turns: {len(window.new_turns)}")
    print(f"  Overlap turns: {len(window.overlap_turns)}")

    # Get combined text for this window
    text = window.get_text()
    print(f"  Preview: {text[:100]}...")
```

### Time-Based Sliding Windows

Create windows based on time duration:

```python
# Create 5-minute windows with 1-minute overlap
for window in episode.sliding_window_by_time(
    window_duration=300,  # 5 minutes in seconds
    overlap_duration=60    # 1 minute overlap
):
    print(f"Time window: {window.time_range[0]/60:.1f}-{window.time_range[1]/60:.1f}min")
    print(f"  Duration: {window.duration/60:.1f} minutes")
    print(f"  Turns: {window.size}")

    # Analyze speaker distribution in this window
    speaker_dist = window.get_speaker_distribution()
    role_dist = window.get_role_distribution()
    print(f"  Speakers: {list(speaker_dist.keys())}")
    print(f"  Roles: {role_dist}")
```

## Advanced Features

### Context-Aware Processing

Use high overlap to maintain conversation context:

```python
# Use high overlap to maintain conversation context
for window in episode.sliding_window(window_size=15, overlap=10):
    if window.has_overlap:
        print(f"Window {window.window_index + 1} has {len(window.overlap_turns)} overlap turns")

        # Show context from previous window
        print("Context from previous window:")
        for turn in window.overlap_turns[:3]:
            speaker = turn.inferred_speaker_name or turn.speaker[0]
            print(f"  {speaker}: {turn.text[:50]}...")

        # Show new content
        print("New content:")
        for turn in window.new_turns[:3]:
            speaker = turn.inferred_speaker_name or turn.speaker[0]
            print(f"  {speaker}: {turn.text[:50]}...")
```

### Window Statistics

Get statistics for different window configurations:

```python
# Get statistics for different window configurations
configs = [
    (10, 0),   # No overlap
    (10, 2),   # Small overlap
    (10, 5),   # Medium overlap
    (20, 5),   # Larger windows
]

for window_size, overlap in configs:
    stats = episode.get_window_statistics(window_size, overlap)
    print(f"Window size: {window_size}, Overlap: {overlap}")
    print(f"  Total windows: {stats['total_windows']}")
    print(f"  Step size: {stats['step_size']}")
    print(f"  Avg window duration: {stats['avg_window_duration']:.1f}s")
```

### Conversation Flow Analysis

Analyze conversation patterns over time:

```python
# Analyze conversation patterns over time
for window in episode.sliding_window_by_time(120, 30):  # 2min windows, 30s overlap
    # Calculate conversation metrics
    total_words = sum(turn.word_count for turn in window.turns)
    avg_words_per_turn = total_words / len(window.turns) if window.turns else 0
    conversation_density = len(window.turns) / (window.duration/60)  # turns per minute

    print(f"Window {window.time_range[0]/60:.1f}-{window.time_range[1]/60:.1f}min:")
    print(f"  Words: {total_words}, Avg per turn: {avg_words_per_turn:.1f}")
    print(f"  Density: {conversation_density:.1f} turns/min")
    print(f"  Speakers: {len(window.get_speaker_distribution())}")
```

## Analysis Patterns

### Conversation Topic Detection

Analyze conversation segments for topic changes:

```python
def analyze_topic_segments(episode, segment_duration=300):
    """Analyze conversation in fixed-duration segments."""
    segments = []

    for window in episode.sliding_window_by_time(segment_duration, 0):
        segment_data = {
            'time_range': window.time_range,
            'total_words': sum(turn.word_count for turn in window.turns),
            'speaker_distribution': window.get_speaker_distribution(),
            'role_distribution': window.get_role_distribution(),
            'conversation_density': len(window.turns) / (window.duration/60)
        }
        segments.append(segment_data)

    return segments

# Use the function
segments = analyze_topic_segments(episode)
for i, segment in enumerate(segments):
    print(f"Segment {i+1}: {segment['total_words']} words, {segment['conversation_density']:.1f} turns/min")
```

### Speaker Interaction Analysis

Analyze how speakers interact in conversation windows:

```python
def analyze_speaker_interactions(episode, window_size=20):
    """Analyze how speakers interact in conversation windows."""
    interactions = []

    for window in episode.sliding_window(window_size, 5):
        # Count speaker transitions
        transitions = {}
        for i in range(len(window.turns) - 1):
            current_speaker = window.turns[i].inferred_speaker_name
            next_speaker = window.turns[i + 1].inferred_speaker_name
            key = (current_speaker, next_speaker)
            transitions[key] = transitions.get(key, 0) + 1

        interaction_data = {
            'window_index': window.window_index,
            'time_range': window.time_range,
            'transitions': transitions,
            'speaker_distribution': window.get_speaker_distribution()
        }
        interactions.append(interaction_data)

    return interactions

# Use the function
interactions = analyze_speaker_interactions(episode)
for interaction in interactions[:3]:  # Show first 3 windows
    print(f"Window {interaction['window_index'] + 1}:")
    print(f"  Speaker transitions: {interaction['transitions']}")
```

## Window Configuration

### Parameter Guidelines

- **Window Size**: Choose based on your analysis needs
  - Small (5-10 turns): Fine-grained analysis, high context preservation
  - Medium (10-20 turns): Balanced analysis and performance
  - Large (20+ turns): Broad patterns, lower context preservation

- **Overlap**: Balance between context and efficiency
  - No overlap (0): Maximum efficiency, no context preservation
  - Small overlap (1-3 turns): Light context preservation
  - Medium overlap (25-50% of window size): Good context preservation
  - High overlap (50%+ of window size): Maximum context preservation

### Time vs Turn-Based Windows

- **Turn-Based Windows**: Use for conversation flow analysis
  - Consistent number of turns per window
  - Good for speaker interaction analysis
  - Predictable processing time

- **Time-Based Windows**: Use for temporal analysis
  - Consistent time duration per window
  - Good for topic analysis and temporal patterns
  - Variable number of turns per window

## Performance Considerations

### Memory Efficiency

- **Lazy Loading**: Use `load_turns_eagerly=False` for large datasets
- **Window Size**: Balance between memory usage and analysis granularity
- **Overlap**: Higher overlap increases memory usage per window

### Processing Efficiency

- **Window Count**: Larger windows reduce total processing time
- **Overlap**: Higher overlap increases processing time but improves context
- **Time-Based Windows**: May be more efficient for temporal analysis

## Best Practices

1. **Choose Appropriate Window Sizes**: Balance between context preservation and processing efficiency
2. **Use Overlap for Context**: Maintain conversation context across window boundaries
3. **Consider Analysis Goals**: Use turn-based windows for conversation flow, time-based for temporal analysis
4. **Validate Parameters**: Ensure window size > overlap and all parameters are positive
5. **Monitor Memory Usage**: Use lazy loading for large datasets

## Error Handling

The sliding window methods include comprehensive error handling:

```python
try:
    # Invalid parameters will raise ValueError
    for window in episode.sliding_window(window_size=0, overlap=0):
        pass
except ValueError as e:
    print(f"Invalid parameters: {e}")

try:
    # Turns must be loaded first
    episode_without_turns = Episode(...)
    for window in episode_without_turns.sliding_window(5, 1):
        pass
except RuntimeError as e:
    print(f"Turns not loaded: {e}")
```

## Integration with Other Features

### Lazy Loading

Sliding windows work seamlessly with lazy loading:

```python
# Load dataset with lazy loading
dataset = SPORCDataset(load_turns_eagerly=False)

# Load turns only when needed
episode = podcast.episodes[0]
dataset.load_turns_for_episode(episode)

# Use sliding windows
for window in episode.sliding_window(10, 2):
    # Process window
    pass
```

### Selective Loading

Combine with selective loading for efficient processing:

```python
# Load specific podcasts
dataset.load_podcast_subset(max_podcasts=5, category1="Technology")

# Process with sliding windows
for podcast in dataset.get_all_podcasts():
    for episode in podcast.episodes:
        dataset.load_turns_for_episode(episode)

        for window in episode.sliding_window(15, 5):
            # Analyze each window
            pass
```

## Examples

See the [sliding window examples](../examples/sliding_window_examples.py) for complete working examples demonstrating all features.

## API Reference

For complete API documentation, see the [Episode class documentation](Episode.md#sliding-window-methods) and [API Reference](API-Reference.md).