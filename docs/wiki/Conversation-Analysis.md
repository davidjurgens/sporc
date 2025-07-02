# Conversation Analysis

This guide covers how to analyze conversation patterns, speaker interactions, and turn-taking behavior in podcast episodes using the SPORC dataset.

## Overview

Conversation analysis in SPORC involves examining:
- **Turn-taking patterns**: How speakers take turns in conversation
- **Speaker interactions**: Who talks to whom and how often
- **Conversation flow**: The structure and timing of conversations
- **Speaker roles**: How different speakers participate
- **Content patterns**: What topics are discussed and how

## Basic Turn Analysis

### Getting All Turns

```python
from sporc import SPORCDataset

# Load dataset and get an episode
sporc = SPORCDataset()
episodes = sporc.search_episodes(min_duration=1800)  # 30+ minute episodes
episode = episodes[0]

# Get all conversation turns
all_turns = episode.get_all_turns()
print(f"Total turns: {len(all_turns)}")

# Display first few turns
for i, turn in enumerate(all_turns[:5]):
    print(f"Turn {i+1}: {turn.speaker} ({turn.duration:.1f}s) - {turn.text[:50]}...")
```

### Turn Statistics

```python
# Calculate basic turn statistics
durations = [turn.duration for turn in all_turns]
word_counts = [turn.word_count for turn in all_turns]

print(f"Average turn duration: {sum(durations) / len(durations):.1f} seconds")
print(f"Average words per turn: {sum(word_counts) / len(word_counts):.1f}")
print(f"Longest turn: {max(durations):.1f} seconds")
print(f"Shortest turn: {min(durations):.1f} seconds")
print(f"Total conversation time: {sum(durations) / 60:.1f} minutes")
```

### Turn Length Analysis

```python
# Analyze turn length distribution
short_turns = [t for t in all_turns if t.duration < 10]
medium_turns = [t for t in all_turns if 10 <= t.duration < 60]
long_turns = [t for t in all_turns if t.duration >= 60]

print(f"Short turns (<10s): {len(short_turns)} ({len(short_turns)/len(all_turns)*100:.1f}%)")
print(f"Medium turns (10-60s): {len(medium_turns)} ({len(medium_turns)/len(all_turns)*100:.1f}%)")
print(f"Long turns (≥60s): {len(long_turns)} ({len(long_turns)/len(all_turns)*100:.1f}%)")

# Find the longest turns
longest_turns = sorted(all_turns, key=lambda t: t.duration, reverse=True)[:5]
print("\nLongest turns:")
for turn in longest_turns:
    print(f"  {turn.speaker}: {turn.duration:.1f}s - {turn.text[:100]}...")
```

## Speaker Analysis

### Speaker Participation

```python
# Analyze each speaker's participation
speaker_stats = {}
for turn in all_turns:
    speaker = turn.speaker
    if speaker not in speaker_stats:
        speaker_stats[speaker] = {
            'turn_count': 0,
            'total_duration': 0,
            'total_words': 0
        }

    speaker_stats[speaker]['turn_count'] += 1
    speaker_stats[speaker]['total_duration'] += turn.duration
    speaker_stats[speaker]['total_words'] += turn.word_count

# Display speaker statistics
print("Speaker Participation:")
for speaker, stats in speaker_stats.items():
    avg_duration = stats['total_duration'] / stats['turn_count']
    avg_words = stats['total_words'] / stats['turn_count']
    percentage = (stats['total_duration'] / episode.duration_seconds) * 100

    print(f"  {speaker}:")
    print(f"    Turns: {stats['turn_count']}")
    print(f"    Total time: {stats['total_duration'] / 60:.1f} minutes")
    print(f"    Percentage: {percentage:.1f}%")
    print(f"    Avg turn duration: {avg_duration:.1f} seconds")
    print(f"    Avg words per turn: {avg_words:.1f}")
```

### Speaker Turn Patterns

```python
# Analyze turn patterns for each speaker
for speaker in episode.main_speakers:
    speaker_turns = episode.get_turns_by_speaker(speaker)

    if speaker_turns:
        # Calculate turn length distribution
        durations = [t.duration for t in speaker_turns]
        short_count = len([d for d in durations if d < 10])
        medium_count = len([d for d in durations if 10 <= d < 60])
        long_count = len([d for d in durations if d >= 60])

        print(f"\n{turn.inferred_name or speaker} turn patterns:")
        print(f"  Short turns: {short_count} ({short_count/len(durations)*100:.1f}%)")
        print(f"  Medium turns: {medium_count} ({medium_count/len(durations)*100:.1f}%)")
        print(f"  Long turns: {long_count} ({long_count/len(durations)*100:.1f}%)")

        # Find typical turn length
        avg_duration = sum(durations) / len(durations)
        print(f"  Average turn length: {avg_duration:.1f} seconds")
```

## Conversation Flow Analysis

### Turn Transitions

```python
# Analyze speaker transitions
transitions = episode.get_speaker_transitions()
print(f"Total speaker transitions: {len(transitions)}")

# Count transition frequencies
from collections import Counter
transition_counts = Counter(transitions)

print("\nMost common transitions:")
for (speaker1, speaker2), count in transition_counts.most_common(10):
    percentage = (count / len(transitions)) * 100
    print(f"  {speaker1} → {speaker2}: {count} times ({percentage:.1f}%)")

# Analyze self-transitions (same speaker)
self_transitions = [(s1, s2) for s1, s2 in transitions if s1 == s2]
print(f"\nSelf-transitions: {len(self_transitions)} ({len(self_transitions)/len(transitions)*100:.1f}%)")
```

### Conversation Segments

```python
# Identify conversation segments (periods of active conversation)
conversation_flow = episode.get_conversation_flow()

# Find gaps in conversation (silence periods)
gaps = []
for i in range(1, len(conversation_flow)):
    gap = conversation_flow[i]['start_time'] - conversation_flow[i-1]['end_time']
    if gap > 2:  # Gaps longer than 2 seconds
        gaps.append(gap)

if gaps:
    print(f"Conversation gaps: {len(gaps)}")
    print(f"Average gap length: {sum(gaps) / len(gaps):.1f} seconds")
    print(f"Longest gap: {max(gaps):.1f} seconds")

# Find conversation segments
segments = []
current_segment = []
for turn in all_turns:
    if not current_segment:
        current_segment = [turn]
    else:
        # Check if this turn follows closely
        time_gap = turn.start_time - current_segment[-1].end_time
        if time_gap <= 5:  # 5 seconds or less
            current_segment.append(turn)
        else:
            # End current segment and start new one
            if len(current_segment) > 1:
                segments.append(current_segment)
            current_segment = [turn]

# Add final segment
if len(current_segment) > 1:
    segments.append(current_segment)

print(f"\nConversation segments: {len(segments)}")
for i, segment in enumerate(segments[:5]):
    duration = segment[-1].end_time - segment[0].start_time
    speakers = set(turn.speaker for turn in segment)
    print(f"  Segment {i+1}: {len(segment)} turns, {duration:.1f}s, {len(speakers)} speakers")
```

## Time-Based Analysis

### Conversation Patterns Over Time

### Sliding Window Analysis

For analyzing large episodes or maintaining conversation context, you can use sliding windows to process conversations in manageable chunks:

```python
# Analyze conversation patterns using sliding windows
for window in episode.sliding_window(window_size=20, overlap=5):
    print(f"Window {window.window_index + 1}/{window.total_windows}")
    print(f"  Time range: {window.time_range[0]/60:.1f}-{window.time_range[1]/60:.1f}min")
    print(f"  Turns: {window.size}")

    # Analyze speaker distribution in this window
    speaker_dist = window.get_speaker_distribution()
    role_dist = window.get_role_distribution()
    print(f"  Speakers: {list(speaker_dist.keys())}")
    print(f"  Roles: {role_dist}")

    # Calculate conversation metrics
    total_words = sum(turn.word_count for turn in window.turns)
    conversation_density = len(window.turns) / (window.duration/60)  # turns per minute
    print(f"  Words: {total_words}, Density: {conversation_density:.1f} turns/min")
```

#### Context-Aware Analysis

Use high overlap to maintain conversation context across windows:

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

#### Time-Based Windows

For temporal analysis, use time-based sliding windows:

```python
# Analyze conversation in 5-minute segments
for window in episode.sliding_window_by_time(
    window_duration=300,  # 5 minutes
    overlap_duration=60    # 1 minute overlap
):
    # Calculate conversation metrics for this time period
    total_words = sum(turn.word_count for turn in window.turns)
    avg_words_per_turn = total_words / len(window.turns) if window.turns else 0
    conversation_density = len(window.turns) / (window.duration/60)

    print(f"Time window: {window.time_range[0]/60:.1f}-{window.time_range[1]/60:.1f}min")
    print(f"  Words: {total_words}, Avg per turn: {avg_words_per_turn:.1f}")
    print(f"  Density: {conversation_density:.1f} turns/min")
    print(f"  Speakers: {len(window.get_speaker_distribution())}")
```

#### Speaker Interaction Analysis with Windows

Analyze how speakers interact within conversation windows:

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

For more detailed information about sliding windows, see the [Sliding Windows](Sliding-Windows.md) documentation.

```python
# Analyze conversation patterns in different time periods
early_turns = episode.get_turns_by_time_range(0, 300)  # First 5 minutes
middle_turns = episode.get_turns_by_time_range(300, 600)  # 5-10 minutes
late_turns = episode.get_turns_by_time_range(episode.duration_seconds - 300, episode.duration_seconds)  # Last 5 minutes

periods = [
    ("Early", early_turns),
    ("Middle", middle_turns),
    ("Late", late_turns)
]

print("Conversation patterns over time:")
for period_name, turns in periods:
    if turns:
        avg_duration = sum(t.duration for t in turns) / len(turns)
        total_words = sum(t.word_count for t in turns)
        print(f"  {period_name}: {len(turns)} turns, avg {avg_duration:.1f}s, {total_words} words")
```

### Advanced Time Range Analysis

The SPORC package provides flexible time range analysis with different behaviors for handling turns that are partially within a time range:

```python
from sporc import TimeRangeBehavior

# Define a time range
start_time = 600  # 10 minutes
end_time = 900    # 15 minutes

# 1. STRICT behavior: Only include turns completely within the range
strict_turns = episode.get_turns_by_time_range(
    start_time, end_time,
    behavior=TimeRangeBehavior.STRICT
)
print(f"Complete turns within range: {len(strict_turns)}")

# 2. INCLUDE_PARTIAL behavior: Include turns that overlap with the range (default)
partial_turns = episode.get_turns_by_time_range(
    start_time, end_time,
    behavior=TimeRangeBehavior.INCLUDE_PARTIAL
)
print(f"Overlapping turns: {len(partial_turns)}")

# 3. INCLUDE_FULL_TURNS behavior: Include complete turns even if they extend beyond
full_turns = episode.get_turns_by_time_range(
    start_time, end_time,
    behavior=TimeRangeBehavior.INCLUDE_FULL_TURNS
)
print(f"Complete turns touching range: {len(full_turns)}")
```

### Time Range with Trimming Information

For precise analysis, you can get turns with trimming information:

```python
# Get turns with trimming metadata
trimmed_data = episode.get_turns_by_time_range_with_trimming(
    start_time, end_time,
    behavior=TimeRangeBehavior.INCLUDE_PARTIAL
)

for turn_data in trimmed_data:
    turn = turn_data['turn']
    print(f"Speaker: {turn.primary_speaker}")
    print(f"Original time: {turn.start_time/60:.1f}-{turn.end_time/60:.1f} min")
    print(f"Trimmed time: {turn_data['trimmed_start']/60:.1f}-{turn_data['trimmed_end']/60:.1f} min")
    print(f"Was trimmed: {turn_data['was_trimmed']}")
    print(f"Text: {turn_data['original_text'][:100]}...")
    print("---")
```

### Practical Time Range Use Cases

```python
# 1. Get only complete turns for analysis
complete_turns = episode.get_turns_by_time_range(
    600, 900,  # 10-15 minutes
    behavior=TimeRangeBehavior.STRICT
)

# 2. Get all turns that touch a specific time range
all_touching_turns = episode.get_turns_by_time_range(
    600, 900,  # 10-15 minutes
    behavior=TimeRangeBehavior.INCLUDE_FULL_TURNS
)

# 3. Analyze conversation flow in specific segments
segments = [
    (0, 300, "Opening"),
    (episode.duration_seconds/2 - 150, episode.duration_seconds/2 + 150, "Middle"),
    (episode.duration_seconds - 300, episode.duration_seconds, "Closing")
]

for start, end, name in segments:
    turns = episode.get_turns_by_time_range(start, end, TimeRangeBehavior.INCLUDE_PARTIAL)
    print(f"{name} segment: {len(turns)} turns")
```

### Comparing Time Range Behaviors

```python
def compare_time_range_behaviors(episode, start_time, end_time):
    """Compare different time range behaviors."""

    behaviors = [
        ("STRICT", TimeRangeBehavior.STRICT),
        ("INCLUDE_PARTIAL", TimeRangeBehavior.INCLUDE_PARTIAL),
        ("INCLUDE_FULL_TURNS", TimeRangeBehavior.INCLUDE_FULL_TURNS)
    ]

    results = {}
    for name, behavior in behaviors:
        turns = episode.get_turns_by_time_range(start_time, end_time, behavior)
        results[name] = {
            'count': len(turns),
            'total_duration': sum(t.duration for t in turns),
            'total_words': sum(t.word_count for t in turns)
        }

    print(f"Time range: {start_time/60:.1f}-{end_time/60:.1f} minutes")
    for name, data in results.items():
        print(f"  {name}: {data['count']} turns, {data['total_duration']:.1f}s, {data['total_words']} words")

    return results

# Compare behaviors for different time ranges
compare_time_range_behaviors(episode, 0, 300)      # First 5 minutes
compare_time_range_behaviors(episode, 600, 900)    # 10-15 minutes
compare_time_range_behaviors(episode, episode.duration_seconds - 300, episode.duration_seconds)  # Last 5 minutes
```

## Content Analysis

### Turn Content Patterns

```python
# Analyze content patterns in turns
long_turns = episode.get_turns_by_min_length(60)  # 1+ minute turns

print(f"Long turns (1+ minute): {len(long_turns)}")
for turn in long_turns[:3]:
    print(f"\nLong turn by {turn.speaker}:")
    print(f"  Duration: {turn.duration:.1f} seconds")
    print(f"  Words: {turn.word_count}")
    print(f"  Speaking rate: {turn.words_per_minute:.1f} wpm")
    print(f"  Content: {turn.text[:200]}...")

# Analyze speaking rates
speaking_rates = [turn.words_per_minute for turn in all_turns]
avg_rate = sum(speaking_rates) / len(speaking_rates)
print(f"\nAverage speaking rate: {avg_rate:.1f} words per minute")

# Find fast and slow speakers
fast_turns = [t for t in all_turns if t.words_per_minute > 200]
slow_turns = [t for t in all_turns if t.words_per_minute < 100]

print(f"Fast turns (>200 wpm): {len(fast_turns)}")
print(f"Slow turns (<100 wpm): {len(slow_turns)}")
```

### Topic Analysis

```python
# Analyze topics across turns (if available)
for turn in all_turns[:5]:
    if hasattr(turn, 'get_topics'):
        topics = turn.get_topics()
        if topics:
            print(f"Turn by {turn.speaker}: {topics}")

# Analyze keywords across speakers
speaker_keywords = {}
for speaker in episode.main_speakers:
    speaker_turns = episode.get_turns_by_speaker(speaker)
    all_keywords = []

    for turn in speaker_turns:
        if hasattr(turn, 'get_keywords'):
            keywords = turn.get_keywords()
            all_keywords.extend(keywords)

    if all_keywords:
        from collections import Counter
        keyword_counts = Counter(all_keywords)
        speaker_keywords[speaker] = keyword_counts.most_common(5)

print("\nTop keywords by speaker:")
for speaker, keywords in speaker_keywords.items():
    print(f"  {speaker}: {[kw for kw, count in keywords]}")
```

## Advanced Analysis

### Conversation Network Analysis

```python
# Build conversation network
import networkx as nx

# Create graph
G = nx.DiGraph()

# Add nodes (speakers)
for speaker in episode.main_speakers:
    G.add_node(speaker)

# Add edges (transitions)
transitions = episode.get_speaker_transitions()
for speaker1, speaker2 in transitions:
    if G.has_edge(speaker1, speaker2):
        G[speaker1][speaker2]['weight'] += 1
    else:
        G.add_edge(speaker1, speaker2, weight=1)

# Analyze network
print("Conversation Network Analysis:")
print(f"  Nodes (speakers): {G.number_of_nodes()}")
print(f"  Edges (transitions): {G.number_of_edges()}")

# Calculate centrality
if G.number_of_nodes() > 1:
    in_centrality = nx.in_degree_centrality(G)
    out_centrality = nx.out_degree_centrality(G)

    print("\nSpeaker Centrality:")
    for speaker in episode.main_speakers:
        in_cent = in_centrality.get(speaker, 0)
        out_cent = out_centrality.get(speaker, 0)
        print(f"  {speaker}: In={in_cent:.3f}, Out={out_cent:.3f}")
```

### Turn-Taking Patterns

```python
# Analyze turn-taking patterns
turn_patterns = []
for i in range(len(all_turns) - 1):
    current_speaker = all_turns[i].speaker
    next_speaker = all_turns[i + 1].speaker
    turn_patterns.append((current_speaker, next_speaker))

# Find common patterns
from collections import Counter
pattern_counts = Counter(turn_patterns)

print("Common turn-taking patterns:")
for (speaker1, speaker2), count in pattern_counts.most_common(10):
    percentage = (count / len(turn_patterns)) * 100
    print(f"  {speaker1} → {speaker2}: {count} times ({percentage:.1f}%)")

# Analyze turn-taking balance
speaker_turn_counts = {}
for turn in all_turns:
    speaker_turn_counts[turn.speaker] = speaker_turn_counts.get(turn.speaker, 0) + 1

total_turns = len(all_turns)
print(f"\nTurn-taking balance:")
for speaker, count in speaker_turn_counts.items():
    percentage = (count / total_turns) * 100
    print(f"  {speaker}: {count} turns ({percentage:.1f}%)")
```

## Quality Assessment

### Conversation Quality Metrics

```python
# Assess conversation quality
metrics = episode.get_quality_metrics()

print("Conversation Quality Metrics:")
print(f"  Overlap proportion (duration): {metrics['overlap_prop_duration']:.3f}")
print(f"  Overlap proportion (turns): {metrics['overlap_prop_turn_count']:.3f}")
print(f"  Average turn duration: {metrics['avg_turn_duration']:.1f} seconds")
print(f"  Turn count: {metrics['turn_count']}")
print(f"  Word count: {metrics['word_count']}")
print(f"  Words per minute: {metrics['words_per_minute']:.1f}")

# Assess quality levels
if metrics['overlap_prop_duration'] < 0.05:
    quality_level = "Excellent"
elif metrics['overlap_prop_duration'] < 0.1:
    quality_level = "Good"
elif metrics['overlap_prop_duration'] < 0.2:
    quality_level = "Moderate"
else:
    quality_level = "Poor"

print(f"  Overall quality: {quality_level}")
```

### Turn Quality Assessment

```python
# Assess individual turn quality
for turn in all_turns[:10]:  # First 10 turns
    quality_issues = []

    if turn.duration < 1:
        quality_issues.append("Very short")
    elif turn.duration > 300:
        quality_issues.append("Very long")

    if turn.words_per_minute > 250:
        quality_issues.append("Very fast")
    elif turn.words_per_minute < 50:
        quality_issues.append("Very slow")

    if len(turn.text.strip()) < 10:
        quality_issues.append("Very short text")

    if quality_issues:
        print(f"Turn by {turn.speaker}: {', '.join(quality_issues)}")
    else:
        print(f"Turn by {turn.speaker}: Good quality")
```

## Research Applications

### Conversation Analysis for Research

```python
# Example: Analyze conversation patterns for research
def analyze_conversation_patterns(episode):
    """Analyze conversation patterns for research purposes."""

    turns = episode.get_all_turns()
    speakers = episode.main_speakers

    # Calculate basic metrics
    total_turns = len(turns)
    total_duration = sum(t.duration for t in turns)
    avg_turn_duration = total_duration / total_turns

    # Calculate speaker participation
    speaker_participation = {}
    for speaker in speakers:
        speaker_turns = [t for t in turns if t.speaker == speaker]
        speaker_participation[speaker] = {
            'turn_count': len(speaker_turns),
            'total_duration': sum(t.duration for t in speaker_turns),
            'percentage': len(speaker_turns) / total_turns * 100
        }

    # Calculate turn-taking patterns
    transitions = episode.get_speaker_transitions()
    transition_matrix = {}
    for speaker1 in speakers:
        transition_matrix[speaker1] = {}
        for speaker2 in speakers:
            count = sum(1 for s1, s2 in transitions if s1 == speaker1 and s2 == speaker2)
            transition_matrix[speaker1][speaker2] = count

    return {
        'total_turns': total_turns,
        'total_duration': total_duration,
        'avg_turn_duration': avg_turn_duration,
        'speaker_participation': speaker_participation,
        'transition_matrix': transition_matrix
    }

# Use the analysis function
analysis = analyze_conversation_patterns(episode)
print("Conversation Analysis Results:")
print(f"Total turns: {analysis['total_turns']}")
print(f"Total duration: {analysis['total_duration'] / 60:.1f} minutes")
print(f"Average turn duration: {analysis['avg_turn_duration']:.1f} seconds")

print("\nSpeaker Participation:")
for speaker, data in analysis['speaker_participation'].items():
    print(f"  {speaker}: {data['turn_count']} turns ({data['percentage']:.1f}%)")
```

## Performance Tips

1. **Use selective loading** for large datasets
2. **Cache analysis results** for repeated analysis
3. **Process turns in batches** for memory efficiency
4. **Use streaming mode** for large episodes
5. **Filter turns early** to reduce processing time
6. **Use appropriate data structures** for your analysis needs

## Sliding Window Analysis

The SPORC dataset provides powerful sliding window functionality for analyzing conversations in manageable chunks with configurable overlap.

### Basic Sliding Windows

```python
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

```python
# Create 5-minute windows with 1-minute overlap
for window in episode.sliding_window_by_time(
    window_duration=300,  # 5 minutes
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

### Context-Aware Processing

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

## Advanced Analysis Patterns

### Conversation Topic Detection

```python
# Analyze conversation segments for topic changes
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

## Best Practices

1. **Use Lazy Loading**: Enable lazy loading for large datasets to manage memory efficiently
2. **Choose Appropriate Window Sizes**: Balance between context preservation and processing efficiency
3. **Consider Overlap**: Use overlap to maintain conversation context across windows
4. **Time vs Turn-Based Windows**: Use time-based windows for temporal analysis, turn-based for conversation flow
5. **Validate Window Parameters**: Ensure window size > overlap and all parameters are positive

## Use Cases

- **Content Analysis**: Analyze conversation topics and themes in segments
- **Speaker Analysis**: Study speaker patterns and interactions
- **Temporal Analysis**: Understand how conversations evolve over time
- **Context Preservation**: Maintain conversation context for NLP processing
- **Batch Processing**: Process large episodes in manageable chunks