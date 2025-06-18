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