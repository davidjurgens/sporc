# Conversation analysis

This guide covers how to analyze conversation patterns, speaker interactions,
and turn-taking behavior in podcast episodes using the SPORC dataset. It builds
on the [Quick start](../quickstart.md); for the full method list see the
[API reference](../reference/index.md).

## What you can analyze

Conversation analysis in SPORC works from an episode's **turns** — contiguous
stretches of speech attributed to one or more speakers. From those you can study:

- **Turn-taking**: how speakers hand off the floor
- **Speaker participation**: who talks, how often, and for how long
- **Timing and pacing**: turn durations, speaking rate, and how they shift over an episode
- **Roles**: how hosts and guests participate (with the caveats below)

!!! warning "Two facts that shape every turn analysis"
    - **Only ~65% of episodes have turn data.** `len(episode.turns) == 0` almost
      always means the corpus never diarized that episode, not that nobody
      spoke. Always gate on `episode.has_turn_data` before drawing conclusions.
    - **Speaker roles are a lower bound, not a partition.** About 90% of turns
      carry `inferred_speaker_role == "NO_INFERRED_ROLE"`; only ~7.4% are
      labelled `"host"` and ~1.9% `"guest"`. So `get_host_turns()`,
      `get_guest_turns()`, and any role distribution count *known* hosts and
      guests — they undercount the real totals and never sum to all turns.

## Getting turns

Load a podcast and pick an episode that actually has turn data:

```python
from sporc import SPORCDataset

sporc = SPORCDataset()
podcast = sporc.search_podcast("The Tim Ferriss Show")

# Pick the first episode that was diarized into speaker turns.
episode = next(ep for ep in podcast.episodes if ep.has_turn_data)

# Two equivalent ways to reach the turns:
all_turns = episode.get_all_turns()   # method form
all_turns = episode.turns             # property form
print(f"Total turns: {len(all_turns)}")

for i, turn in enumerate(all_turns[:5]):
    # turn.speaker is a LIST of diarization labels; primary_speaker is the first.
    print(f"Turn {i+1}: {turn.primary_speaker} "
          f"({turn.duration:.1f}s) - {turn.text[:50]}...")
```

!!! note "`turn.speaker` is a list"
    Each turn can be attributed to more than one speaker (overlapping speech),
    so `turn.speaker` is a list of labels. Use `turn.primary_speaker` for the
    first label, and `turn.is_overlapping` to detect multi-speaker turns.

## Basic turn statistics

The episode computes a summary for you:

```python
stats = episode.get_turn_statistics()
print(f"Total turns:      {stats['total_turns']}")
print(f"Total words:      {stats['total_words']}")
print(f"Avg turn length:  {stats['avg_turn_duration']:.1f} s")
print(f"Avg words/turn:   {stats['avg_words_per_turn']:.1f}")
```

Or roll your own from the turn objects:

```python
durations = [turn.duration for turn in all_turns]
word_counts = [turn.word_count for turn in all_turns]

print(f"Average turn duration: {sum(durations) / len(durations):.1f} seconds")
print(f"Average words per turn: {sum(word_counts) / len(word_counts):.1f}")
print(f"Longest turn:  {max(durations):.1f} seconds")
print(f"Shortest turn: {min(durations):.1f} seconds")
print(f"Total speech time: {sum(durations) / 60:.1f} minutes")
```

### Turn length distribution

```python
short_turns = [t for t in all_turns if t.duration < 10]
medium_turns = [t for t in all_turns if 10 <= t.duration < 60]
long_turns = [t for t in all_turns if t.duration >= 60]

n = len(all_turns)
print(f"Short  (<10s):   {len(short_turns)} ({len(short_turns)/n*100:.1f}%)")
print(f"Medium (10-60s): {len(medium_turns)} ({len(medium_turns)/n*100:.1f}%)")
print(f"Long   (>=60s):  {len(long_turns)} ({len(long_turns)/n*100:.1f}%)")

# The longest turns (often a guest telling a story)
for turn in sorted(all_turns, key=lambda t: t.duration, reverse=True)[:5]:
    print(f"  {turn.primary_speaker}: {turn.duration:.1f}s - {turn.text[:80]}...")
```

`get_turns_by_min_length` does the same filtering server-side of your loop:

```python
monologue_turns = episode.get_turns_by_min_length(60)  # turns >= 60s
print(f"Turns of a minute or more: {len(monologue_turns)}")
```

## Speaker participation

The turn statistics carry a speaker distribution keyed by diarization label. Use
it to size each speaker's contribution:

```python
speaker_counts = episode.get_turn_statistics()["speaker_distribution"]

participation = {}
for speaker in speaker_counts:
    turns = episode.get_turns_by_speaker(speaker)
    participation[speaker] = {
        "turn_count": len(turns),
        "total_duration": sum(t.duration for t in turns),
        "total_words": sum(t.word_count for t in turns),
    }

for speaker, s in participation.items():
    share = (s["total_duration"] / episode.duration_seconds) * 100
    print(f"{speaker}:")
    print(f"  Turns: {s['turn_count']}, "
          f"time: {s['total_duration']/60:.1f} min ({share:.1f}% of episode)")
    print(f"  Avg turn: {s['total_duration']/s['turn_count']:.1f}s, "
          f"avg words: {s['total_words']/s['turn_count']:.1f}")
```

## Working with roles

When you specifically want known hosts or guests, use the role accessors — but
remember they are a lower bound (see the warning above):

```python
host_turns = episode.get_host_turns()          # inferred_speaker_role == "host"
guest_turns = episode.get_guest_turns()         # inferred_speaker_role == "guest"
role_turns = episode.get_turns_by_role("host")  # the general form

role_dist = episode.get_turn_statistics()["role_distribution"]
print(role_dist)  # usually dominated by "NO_INFERRED_ROLE"

labelled = len(host_turns) + len(guest_turns)
print(f"Turns with a host/guest label: {labelled} of {len(all_turns)} "
      f"({labelled/len(all_turns)*100:.1f}%) — the rest are unlabelled, "
      f"not silent.")
```

Episode-level role facts come from metadata, not from the turn labels, and are
more reliable for a yes/no question:

```python
print("Has guests:", episode.has_guests)
print("Hosts:", episode.host_names)
print("Guests:", episode.guest_names)
print("Interview?", episode.is_interview, "| Solo?", episode.is_solo)
```

## Turn-taking patterns

Speaker transitions are not a stored method — build them from consecutive turns
using `primary_speaker`:

```python
from collections import Counter

transitions = Counter(
    (all_turns[i].primary_speaker, all_turns[i + 1].primary_speaker)
    for i in range(len(all_turns) - 1)
)

print("Most common transitions:")
total = sum(transitions.values())
for (a, b), count in transitions.most_common(10):
    label = "same speaker" if a == b else "hand-off"
    print(f"  {a} -> {b}: {count} ({count/total*100:.1f}%, {label})")

self_transitions = sum(c for (a, b), c in transitions.items() if a == b)
print(f"\nConsecutive same-speaker turns: "
      f"{self_transitions} ({self_transitions/total*100:.1f}%)")
```

!!! tip "Segmenting conversation"
    To find bursts of active back-and-forth, group turns whose start follows the
    previous turn's end by a small gap:

    ```python
    segments, current = [], []
    for turn in all_turns:
        if current and turn.start_time - current[-1].end_time > 5:  # >5s gap
            if len(current) > 1:
                segments.append(current)
            current = [turn]
        else:
            current.append(turn)
    if len(current) > 1:
        segments.append(current)

    print(f"Conversation segments: {len(segments)}")
    for i, seg in enumerate(segments[:5]):
        span = seg[-1].end_time - seg[0].start_time
        speakers = {t.primary_speaker for t in seg}
        print(f"  Segment {i+1}: {len(seg)} turns, {span:.1f}s, "
              f"{len(speakers)} speakers")
    ```

## Time-based analysis

### Comparing periods of an episode

```python
duration = episode.duration_seconds
periods = [
    ("Opening", episode.get_turns_by_time_range(0, 300)),
    ("Middle",  episode.get_turns_by_time_range(300, 600)),
    ("Closing", episode.get_turns_by_time_range(duration - 300, duration)),
]

for name, turns in periods:
    if turns:
        avg = sum(t.duration for t in turns) / len(turns)
        words = sum(t.word_count for t in turns)
        print(f"{name}: {len(turns)} turns, avg {avg:.1f}s, {words} words")
```

### Choosing how partial turns are handled

`get_turns_by_time_range` accepts a `TimeRangeBehavior` controlling turns that
straddle the boundary:

```python
from sporc import TimeRangeBehavior

start, end = 600, 900  # 10-15 minutes

strict = episode.get_turns_by_time_range(
    start, end, behavior=TimeRangeBehavior.STRICT)            # fully inside only
partial = episode.get_turns_by_time_range(
    start, end, behavior=TimeRangeBehavior.INCLUDE_PARTIAL)   # any overlap (default)
full = episode.get_turns_by_time_range(
    start, end, behavior=TimeRangeBehavior.INCLUDE_FULL_TURNS)  # whole turns that touch

print(f"STRICT: {len(strict)}, PARTIAL: {len(partial)}, FULL: {len(full)}")
```

### Turns with trimming metadata

When you need to know exactly how a boundary clipped each turn, ask for the
trimming record:

```python
trimmed = episode.get_turns_by_time_range_with_trimming(
    600, 900, behavior=TimeRangeBehavior.INCLUDE_PARTIAL)

for row in trimmed[:5]:
    turn = row["turn"]
    print(f"{turn.primary_speaker}: "
          f"{turn.start_time/60:.1f}-{turn.end_time/60:.1f} min "
          f"-> trimmed {row['trimmed_start']/60:.1f}-{row['trimmed_end']/60:.1f} "
          f"(was_trimmed={row['was_trimmed']})")
```

Each record has `turn`, `original_text`, `trimmed_text`, `trimmed_start`,
`trimmed_end`, and `was_trimmed`.

## Pacing and speaking rate

Turns expose `words_per_second`; multiply by 60 for a words-per-minute feel:

```python
rates = [t.words_per_second for t in all_turns if t.duration > 0]
avg_wps = sum(rates) / len(rates)
print(f"Average speaking rate: {avg_wps:.2f} words/sec "
      f"(~{avg_wps*60:.0f} wpm)")

fast = [t for t in all_turns if t.words_per_second > 3.5]  # ~210 wpm
slow = [t for t in all_turns if 0 < t.words_per_second < 1.7]  # ~100 wpm
print(f"Fast turns (>3.5 w/s): {len(fast)} | slow turns (<1.7 w/s): {len(slow)}")
```

!!! note "`word_count` vs `token_count`"
    `word_count` is whitespace-separated words and is defined for every turn.
    `token_count` is the number of timestamped tokens the aligner produced
    (punctuation counted separately, ~21% higher) and is `None` for turns
    carried over from dataset 1.0. Use `word_count` for stable rate math.

## Sliding-window analysis

For long episodes, or to keep conversational context together while you process
it, iterate the transcript in overlapping windows. That has its own guide:

- [Sliding windows](sliding-windows.md) — turn-based and time-based windows,
  overlap for context, and per-window statistics.

A one-line taste:

```python
for window in episode.sliding_window(window_size=20, overlap=5):
    print(f"Window {window.window_index + 1}/{window.total_windows}: "
          f"{window.size} turns, {window.duration/60:.1f} min, "
          f"roles={window.get_role_distribution()}")
```

## A reusable analysis function

```python
from collections import Counter

def analyze_conversation(episode):
    """Summarize turn-taking and participation for one episode."""
    if not episode.has_turn_data:
        return None

    turns = episode.get_all_turns()
    speaker_counts = episode.get_turn_statistics()["speaker_distribution"]

    participation = {}
    for speaker in speaker_counts:
        s_turns = episode.get_turns_by_speaker(speaker)
        participation[speaker] = {
            "turn_count": len(s_turns),
            "total_duration": sum(t.duration for t in s_turns),
            "share": len(s_turns) / len(turns) * 100,
        }

    transitions = Counter(
        (turns[i].primary_speaker, turns[i + 1].primary_speaker)
        for i in range(len(turns) - 1)
    )

    return {
        "total_turns": len(turns),
        "total_duration": sum(t.duration for t in turns),
        "avg_turn_duration": sum(t.duration for t in turns) / len(turns),
        "participation": participation,
        "top_transitions": transitions.most_common(5),
    }

result = analyze_conversation(episode)
if result:
    print(f"Total turns: {result['total_turns']}")
    print(f"Total time:  {result['total_duration']/60:.1f} min")
    for speaker, data in result["participation"].items():
        print(f"  {speaker}: {data['turn_count']} turns ({data['share']:.1f}%)")
```

## Performance tips

1. **Gate on `episode.has_turn_data`** so you skip the ~35% of episodes with no
   turns instead of processing empty lists.
2. **Fetch a subset up front** with `SPORCDataset(subset=[...])` when you know
   which shows you need — it keeps later access off the network. See
   [Working with the data](../data-access.md).
3. **Filter turns early** (`get_turns_by_min_length`, `get_turns_by_time_range`)
   rather than materializing every turn and filtering in Python.
4. **Reuse `get_turn_statistics()`** instead of recomputing sums per speaker.
5. **Process long episodes in [sliding windows](sliding-windows.md)** to keep
   memory bounded.

## Related

- [Sliding windows](sliding-windows.md)
- [Quick start](../quickstart.md)
- [API reference](../reference/index.md)
