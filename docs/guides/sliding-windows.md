# Sliding windows

SPORC can iterate an episode's conversation in fixed-size, overlapping chunks.
Sliding windows are useful for processing long episodes, keeping conversational
context together across chunk boundaries, and analyzing how a conversation
changes over time. This guide builds on the [Quick start](../quickstart.md); for
the full method list see the [API reference](../reference/index.md).

## When to use them

- **Long-episode processing**: break a two-hour interview into manageable pieces
- **Context preservation**: overlap keeps the tail of one window at the head of
  the next, so a chunk never starts mid-thought
- **Temporal analysis**: study topic, pacing, or speaker balance over the timeline
- **Bounded memory**: work a slice at a time instead of all turns at once

!!! warning "Windows need turns"
    A window is a slice of `episode.turns`, and only ~65% of episodes were
    diarized into turns. Gate on `episode.has_turn_data` first; iterating an
    episode with no turns yields a single empty window (or, on an episode with
    no turn loader attached, raises `RuntimeError`).

## Two kinds of windows

| Method | Slices by | Each window has | Best for |
|---|---|---|---|
| `sliding_window(window_size, overlap)` | a fixed **number of turns** | a consistent turn count, variable duration | conversation flow, turn-taking |
| `sliding_window_by_time(window_duration, overlap_duration)` | a fixed **number of seconds** | a consistent duration, variable turn count | temporal / topic analysis |

Both yield `TurnWindow` objects and both take their overlap in the same unit as
their size (turns, or seconds).

## The `TurnWindow` object

Each window exposes its position, contents, and helpers:

```python
from sporc import SPORCDataset

sporc = SPORCDataset()
podcast = sporc.search_podcast("How I Built This")
episode = next(ep for ep in podcast.episodes if ep.has_turn_data)

window = next(episode.sliding_window(window_size=10, overlap=2))

# Contents
print(f"Size:       {window.size} turns")
print(f"Time range: {window.time_range[0]:.1f}s - {window.time_range[1]:.1f}s")
print(f"Duration:   {window.duration:.1f}s")

# Position within the episode
print(f"Window {window.window_index + 1} of {window.total_windows}")
print(f"Turn indices: {window.start_index}-{window.end_index}")
print(f"Is first: {window.is_first} | Is last: {window.is_last}")

# Overlap bookkeeping
print(f"Has overlap: {window.has_overlap}")
print(f"New turns:     {len(window.new_turns)}")
print(f"Overlap turns: {len(window.overlap_turns)}")
```

Helper methods on a window:

- `window.get_text(separator=" ")` — the window's turns joined into one string
- `window.get_speaker_distribution()` — `{speaker_label: turn_count}`
- `window.get_role_distribution()` — `{role: turn_count}` (mostly
  `"NO_INFERRED_ROLE"`; see the caveat below)
- `window.to_dict()` — a plain-dict summary

!!! note "Role distributions are a lower bound"
    About 90% of turns carry `inferred_speaker_role == "NO_INFERRED_ROLE"`; only
    ~7.4% are labelled `"host"` and ~1.9% `"guest"`. A window's role
    distribution therefore counts *known* hosts and guests and undercounts the
    rest — treat `"host"`/`"guest"` totals as floors, not a partition.

## Turn-based windows

```python
for window in episode.sliding_window(window_size=10, overlap=2):
    print(f"Window {window.window_index + 1}/{window.total_windows}")
    print(f"  Turns {window.start_index}-{window.end_index} ({window.size} turns)")
    print(f"  Time: {window.time_range[0]:.1f}s - {window.time_range[1]:.1f}s")
    print(f"  New: {len(window.new_turns)}, overlap: {len(window.overlap_turns)}")
    print(f"  Preview: {window.get_text()[:100]}...")
```

With `window_size=10, overlap=2`, each window shares 2 turns with the previous
one and adds 8 new turns — the **step size** is `window_size - overlap`.

## Time-based windows

```python
# 5-minute windows with 1 minute of overlap
for window in episode.sliding_window_by_time(
    window_duration=300,   # seconds
    overlap_duration=60,
):
    print(f"{window.time_range[0]/60:.1f}-{window.time_range[1]/60:.1f} min "
          f"({window.duration/60:.1f} min, {window.size} turns)")
    print(f"  Speakers: {list(window.get_speaker_distribution())}")
    print(f"  Roles:    {window.get_role_distribution()}")
```

## Context-aware processing

Set a high overlap when each chunk must carry enough of the previous
conversation to stand on its own — for example when feeding windows to a
language model. `new_turns` and `overlap_turns` let you tell fresh content from
carried-over context:

```python
for window in episode.sliding_window(window_size=15, overlap=10):
    if not window.has_overlap:
        continue
    print(f"Window {window.window_index + 1}: "
          f"{len(window.overlap_turns)} turns of context")

    print("  Context (from previous window):")
    for turn in window.overlap_turns[:3]:
        speaker = turn.inferred_speaker_name or turn.primary_speaker
        print(f"    {speaker}: {turn.text[:50]}...")

    print("  New content:")
    for turn in window.new_turns[:3]:
        speaker = turn.inferred_speaker_name or turn.primary_speaker
        print(f"    {speaker}: {turn.text[:50]}...")
```

!!! note "`inferred_speaker_name` may be empty"
    Name inference, like role inference, is filled in for only a minority of
    turns. Fall back to `turn.primary_speaker` (the first diarization label)
    when the inferred name is missing, as above.

## Window statistics without iterating

To size a configuration before committing to a full pass, ask for its statistics:

```python
for window_size, overlap in [(10, 0), (10, 2), (10, 5), (20, 5)]:
    stats = episode.get_window_statistics(window_size, overlap)
    print(f"size={window_size}, overlap={overlap}: "
          f"{stats['total_windows']} windows, "
          f"step {stats['step_size']}, "
          f"avg {stats['avg_window_duration']:.1f}s each")
```

`get_window_statistics` returns `total_turns`, `window_size`, `overlap`,
`step_size`, `total_windows`, `avg_window_duration`, `total_duration`, and
`avg_turn_duration`.

## Analysis patterns

### Conversation pacing over time

```python
for window in episode.sliding_window_by_time(120, 30):  # 2-min windows, 30s overlap
    total_words = sum(t.word_count for t in window.turns)
    avg_words = total_words / window.size if window.size else 0
    density = window.size / (window.duration / 60)  # turns per minute

    print(f"{window.time_range[0]/60:.1f}-{window.time_range[1]/60:.1f} min: "
          f"{total_words} words, {avg_words:.1f}/turn, {density:.1f} turns/min, "
          f"{len(window.get_speaker_distribution())} speakers")
```

### Fixed-duration segments

```python
def analyze_segments(episode, segment_duration=300):
    """Summarize an episode in back-to-back, non-overlapping segments."""
    segments = []
    for window in episode.sliding_window_by_time(segment_duration, 0):
        segments.append({
            "time_range": window.time_range,
            "total_words": sum(t.word_count for t in window.turns),
            "speaker_distribution": window.get_speaker_distribution(),
            "role_distribution": window.get_role_distribution(),
            "density": window.size / (window.duration / 60) if window.duration else 0,
        })
    return segments

for i, seg in enumerate(analyze_segments(episode)):
    print(f"Segment {i+1}: {seg['total_words']} words, "
          f"{seg['density']:.1f} turns/min")
```

### Speaker interaction within windows

```python
from collections import Counter

def window_transitions(episode, window_size=20, overlap=5):
    """Count speaker hand-offs inside each window."""
    for window in episode.sliding_window(window_size, overlap):
        transitions = Counter(
            (window.turns[i].primary_speaker, window.turns[i + 1].primary_speaker)
            for i in range(window.size - 1)
        )
        yield window.window_index, window.time_range, transitions

for idx, time_range, transitions in window_transitions(episode):
    if idx >= 3:
        break
    print(f"Window {idx + 1}: {dict(transitions)}")
```

## Choosing parameters

**Window size**

- Small (5-10 turns): fine-grained, maximal context preservation with overlap
- Medium (10-20 turns): a balance of detail and throughput
- Large (20+ turns): broad patterns, coarser resolution

**Overlap**

- `0`: fastest, no shared context between windows
- 1-3 turns: light stitching across boundaries
- 25-50% of the window: solid context preservation
- 50%+: maximum context, at the cost of re-processing turns

Overlap must be strictly less than the window size, and all parameters must be
positive — otherwise the methods raise `ValueError` (see below).

**Turn-based vs time-based**

- Turn-based windows give a consistent turn count — good for turn-taking and
  speaker-interaction work.
- Time-based windows give a consistent duration — good for temporal and topic
  analysis, where you want equal spans of the timeline.

## Error handling

```python
# Invalid parameters raise ValueError
try:
    for window in episode.sliding_window(window_size=5, overlap=5):  # overlap >= size
        pass
except ValueError as e:
    print(f"Invalid parameters: {e}")

# An episode with no turn loader attached raises RuntimeError
try:
    for window in episode.sliding_window(5, 1):
        pass
except RuntimeError as e:
    print(f"Turns not loaded: {e}")
```

## Working efficiently

Episodes reached through `search_podcast` or `search_episodes` load their turns
on demand — the first time you touch `episode.turns` or call a sliding-window
method — so no explicit "load turns" step is needed. To keep a whole run off the
network, fetch the shows you need up front and iterate their episodes:

```python
# Fetch two shows up front; later access needs no network.
sporc = SPORCDataset(subset=["How I Built This", "The Moth"])

for podcast in sporc.get_all_podcasts():
    for episode in podcast.episodes:
        if not episode.has_turn_data:
            continue
        for window in episode.sliding_window(15, 5):
            ...  # analyze each window
```

See [Working with the data](../data-access.md) for more on subsets and lazy
fetching, and [Conversation analysis](conversation-analysis.md) for turn-level
metrics you can compute inside each window.

## Related

- [Conversation analysis](conversation-analysis.md)
- [Quick start](../quickstart.md)
- [API reference](../reference/index.md)
