# Frequently Asked Questions (FAQ)

This page answers common questions about the SPORC package and provides solutions to frequently encountered issues.

## Installation and Setup

### Q: How do I install the SPORC package?

**A:** Install the package using pip:

```bash
pip install sporc
```

Make sure you have:
1. Accepted the dataset terms on [Hugging Face](https://huggingface.co/datasets/blitt/SPoRC)
2. Set up Hugging Face authentication with `huggingface-cli login`

### Q: I get an authentication error when trying to load the dataset. What should I do?

**A:** This usually means you need to authenticate with Hugging Face:

1. Install the Hugging Face Hub: `pip install huggingface_hub`
2. Login: `huggingface-cli login`
3. Enter your token from [https://huggingface.co/settings/tokens](https://huggingface.co/settings/tokens)

### Q: I get a "Dataset not found" error. What's wrong?

**A:** You need to accept the dataset terms first:

1. Visit [https://huggingface.co/datasets/blitt/SPoRC](https://huggingface.co/datasets/blitt/SPoRC)
2. Log in to your Hugging Face account
3. Click "I agree" to accept the dataset terms
4. Try loading the dataset again

### Q: The installation is taking a long time. Is this normal?

**A:** Yes, the first time you load the dataset, it needs to download several GB of data. This can take 10-30 minutes depending on your internet connection. Subsequent loads will be much faster as the data is cached locally.

## Usage

### Q: What's the difference between memory mode and streaming mode?

**A:**
- **Memory mode** (default): Loads the entire dataset into RAM for fast access. Requires 8GB+ RAM but provides instant search and access.
- **Streaming mode**: Loads data on-demand, using much less memory (50-100MB) but slower access. Good for systems with limited RAM.

### Q: How do I search for a specific podcast?

**A:** Use the `search_podcast` method:

```python
from sporc import SPORCDataset

sporc = SPORCDataset()
podcast = sporc.search_podcast("Exact Podcast Name")
if podcast:
    print(f"Found: {podcast.title}")
```

### Q: How do I find episodes longer than 30 minutes?

**A:** Use the `search_episodes` method:

```python
long_episodes = sporc.search_episodes(min_duration=1800)  # 1800 seconds = 30 minutes
print(f"Found {len(long_episodes)} episodes")
```

### Q: How do I get all conversation turns from an episode?

**A:** Use the `get_all_turns` method:

```python
episode = podcast.episodes[0]
turns = episode.get_all_turns()
for turn in turns:
    print(f"{turn.speaker}: {turn.text[:100]}...")
```

### Q: How do I analyze speaker participation?

**A:** Use the `get_turns_by_speaker` method:

```python
for speaker in episode.main_speakers:
    speaker_turns = episode.get_turns_by_speaker(speaker)
    total_time = sum(t.duration for t in speaker_turns)
    print(f"{speaker}: {total_time/60:.1f} minutes")
```

## Performance and Memory

### Q: The dataset is using too much memory. What can I do?

**A:** Use streaming mode:

```python
sporc = SPORCDataset(streaming=True)
```

Or use selective loading to load only specific podcasts:

```python
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(categories=['education'])
```

### Q: Search operations are very slow. How can I speed them up?

**A:**
1. Use memory mode instead of streaming mode
2. Use selective loading to work with a smaller subset
3. Cache search results if you're doing repeated searches

### Q: I'm getting a "RuntimeError" when trying to use `len()` on the dataset. Why?

**A:** In streaming mode, you can't use `len()` because the dataset size isn't known until you iterate through all data. Either:
1. Use memory mode: `sporc = SPORCDataset()`
2. Load a subset first: `sporc.load_podcast_subset(...)`
3. Count manually: `count = sum(1 for _ in sporc.iterate_episodes())`

## Data Quality

### Q: What do the overlap proportion metrics mean?

**A:** These indicate the quality of speaker diarization:
- `overlap_prop_duration`: Proportion of time where multiple speakers are talking simultaneously
- `overlap_prop_turn_count`: Proportion of turns that overlap with other turns
- Lower values (0.05-0.1) indicate better quality
- Higher values (0.2+) indicate poorer quality

### Q: How do I filter for high-quality episodes?

**A:** Use quality filters in your search:

```python
good_episodes = sporc.search_episodes(
    max_overlap_prop_duration=0.1,    # Less than 10% overlap
    max_overlap_prop_turn_count=0.2   # Less than 20% overlapping turns
)
```

### Q: Some episodes have very short turns. Is this normal?

**A:** Yes, this is normal. Podcasts often have:
- Short interjections ("uh-huh", "right")
- Quick responses
- Overlapping speech
- Background noise or music

You can filter for longer turns if needed:

```python
long_turns = episode.get_turns_by_min_length(10)  # 10+ second turns
```

## Troubleshooting

### Q: I get an "ImportError: No module named 'datasets'". What should I do?

**A:** Install the required dependencies:

```bash
pip install datasets huggingface_hub
```

Or reinstall the package:

```bash
pip install --force-reinstall sporc
```

### Q: I get a "PermissionError" when trying to access the cache. What's wrong?

**A:** This is usually a file permission issue. Try:

1. Use a virtual environment
2. Install with `--user` flag: `pip install --user sporc`
3. Check permissions on your cache directory
4. Clear the cache: `rm -rf ~/.cache/huggingface/`

### Q: The dataset seems to be corrupted. How do I fix it?

**A:** Clear the cache and redownload:

```bash
rm -rf ~/.cache/huggingface/
```

Then try loading the dataset again.

### Q: I'm getting different results each time I run the same search. Why?

**A:** This shouldn't happen with the same dataset version. Possible causes:
1. You're using streaming mode and the data order is different
2. The dataset was updated
3. You're using different search parameters

Try using memory mode for consistent results.

## Advanced Usage

### Q: How do I analyze conversation patterns across multiple episodes?

**A:** Use selective loading to get a subset, then analyze:

```python
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(categories=['education'])

all_turns = []
for episode in sporc.get_all_episodes():
    turns = episode.get_all_turns()
    all_turns.extend(turns)

# Analyze all turns
print(f"Total turns: {len(all_turns)}")
```

### Q: How do I export conversation data for analysis in other tools?

**A:** Convert to common formats:

```python
import pandas as pd

# Convert turns to DataFrame
turns_data = []
for episode in sporc.get_all_episodes():
    for turn in episode.get_all_turns():
        turns_data.append({
            'episode_title': episode.title,
            'speaker': turn.speaker,
            'start_time': turn.start_time,
            'end_time': turn.end_time,
            'duration': turn.duration,
            'text': turn.text,
            'word_count': turn.word_count
        })

df = pd.DataFrame(turns_data)
df.to_csv('conversation_data.csv', index=False)
```

### Q: How do I analyze speaker transitions?

**A:** Use the `get_speaker_transitions` method:

```python
transitions = episode.get_speaker_transitions()
from collections import Counter
transition_counts = Counter(transitions)

print("Most common transitions:")
for (speaker1, speaker2), count in transition_counts.most_common(5):
    print(f"{speaker1} → {speaker2}: {count} times")
```

### Q: How do I find episodes with specific hosts?

**A:** Use the `host_name` parameter:

```python
simon_episodes = sporc.search_episodes(host_name="Simon Shapiro")
print(f"Found {len(simon_episodes)} episodes by Simon Shapiro")
```

### Q: How do I analyze conversation quality across the dataset?

**A:** Use streaming mode to process all episodes:

```python
sporc = SPORCDataset(streaming=True)

quality_stats = []
for episode in sporc.iterate_episodes():
    quality_stats.append({
        'title': episode.title,
        'overlap_duration': episode.overlap_prop_duration,
        'overlap_turns': episode.overlap_prop_turn_count,
        'avg_turn_duration': episode.avg_turn_duration
    })

# Analyze quality distribution
import pandas as pd
df = pd.DataFrame(quality_stats)
print(f"Average overlap: {df['overlap_duration'].mean():.3f}")
```

## Research Applications

### Q: How do I use SPORC for conversation analysis research?

**A:** Start with the [Conversation Analysis](Conversation-Analysis) guide. Key approaches:

1. **Turn-taking analysis**: Analyze speaker transitions and turn patterns
2. **Speaker role analysis**: Examine how different speakers participate
3. **Content analysis**: Study what topics are discussed and how
4. **Quality assessment**: Filter for high-quality conversations

### Q: How do I compare conversation patterns across different podcast categories?

**A:** Use selective loading for different categories:

```python
# Load education podcasts
sporc_edu = SPORCDataset(streaming=True)
sporc_edu.load_podcast_subset(categories=['education'])

# Load science podcasts
sporc_sci = SPORCDataset(streaming=True)
sporc_sci.load_podcast_subset(categories=['science'])

# Compare patterns
edu_turns = sum(len(ep.get_all_turns()) for ep in sporc_edu.get_all_episodes())
sci_turns = sum(len(ep.get_all_turns()) for ep in sporc_sci.get_all_episodes())

print(f"Education: {edu_turns} turns")
print(f"Science: {sci_turns} turns")
```

### Q: How do I extract features for machine learning?

**A:** Convert conversation data to features:

```python
def extract_features(episode):
    turns = episode.get_all_turns()
    return {
        'turn_count': len(turns),
        'avg_turn_duration': sum(t.duration for t in turns) / len(turns),
        'speaker_count': episode.speaker_count,
        'overlap_prop': episode.overlap_prop_duration,
        'word_count': episode.word_count,
        'words_per_minute': episode.word_count / episode.duration_minutes
    }

features = [extract_features(ep) for ep in sporc.get_all_episodes()]
```

## Getting Help

### Q: Where can I get more help?

**A:**
1. Check the [documentation](Home) for detailed guides
2. Search existing [issues](https://github.com/yourusername/sporc/issues)
3. Create a new issue with:
   - Your operating system and Python version
   - Complete error message
   - Steps to reproduce the issue
   - Code example

### Q: How do I report a bug?

**A:** Create an issue on GitHub with:
1. **Title**: Clear description of the problem
2. **Description**: Detailed explanation
3. **Environment**: OS, Python version, package version
4. **Steps to reproduce**: Exact code and steps
5. **Expected vs actual behavior**: What you expected vs what happened
6. **Error messages**: Complete error output

### Q: How can I contribute to the project?

**A:**
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

See the [Contributing](Contributing) guide for more details.