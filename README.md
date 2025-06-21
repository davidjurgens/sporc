# SPORC: Structured Podcast Open Research Corpus

A Python package for working with the [SPORC (Structured Podcast Open Research Corpus)](https://huggingface.co/datasets/blitt/SPoRC) dataset from Hugging Face.

## Overview

SPORC is a large multimodal dataset for the study of the podcast ecosystem. This package provides easy-to-use Python classes and functions to interact with the dataset, including:

- **Podcast** class: Collection of episodes and metadata about a podcast
- **Episode** class: Single episode with information about its contents
- Search functionality for podcasts and episodes
- Conversation turn analysis and filtering
- Time-based and speaker-based queries
- **Streaming support** for memory-efficient processing of large datasets
- **Selective loading** for filtering and loading specific podcast subsets into memory

## Installation

### Prerequisites

Before installing this package, you need to:

1. **Accept the SPORC dataset terms** on Hugging Face:
   - Visit [https://huggingface.co/datasets/blitt/SPoRC](https://huggingface.co/datasets/blitt/SPoRC)
   - Log in to your Hugging Face account
   - Click "I agree" to accept the dataset terms

2. **Set up Hugging Face credentials** on your local machine:
   ```bash
   pip install huggingface_hub
   huggingface-cli login
   ```

### Install the Package

```bash
pip install sporc
```

Or install from source:

```bash
git clone https://github.com/yourusername/sporc.git
cd sporc
pip install -e .
```

## Quick Start

```python
from sporc import SPORCDataset

# Initialize the dataset (memory mode - default)
sporc = SPORCDataset()

# Or use streaming mode for memory efficiency
sporc_streaming = SPORCDataset(streaming=True)

# Or use selective mode to load specific podcasts into memory
sporc_selective = SPORCDataset(streaming=True)
sporc_selective.load_podcast_subset(categories=['education'])

# Search for a specific podcast
podcast = sporc.search_podcast("SingOut SpeakOut")

# Get all episodes for this podcast
for episode in podcast.episodes:
    print(f"Episode: {episode.title}")
    print(f"Duration: {episode.duration_seconds} seconds")
    print(f"Hosts: {episode.host_names}")
    print("---")

# Search for episodes with specific criteria
episodes = sporc.search_episodes(
    min_duration=300,  # At least 5 minutes
    max_speakers=3,    # Maximum 3 speakers
    host_name="Simon Shapiro"
)

# Get conversation turns for a specific episode
episode = episodes[0]
turns = episode.get_turns_by_time_range(0, 180)  # First 3 minutes
for turn in turns:
    print(f"Speaker: {turn.speaker}")
    print(f"Text: {turn.text[:100]}...")
    print(f"Duration: {turn.duration} seconds")
    print("---")
```

## Memory vs Streaming vs Selective Mode

The SPORC package supports three modes for loading the dataset:

### Memory Mode (Default)
```python
sporc = SPORCDataset()  # streaming=False (default)
```

**Advantages:**
- Fast access to all data
- Can use `len()` to get dataset size
- All search operations are instant
- Can iterate over data multiple times
- Full dataset statistics available immediately

**Disadvantages:**
- High memory usage (loads entire dataset into RAM)
- Slower initial loading time
- May not work on systems with limited memory

### Streaming Mode
```python
sporc = SPORCDataset(streaming=True)
```

**Advantages:**
- Low memory usage (loads data on-demand)
- Fast initial loading
- Works on systems with limited memory
- Can process datasets larger than available RAM

**Disadvantages:**
- Slower access to individual items
- In streaming mode, `len(sporc)` returns 1,134,058 (the total number of episodes) unless a subset has been loaded, in which case it returns the size of the subset.
- Search operations require iterating through data
- Cannot iterate over data multiple times without reloading
- Statistics calculation requires full iteration

### Selective Mode
```python
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(categories=['education'])
```

**Advantages:**
- Memory efficient (only loads selected subset)
- O(1) access to selected podcasts and episodes
- Fast search operations on the subset
- Can iterate over data multiple times
- Statistics available immediately for the subset
- Best of both worlds: memory efficiency + fast access

**Disadvantages:**
- Initial filtering requires O(n) iteration
- Cannot access podcasts outside the selected subset
- Requires knowing filtering criteria in advance

### When to Use Each Mode

**Use Memory Mode when:**
- You have sufficient RAM (8GB+ recommended)
- You need fast access to multiple episodes
- You want to perform complex searches frequently
- You need to iterate over data multiple times
- You're working with smaller subsets of the dataset

**Use Streaming Mode when:**
- You have limited RAM (< 8GB)
- You're processing the entire dataset sequentially
- You only need to access a few episodes
- You're doing one-pass analysis
- You're working on systems with memory constraints

**Use Selective Mode when:**
- You want to work with a specific genre, host, or category
- You have limited RAM but need fast access to a subset
- You want to perform frequent searches on a filtered dataset
- You know your filtering criteria in advance
- You want the best balance of memory efficiency and performance

### Selective Mode Examples

```python
# Initialize streaming mode
sporc = SPORCDataset(streaming=True)

# Load only education podcasts
sporc.load_podcast_subset(categories=['education'])
print(f"Loaded {len(sporc)} episodes from education podcasts")

# Now you have O(1) access to education podcasts
education_podcasts = sporc.get_all_podcasts()
for podcast in education_podcasts:
    print(f"Education podcast: {podcast.title}")

# Fast search within the subset
long_education_episodes = sporc.search_episodes(min_duration=1800)  # 30+ minutes
print(f"Found {len(long_education_episodes)} long education episodes")

# Load podcasts by specific hosts
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(hosts=['Simon Shapiro', 'John Doe'])
print(f"Loaded {len(sporc)} episodes from selected hosts")

# Load podcasts with at least 10 episodes
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(min_episodes=10)
print(f"Loaded {len(sporc)} episodes from podcasts with 10+ episodes")

# Load English podcasts with at least 5 hours of content
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(language='en', min_total_duration=5.0)
print(f"Loaded {len(sporc)} episodes from substantial English podcasts")

# Complex filtering
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(
    categories=['education', 'science'],
    min_episodes=5,
    min_total_duration=2.0,  # 2+ hours
    language='en'
)
print(f"Loaded {len(sporc)} episodes from substantial English education/science podcasts")
```

## Core Classes

### SPORCDataset

The main class for interacting with the SPORC dataset.

```python
from sporc import SPORCDataset

# Memory mode
sporc = SPORCDataset()

# Streaming mode
sporc = SPORCDataset(streaming=True)

# Selective mode
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(categories=['education'])
```

**Methods:**
- `search_podcast(name: str) -> Podcast`: Find a podcast by name
- `search_episodes(**criteria) -> List[Episode]`: Search episodes by various criteria
- `get_all_podcasts() -> List[Podcast]`: Get all podcasts in the dataset
- `get_all_episodes() -> List[Episode]`: Get all episodes in the dataset
- `iterate_episodes() -> Iterator[Episode]`: Iterate over episodes (streaming only)
- `iterate_podcasts() -> Iterator[Podcast]`: Iterate over podcasts (streaming only)
- `load_podcast_subset(**criteria) -> None`: Load filtered subset into memory (streaming only)
- `get_dataset_statistics() -> Dict[str, Any]`: Get dataset statistics

### Podcast

Represents a podcast with its episodes and metadata.

```python
podcast = sporc.search_podcast("Example Podcast")
print(f"Title: {podcast.title}")
print(f"Description: {podcast.description}")
print(f"Category: {podcast.category}")
print(f"Number of episodes: {len(podcast.episodes)}")
```

**Properties:**
- `title`: Podcast title
- `description`: Podcast description
- `category`: Primary category
- `episodes`: List of Episode objects
- `host_names`: List of predicted host names

### Episode

Represents a single podcast episode.

```python
episode = podcast.episodes[0]
print(f"Title: {episode.title}")
print(f"Duration: {episode.duration_seconds} seconds")
print(f"Hosts: {episode.host_names}")
print(f"Guests: {episode.guest_names}")
```

**Methods:**
- `get_turns_by_time_range(start_time: float, end_time: float) -> List[Turn]`
- `get_turns_by_speaker(speaker_name: str) -> List[Turn]`
- `get_turns_by_min_length(min_length: int) -> List[Turn]`
- `get_all_turns() -> List[Turn]`

**Properties:**
- `title`: Episode title
- `description`: Episode description
- `duration_seconds`: Episode duration in seconds
- `host_names`: List of predicted host names
- `guest_names`: List of predicted guest names
- `main_speakers`: List of main speaker labels
- `transcript`: Full episode transcript

### Turn

Represents a single conversation turn in an episode.

```python
turn = episode.get_all_turns()[0]
print(f"Speaker: {turn.speaker}")
print(f"Text: {turn.text}")
print(f"Start time: {turn.start_time} seconds")
print(f"End time: {turn.end_time} seconds")
print(f"Duration: {turn.duration} seconds")
```

**Properties:**
- `speaker`: Speaker label (e.g., "SPEAKER_00")
- `text`: Spoken text
- `start_time`: Turn start time in seconds
- `end_time`: Turn end time in seconds
- `duration`: Turn duration in seconds
- `inferred_role`: Inferred speaker role (host, guest, etc.)
- `inferred_name`: Inferred speaker name

## Search Examples

### Search by Podcast Name
```python
podcast = sporc.search_podcast("Brazen Education")
```

### Search Episodes by Duration
```python
# Episodes longer than 10 minutes
long_episodes = sporc.search_episodes(min_duration=600)

# Episodes between 5-15 minutes
medium_episodes = sporc.search_episodes(min_duration=300, max_duration=900)
```

### Search Episodes by Speaker Count
```python
# Episodes with exactly 2 speakers
two_speaker_episodes = sporc.search_episodes(min_speakers=2, max_speakers=2)

# Episodes with 3 or more speakers
multi_speaker_episodes = sporc.search_episodes(min_speakers=3)
```

### Search Episodes by Host
```python
# Episodes hosted by Simon Shapiro
simon_episodes = sporc.search_episodes(host_name="Simon Shapiro")
```

### Search Episodes by Category
```python
# Education podcasts
education_episodes = sporc.search_episodes(category="education")

# Music podcasts
music_episodes = sporc.search_episodes(category="music")
```

## Selective Loading Examples

### Load by Category
```python
# Load only education podcasts
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(categories=['education'])

# Now fast access to education content
education_podcasts = sporc.get_all_podcasts()
long_education_episodes = sporc.search_episodes(min_duration=1800)
```

### Load by Host
```python
# Load podcasts by specific hosts
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(hosts=['Simon Shapiro', 'John Doe'])

# Fast access to episodes from these hosts
host_episodes = sporc.get_all_episodes()
```

### Load by Episode Count
```python
# Load podcasts with substantial episode counts
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(min_episodes=10)

# Work with established podcasts
established_podcasts = sporc.get_all_podcasts()
```

### Load by Duration
```python
# Load podcasts with substantial content
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(min_total_duration=5.0)  # 5+ hours

# Work with substantial podcasts
substantial_podcasts = sporc.get_all_podcasts()
```

### Complex Filtering
```python
# Load substantial English education/science podcasts
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(
    categories=['education', 'science'],
    min_episodes=5,
    min_total_duration=2.0,
    language='en'
)

# Now you have fast access to a curated subset
curated_episodes = sporc.get_all_episodes()
curated_podcasts = sporc.get_all_podcasts()
```

## Conversation Turn Analysis

### Get Turns by Time Range
```python
# First 5 minutes of an episode
early_turns = episode.get_turns_by_time_range(0, 300)

# Last 10 minutes of an episode
late_turns = episode.get_turns_by_time_range(
    episode.duration_seconds - 600,
    episode.duration_seconds
)
```

### Get Turns by Speaker
```python
# All turns by the host
host_turns = episode.get_turns_by_speaker("SPEAKER_00")

# All turns by a specific guest
guest_turns = episode.get_turns_by_speaker("SPEAKER_01")
```

### Get Turns by Length
```python
# Turns longer than 30 seconds
long_turns = episode.get_turns_by_min_length(30)

# Turns longer than 2 minutes
very_long_turns = episode.get_turns_by_min_length(120)
```

### Time-Based Analysis

```python
# Get turns from specific time ranges
turns = episode.get_turns_by_time_range(0, 180)  # First 3 minutes

# Advanced time range analysis with different behaviors
from sporc import TimeRangeBehavior

# Only complete turns within the range
strict_turns = episode.get_turns_by_time_range(300, 600, TimeRangeBehavior.STRICT)

# Include turns that overlap with the range (default)
partial_turns = episode.get_turns_by_time_range(300, 600, TimeRangeBehavior.INCLUDE_PARTIAL)

# Include complete turns even if they extend beyond the range
full_turns = episode.get_turns_by_time_range(300, 600, TimeRangeBehavior.INCLUDE_FULL_TURNS)
```

## Data Quality Indicators

The SPORC dataset includes several quality indicators that can help you filter data:

```python
episode = podcast.episodes[0]

# Diarization quality indicators
print(f"Overlap proportion (duration): {episode.overlap_prop_duration}")
print(f"Overlap proportion (turn count): {episode.overlap_prop_turn_count}")
print(f"Average turn duration: {episode.avg_turn_duration}")
print(f"Total speaker labels: {episode.total_speaker_labels}")

# Filter episodes with good diarization quality
good_quality_episodes = sporc.search_episodes(
    max_overlap_prop_duration=0.1,  # Less than 10% overlap
    max_overlap_prop_turn_count=0.2  # Less than 20% overlapping turns
)
```

## Performance Considerations

### Memory Usage

**Memory Mode:**
- Initial memory usage: ~2-4GB (depending on dataset size)
- Memory usage remains constant during processing
- Fast access to all data

**Streaming Mode:**
- Initial memory usage: ~50-100MB
- Memory usage varies during processing (typically 100-500MB per episode)
- Memory is freed after processing each episode

**Selective Mode:**
- Initial memory usage: ~50-100MB
- Memory usage after loading: ~100MB-2GB (depending on subset size)
- Memory usage remains constant during processing
- Fast access to selected subset

### Processing Speed

**Memory Mode:**
- Fast search operations (O(1) for indexed data)
- Instant access to any episode
- Slower initial loading

**Streaming Mode:**
- Slower search operations (O(n) - must iterate through data)
- Slower access to individual episodes
- Fast initial loading

**Selective Mode:**
- Initial filtering: O(n) (one-time cost)
- Fast search operations on subset (O(1) for indexed data)
- Fast access to selected episodes
- Fast initial loading

### Recommended System Requirements

**Memory Mode:**
- RAM: 8GB+ recommended
- Storage: 10GB+ for dataset cache
- CPU: Any modern processor

**Streaming Mode:**
- RAM: 4GB+ minimum, 8GB+ recommended
- Storage: 10GB+ for dataset cache
- CPU: Any modern processor

**Selective Mode:**
- RAM: 4GB+ minimum, 8GB+ recommended
- Storage: 10GB+ for dataset cache
- CPU: Any modern processor

## Error Handling

The package includes comprehensive error handling for common issues:

```python
from sporc import SPORCDataset, SPORCError

try:
    sporc = SPORCDataset()
    podcast = sporc.search_podcast("Nonexistent Podcast")
except SPORCError as e:
    print(f"Error: {e}")
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Citation

If you use this package in your research, please cite the original SPORC paper:

```bibtex
@article{blitt2025sporc,
  title={SPORC: the Structured Podcast Open Research Corpus},
  author={Litterer, Ben and Jurgens, David and Card, Dallas},
  booktitle={Proceedings of the 63rd Annual Meeting of the Association for Computational Linguistics},
  year={2025}
}
```

## Support

For questions, issues, or feature requests, please:

1. Check the [documentation](https://github.com/yourusername/sporc/wiki)
2. Search existing [issues](https://github.com/yourusername/sporc/issues)
3. Create a new issue if your problem isn't already addressed

## Acknowledgments

- Hugging Face for hosting the dataset
- The open-source community for the tools that made this package possible