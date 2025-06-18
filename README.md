# SPORC: Structured Podcast Open Research Corpus

A Python package for working with the [SPORC (Structured Podcast Open Research Corpus)](https://huggingface.co/datasets/blitt/SPoRC) dataset from Hugging Face.

## Overview

SPORC is a large multimodal dataset for the study of the podcast ecosystem. This package provides easy-to-use Python classes and functions to interact with the dataset, including:

- **Podcast** class: Collection of episodes and metadata about a podcast
- **Episode** class: Single episode with information about its contents
- Search functionality for podcasts and episodes
- Conversation turn analysis and filtering
- Time-based and speaker-based queries

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

# Initialize the dataset
sporc = SPORCDataset()

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

## Core Classes

### SPORCDataset

The main class for interacting with the SPORC dataset.

```python
from sporc import SPORCDataset

sporc = SPORCDataset()
```

**Methods:**
- `search_podcast(name: str) -> Podcast`: Find a podcast by name
- `search_episodes(**criteria) -> List[Episode]`: Search episodes by various criteria
- `get_all_podcasts() -> List[Podcast]`: Get all podcasts in the dataset

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
@article{blitt2024sporc,
  title={SPORC: the Structured Podcast Open Research Corpus},
  author={Blitt, Joshua and others},
  journal={arXiv preprint arXiv:2411.07892},
  year={2024}
}
```

## Support

For questions, issues, or feature requests, please:

1. Check the [documentation](https://github.com/yourusername/sporc/wiki)
2. Search existing [issues](https://github.com/yourusername/sporc/issues)
3. Create a new issue if your problem isn't already addressed

## Acknowledgments

- The SPORC dataset creators at the University of Michigan
- Hugging Face for hosting the dataset
- The open-source community for the tools that made this package possible