# SPORC: Structured Podcast Open Research Corpus

A Python package for working with the [SPORC (Structured Podcast Open Research Corpus)](https://huggingface.co/datasets/blitt/SPoRC) dataset from Hugging Face.

## Overview

SPORC is a large multimodal dataset for the study of the podcast ecosystem. This package provides easy-to-use Python classes and functions to interact with the dataset, including:

- **Podcast** class: Collection of episodes and metadata about a podcast
- **Episode** class: Single episode with information about its contents
- **Turn** class: Individual conversation turns with speaker information
- Search functionality for podcasts and episodes
- Conversation turn analysis and filtering
- **Sliding windows** for processing large episodes in manageable chunks
- **Streaming support** for memory-efficient processing of large datasets
- **Selective loading** for filtering and loading specific podcast subsets into memory
- **Lazy loading** for efficient turn data access

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
```

## Core Classes

### SPORCDataset

The main class for interacting with the SPORC dataset.

```python
from sporc import SPORCDataset

# Memory mode (default)
sporc = SPORCDataset()

# Streaming mode for memory efficiency
sporc = SPORCDataset(streaming=True)

# Selective mode to load specific podcasts
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(categories=['education'])
```

### Podcast

Represents a podcast with its episodes and metadata.

```python
podcast = sporc.search_podcast("Example Podcast")
print(f"Title: {podcast.title}")
print(f"Category: {podcast.category}")
print(f"Number of episodes: {len(podcast.episodes)}")
```

### Episode

Represents a single podcast episode.

```python
episode = podcast.episodes[0]
print(f"Title: {episode.title}")
print(f"Duration: {episode.duration_seconds} seconds")
print(f"Hosts: {episode.host_names}")
```

### Turn

Represents a single conversation turn in an episode.

```python
turn = episode.get_all_turns()[0]
print(f"Speaker: {turn.speaker}")
print(f"Text: {turn.text}")
print(f"Duration: {turn.duration} seconds")
```

## Key Features

### Memory Modes

The package supports three modes for different use cases:

- **Memory Mode**: Fast access, high memory usage (default)
- **Streaming Mode**: Memory efficient, slower access
- **Selective Mode**: Best of both worlds - load specific subsets into memory

### Sliding Windows

Process large episodes in manageable chunks with configurable overlap:

```python
# Process episode in 10-turn windows with 2-turn overlap
for window in episode.sliding_window(window_size=10, overlap=2):
    print(f"Window: {window.size} turns")
    print(f"Time range: {window.time_range[0]/60:.1f}-{window.time_range[1]/60:.1f}min")
```

### Search Capabilities

Search podcasts and episodes by various criteria:

```python
# Search by duration, speakers, hosts, categories, etc.
episodes = sporc.search_episodes(
    min_duration=1800,  # 30+ minutes
    category="education",
    host_name="Simon Shapiro"
)
```

## Documentation

For comprehensive documentation and examples, see the [Wiki](docs/wiki/):

- **[Installation Guide](docs/wiki/Installation.md)**: Detailed setup instructions
- **[Basic Usage](docs/wiki/Basic-Usage.md)**: Simple examples to get started
- **[Search Examples](docs/wiki/Search-Examples.md)**: How to search for podcasts and episodes
- **[Conversation Analysis](docs/wiki/Conversation-Analysis.md)**: Analyzing conversation turns and patterns
- **[Sliding Windows](docs/wiki/Sliding-Windows.md)**: Process large episodes in manageable chunks
- **[Streaming Mode](docs/wiki/Streaming-Mode.md)**: Memory-efficient processing
- **[Selective Loading](docs/wiki/Selective-Loading.md)**: Filtered subset processing
- **[Lazy Loading](docs/wiki/Lazy-Loading.md)**: Efficient turn data loading
- **[API Reference](docs/wiki/API-Reference.md)**: Complete API documentation

## Performance Considerations

- **Memory Mode**: Requires 8GB+ RAM, fast access to all data
- **Streaming Mode**: Works with 4GB+ RAM, slower but memory efficient
- **Selective Mode**: Best balance for working with specific subsets

## Error Handling

The package includes comprehensive error handling:

```python
from sporc import SPORCDataset, SPORCError

try:
    sporc = SPORCDataset()
    podcast = sporc.search_podcast("Example Podcast")
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

1. Check the [documentation](docs/wiki/)
2. Search existing [issues](https://github.com/yourusername/sporc/issues)
3. Create a new issue if your problem isn't already addressed

## Acknowledgments

- Hugging Face for hosting the dataset
- The open-source community for the tools that made this package possible