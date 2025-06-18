# SPORC Python Package

Welcome to the SPORC (Structured Podcast Open Research Corpus) Python package documentation!

## Overview

The SPORC package provides easy-to-use Python classes and functions to work with the [SPORC dataset](https://huggingface.co/datasets/blitt/SPoRC) from Hugging Face. This dataset contains a large collection of podcast episodes with rich metadata, transcripts, speaker diarization, and conversation turn analysis.

## Quick Start

```python
from sporc import SPORCDataset

# Initialize the dataset
sporc = SPORCDataset()

# Search for a podcast
podcast = sporc.search_podcast("SingOut SpeakOut")

# Get episodes
for episode in podcast.episodes:
    print(f"Episode: {episode.title}")
    print(f"Duration: {episode.duration_minutes:.1f} minutes")
    print(f"Hosts: {episode.host_names}")
```

## Key Features

- **Easy Dataset Access**: Simple interface to load and work with the SPORC dataset
- **Rich Search Capabilities**: Search podcasts and episodes by various criteria
- **Conversation Analysis**: Analyze conversation turns, speaker interactions, and content patterns
- **Data Quality Assessment**: Built-in tools to assess diarization quality
- **Comprehensive Metadata**: Access to episode metadata, speaker information, and audio features

## Installation

### Prerequisites

1. **Accept Dataset Terms**: Visit [https://huggingface.co/datasets/blitt/SPoRC](https://huggingface.co/datasets/blitt/SPoRC) and accept the terms
2. **Set up Hugging Face Authentication**: Run `huggingface-cli login`

### Install the Package

```bash
pip install sporc
```

## Core Classes

- **[SPORCDataset](SPORCDataset.md)**: Main class for accessing and searching the dataset
- **[Podcast](Podcast.md)**: Represents a podcast with its episodes and metadata
- **[Episode](Episode.md)**: Represents a single episode with conversation turns
- **[Turn](Turn.md)**: Represents a single conversation turn

## Getting Started

- **[Installation Guide](Installation.md)**: Detailed setup instructions
- **[Basic Usage](Basic-Usage.md)**: Simple examples to get started
- **[Search Examples](Search-Examples.md)**: How to search for podcasts and episodes
- **[Conversation Analysis](Conversation-Analysis.md)**: Analyzing conversation turns and patterns

## Advanced Topics

- **[Categories](Categories.md)**: Understanding and using podcast categories
- **[Streaming Mode](Streaming-Mode.md)**: Memory-efficient processing
- **[Selective Loading](Selective-Loading.md)**: Filtered subset processing

## Reference

- **[Complete API Documentation](API-Reference.md)**: Full documentation of all classes and methods

## Development

- **[Contributing Guidelines](Contributing.md)**: How to contribute to the project

## Support

- **[FAQ](FAQ.md)**: Frequently asked questions
- **[Issues](https://github.com/yourusername/sporc/issues)**: Report bugs and request features
- **[Discussions](https://github.com/yourusername/sporc/discussions)**: Community discussions

## Citation

If you use this package in your research, please cite:

```bibtex
@article{blitt2024sporc,
  title={SPORC: the Structured Podcast Open Research Corpus},
  author={Blitt, Joshua and others},
  journal={arXiv preprint arXiv:2411.07892},
  year={2024}
}
```

---

**Ready to get started?** Check out the [Installation Guide](Installation.md) and [Basic Usage](Basic-Usage.md) pages!