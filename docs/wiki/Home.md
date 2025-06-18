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

- **[SPORCDataset](SPORCDataset)**: Main class for accessing and searching the dataset
- **[Podcast](Podcast)**: Represents a podcast with its episodes and metadata
- **[Episode](Episode)**: Represents a single episode with conversation turns
- **[Turn](Turn)**: Represents a single conversation turn

## Getting Started

- **[Installation Guide](Installation)**: Detailed setup instructions
- **[Basic Usage](Basic-Usage)**: Simple examples to get started
- **[Search Examples](Search-Examples)**: How to search for podcasts and episodes
- **[Conversation Analysis](Conversation-Analysis)**: Analyzing conversation turns and patterns

## Advanced Topics

- **[Data Quality](Data-Quality)**: Understanding and assessing data quality
- **[Advanced Analysis](Advanced-Analysis)**: Sophisticated analysis techniques
- **[Performance Tips](Performance-Tips)**: Optimizing for large datasets
- **[Troubleshooting](Troubleshooting)**: Common issues and solutions

## Examples

- **[Basic Examples](Examples)**: Simple usage examples
- **[Advanced Examples](Advanced-Examples)**: Complex analysis examples
- **[Research Use Cases](Research-Use-Cases)**: Examples for research applications

## API Reference

- **[Complete API Documentation](API-Reference)**: Full documentation of all classes and methods
- **[Data Structures](Data-Structures)**: Detailed information about data formats
- **[Error Handling](Error-Handling)**: Understanding exceptions and error messages

## Contributing

- **[Contributing Guidelines](Contributing)**: How to contribute to the project
- **[Development Setup](Development-Setup)**: Setting up for development
- **[Testing](Testing)**: Running tests and writing new ones

## Support

- **[FAQ](FAQ)**: Frequently asked questions
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

**Ready to get started?** Check out the [Installation Guide](Installation) and [Basic Usage](Basic-Usage) pages!