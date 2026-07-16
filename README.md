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
- **Lazy downloading**: fetch only the podcasts you touch, not the whole corpus
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
git clone https://github.com/davidjurgens/sporc.git
cd sporc
pip install -e .
```

## Data Access

The corpus is ~57 GB, partitioned by podcast. By default `SPORCDataset()`
downloads only the metadata catalogs (~195 MB) and fetches each podcast's data
the first time you touch it, so a study of ten podcasts costs megabytes, not
tens of gigabytes.

```python
from sporc import SPORCDataset

# Default: metadata now (~195 MB), per-podcast data on demand.
sporc = SPORCDataset()

# Fetch a known slice up front. Accepts podcast ids, podcast titles,
# episode ids, or a path to a .json / newline-delimited .txt file of them.
sporc = SPORCDataset(subset=["Radiolab", "99% Invisible"])
sporc = SPORCDataset(subset="my_podcast_ids.txt")

# Pin a run to exactly that slice: anything outside it raises
# DataNotLocalError instead of silently downloading more.
sporc = SPORCDataset(subset="my_podcast_ids.txt", allow_downloads=False)

# Work entirely offline from data already on disk.
sporc = SPORCDataset(allow_downloads=False)

# Download the whole corpus up front (~31 GB, ~685k files).
sporc = SPORCDataset(lazy=False)

# Use a local copy of the layout; never downloads anything.
sporc = SPORCDataset(parquet_dir="/path/to/sporc_parquet")
```

### Text search

`search_turns`, `search_episodes_by_text` and `concordance` use the DuckDB
full-text index when it is present, and otherwise scan the turn partitions on
disk. Scanning is the right choice for a subset and needs no extra dependency:

| Local data | Scan (pyarrow) | 26 GB DuckDB index |
|---|---|---|
| 250 episodes (5 MB) | 0.37s | n/a — needs the full 26 GB |
| 1,000 episodes (17 MB) | **1.1s** | " |
| 5,000 episodes (83 MB) | 5.4s | " |
| all 78M turns | impractical | 1.7s open, then ~5s/query (50s cold) |

So the index earns its 26 GB only when you need the whole corpus. Below roughly
5,000 episodes, scanning is faster than the index's cold-start alone.

```python
sporc = SPORCDataset(include_search_db=True)   # whole corpus; pip install duckdb
```

Without the index, `mode="fts"` ranks by term frequency rather than BM25, and
results cover only local data — the package warns, naming how many podcasts it
scanned.

### Building teaching subsets

`scripts/make_subset.py` cuts a self-contained mini-SPoRC. It filters the
catalogs *and* the episode partitions to match, so counts, searches and
statistics are all true of the subset — a learner never sees a podcast the
subset does not contain, nor an episode with silently missing turns:

```bash
# Ten disjoint 1k-episode subsets (~57 MB each)
for i in $(seq 1 10); do
  python scripts/make_subset.py --data-dir /path/to/sporc_parquet \
      --out subsets/subset_$i --episodes 1000 --seed $i \
      --exclude-used subsets/used.txt
done
```

Subsets are diarized-only by default, so every episode has turns. Use
`--include-undiarized` for a sample that mirrors the real corpus's ~33%
coverage. Learners then just point at the directory, and nothing downloads:

```python
sporc = SPORCDataset(parquet_dir="subsets/subset_1")
```

### Selecting a subset efficiently

The layout is partitioned by podcast, so **the podcast is the unit of transfer**.
A median podcast is ~75 KB of episodes + turns (p90 ~307 KB), which means the
one-time ~195 MB of catalogs dominates any study under a few thousand podcasts:

| Study size | Data fetched |
|---|---|
| 10 podcasts | ~750 KB |
| 200 podcasts | ~15 MB |
| 1,000 podcasts | ~75 MB |
| whole corpus | ~31 GB |

The catalogs are what make this cheap: **selection is metadata-only**. Every
query below runs off the already-downloaded catalogs and fetches nothing, so
you can narrow to exactly the episodes you want and only then pull data.

```python
sporc = SPORCDataset()

# All metadata-only -- zero partitions read:
hits = sporc.filter_episodes_by_metrics(min_word_count=5000, limit=200)
hits = sporc.search_by_speaker_name("Ira Glass", role="host")

# Then fetch only what those resolve to (~KBs per podcast).
sporc.prefetch({"episode_ids": [h["episode_id"] for h in hits]})
```

Two things worth knowing:

- **Pass `max_episodes` to `search_episodes`.** Matching is metadata-only, but
  building each `Episode` reads that podcast's partition. `category="comedy"`
  matches 62,622 episodes across 14,668 podcasts (~1.1 GB); with
  `max_episodes=10` only 10 partitions are read.
- **`filter_episodes_by_metrics` implies turn data.** `episode_metrics` is
  derived from turns, so it only covers the 372,604 diarized episodes. Filtering
  through it never wastes a fetch on an episode with no turns. To filter the
  catalog directly, `num_main_speakers > 0` marks the diarized episodes.

### Turn coverage

Only about a third of episodes have speaker-turn data; the rest have a
transcript but were never diarized. An empty `episode.turns` is therefore
usually a gap in the corpus rather than a fact about the episode, so check
before drawing conclusions from it:

```python
if episode.has_turn_data:
    analyze(episode.turns)
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