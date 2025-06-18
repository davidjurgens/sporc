# API Reference

This document provides a complete reference for all classes, methods, and properties in the SPORC package.

## SPORCDataset

The main class for interacting with the SPORC dataset.

### Constructor

```python
SPORCDataset(streaming=False, cache_dir=None, token=None)
```

**Parameters:**
- `streaming` (bool, optional): If `True`, uses streaming mode. Default is `False`.
- `cache_dir` (str, optional): Directory to cache the dataset. Default is Hugging Face cache directory.
- `token` (str, optional): Hugging Face token for authentication. If not provided, uses cached credentials.

### Methods

#### `search_podcast(name: str) -> Optional[Podcast]`

Find a podcast by exact name.

**Parameters:**
- `name` (str): Exact podcast name to search for

**Returns:**
- `Podcast` object if found, `None` if not found

**Raises:**
- `SPORCError`: If search fails or multiple matches found

#### `search_episodes(**criteria) -> List[Episode]`

Search for episodes matching specific criteria.

**Parameters:**
- `min_duration` (int, optional): Minimum episode duration in seconds
- `max_duration` (int, optional): Maximum episode duration in seconds
- `min_speakers` (int, optional): Minimum number of speakers
- `max_speakers` (int, optional): Maximum number of speakers
- `host_name` (str or List[str], optional): Host name(s) to search for
- `guest_name` (str or List[str], optional): Guest name(s) to search for
- `category` (str or List[str], optional): Podcast category(ies)
- `language` (str or List[str], optional): Language code(s)
- `min_total_duration` (float, optional): Minimum total podcast duration in hours
- `max_overlap_prop_duration` (float, optional): Maximum overlap proportion by duration
- `max_overlap_prop_turn_count` (float, optional): Maximum overlap proportion by turn count

**Returns:**
- `List[Episode]`: List of matching episodes

#### `get_all_podcasts() -> List[Podcast]`

Get all podcasts in the dataset.

**Returns:**
- `List[Podcast]`: All podcasts in the dataset

**Note:** In streaming mode, this requires iterating through the entire dataset.

#### `get_all_episodes() -> List[Episode]`

Get all episodes in the dataset.

**Returns:**
- `List[Episode]`: All episodes in the dataset

**Note:** In streaming mode, this requires iterating through the entire dataset.

#### `iterate_podcasts() -> Iterator[Podcast]`

Iterate over podcasts without loading them all into memory.

**Returns:**
- `Iterator[Podcast]`: Iterator over podcasts

**Note:** Only available in streaming mode.

#### `iterate_episodes() -> Iterator[Episode]`

Iterate over episodes without loading them all into memory.

**Returns:**
- `Iterator[Episode]`: Iterator over episodes

**Note:** Only available in streaming mode.

#### `load_podcast_subset(**criteria) -> None`

Load a filtered subset of podcasts into memory for fast access.

**Parameters:**
- `categories` (List[str], optional): List of podcast categories to include
- `hosts` (List[str], optional): List of host names to include
- `min_episodes` (int, optional): Minimum number of episodes per podcast
- `min_total_duration` (float, optional): Minimum total podcast duration in hours
- `language` (str, optional): Language code to filter by
- `max_podcasts` (int, optional): Maximum number of podcasts to load

**Note:** Only available in streaming mode. This method modifies the dataset object.

#### `get_dataset_statistics() -> Dict[str, Any]`

Get comprehensive statistics about the dataset.

**Returns:**
- `Dict[str, Any]`: Dictionary containing various dataset statistics

### Properties

#### `streaming: bool`

Check if the dataset is in streaming mode.

### Special Methods

#### `len(sporc) -> int`

Get the number of episodes in the dataset.

**Note:** In streaming mode, this raises a `RuntimeError` unless a subset has been loaded.

## Podcast

Represents a podcast with its episodes and metadata.

### Properties

#### `title: str`

The title of the podcast.

#### `description: str`

The description of the podcast.

#### `category: str`

The primary category of the podcast.

#### `language: str`

The language of the podcast (e.g., "en", "es").

#### `episodes: List[Episode]`

List of all episodes belonging to this podcast.

#### `episode_count: int`

The number of episodes in this podcast.

#### `host_names: List[str]`

List of predicted host names for this podcast.

#### `main_speakers: List[str]`

List of main speaker labels used in this podcast.

#### `total_duration_seconds: float`

Total duration of all episodes in seconds.

#### `total_duration_hours: float`

Total duration of all episodes in hours.

#### `average_episode_duration_minutes: float`

Average duration of episodes in minutes.

#### `overlap_prop_duration: float`

Proportion of overlapping speech by duration across all episodes.

#### `overlap_prop_turn_count: float`

Proportion of overlapping speech by turn count across all episodes.

#### `avg_turn_duration: float`

Average turn duration across all episodes.

### Methods

#### `get_episode_by_title(title: str) -> Optional[Episode]`

Find a specific episode by title.

**Parameters:**
- `title` (str): Exact episode title to search for

**Returns:**
- `Episode` object if found, `None` if not found

#### `get_episodes_by_duration(min_duration: float = None, max_duration: float = None) -> List[Episode]`

Get episodes within a specific duration range.

**Parameters:**
- `min_duration` (float, optional): Minimum duration in seconds
- `max_duration` (float, optional): Maximum duration in seconds

**Returns:**
- `List[Episode]`: Episodes matching the duration criteria

#### `get_episodes_by_speaker_count(min_speakers: int = None, max_speakers: int = None) -> List[Episode]`

Get episodes with a specific number of speakers.

**Parameters:**
- `min_speakers` (int, optional): Minimum number of speakers
- `max_speakers` (int, optional): Maximum number of speakers

**Returns:**
- `List[Episode]`: Episodes matching the speaker count criteria

#### `get_statistics() -> Dict[str, Any]`

Get comprehensive statistics about this podcast.

**Returns:**
- `Dict[str, Any]`: Dictionary containing podcast statistics

#### `get_all_turns() -> List[Turn]`

Get all conversation turns from all episodes.

**Returns:**
- `List[Turn]`: All conversation turns from all episodes

#### `get_turns_by_speaker(speaker_name: str) -> List[Turn]`

Get all turns by a specific speaker across all episodes.

**Parameters:**
- `speaker_name` (str): Speaker label to search for

**Returns:**
- `List[Turn]`: All turns by the specified speaker

#### `get_turns_by_time_range(start_time: float, end_time: float) -> List[Turn]`

Get all turns within a specific time range across all episodes.

**Parameters:**
- `start_time` (float): Start time in seconds
- `end_time` (float): End time in seconds

**Returns:**
- `List[Turn]`: All turns within the specified time range

### Special Methods

#### `len(podcast) -> int`

Get the number of episodes in the podcast.

## Episode

Represents a single podcast episode with its metadata, transcript, and conversation turns.

### Properties

#### `title: str`

The title of the episode.

#### `description: str`

The description of the episode.

#### `duration_seconds: float`

The duration of the episode in seconds.

#### `duration_minutes: float`

The duration of the episode in minutes.

#### `duration_hours: float`

The duration of the episode in hours.

#### `host_names: List[str]`

List of predicted host names for this episode.

#### `guest_names: List[str]`

List of predicted guest names for this episode.

#### `main_speakers: List[str]`

List of main speaker labels used in this episode.

#### `speaker_count: int`

The number of speakers in this episode.

#### `transcript: str`

The full transcript of the episode.

#### `word_count: int`

The number of words in the transcript.

#### `overlap_prop_duration: float`

Proportion of overlapping speech by duration.

#### `overlap_prop_turn_count: float`

Proportion of overlapping speech by turn count.

#### `avg_turn_duration: float`

Average turn duration in seconds.

#### `total_speaker_labels: int`

Total number of speaker labels in the episode.

### Methods

#### `get_all_turns() -> List[Turn]`

Get all conversation turns in the episode.

**Returns:**
- `List[Turn]`: All conversation turns in the episode

#### `get_turns_by_speaker(speaker_name: str) -> List[Turn]`

Get all turns by a specific speaker.

**Parameters:**
- `speaker_name` (str): Speaker label to search for

**Returns:**
- `List[Turn]`: All turns by the specified speaker

#### `get_turns_by_time_range(start_time: float, end_time: float) -> List[Turn]`

Get all turns within a specific time range.

**Parameters:**
- `start_time` (float): Start time in seconds
- `end_time` (float): End time in seconds

**Returns:**
- `List[Turn]`: All turns within the specified time range

#### `get_turns_by_min_length(min_length: int) -> List[Turn]`

Get turns longer than a specified minimum length.

**Parameters:**
- `min_length` (int): Minimum turn length in seconds

**Returns:**
- `List[Turn]`: All turns longer than the specified minimum

#### `get_speaker_statistics() -> Dict[str, Any]`

Get statistics about speaker participation.

**Returns:**
- `Dict[str, Any]`: Dictionary containing speaker statistics

#### `get_conversation_flow() -> List[Dict[str, Any]]`

Get the conversation flow with timing information.

**Returns:**
- `List[Dict[str, Any]]`: List of conversation segments with timing

#### `get_speaker_transitions() -> List[Tuple[str, str]]`

Get the sequence of speaker transitions.

**Returns:**
- `List[Tuple[str, str]]`: List of speaker transition pairs

#### `get_quality_metrics() -> Dict[str, float]`

Get comprehensive quality metrics for the episode.

**Returns:**
- `Dict[str, float]`: Dictionary containing quality metrics

## Turn

Represents a single conversation turn in a podcast episode.

### Properties

#### `speaker: str`

The speaker label for this turn (e.g., "SPEAKER_00", "SPEAKER_01").

#### `text: str`

The spoken text in this turn.

#### `start_time: float`

The start time of the turn in seconds from the beginning of the episode.

#### `end_time: float`

The end time of the turn in seconds from the beginning of the episode.

#### `duration: float`

The duration of the turn in seconds.

#### `inferred_role: str`

The inferred role of the speaker (e.g., "host", "guest", "co-host").

#### `inferred_name: str`

The inferred name of the speaker.

#### `word_count: int`

The number of words in the turn.

#### `words_per_minute: float`

The speaking rate in words per minute.

### Methods

#### `get_keywords() -> List[str]`

Extract keywords from the turn text.

**Returns:**
- `List[str]`: List of extracted keywords

#### `get_sentiment() -> Dict[str, float]`

Analyze the sentiment of the turn text.

**Returns:**
- `Dict[str, float]`: Dictionary with sentiment scores

#### `get_topics() -> List[str]`

Extract topics from the turn text.

**Returns:**
- `List[str]`: List of extracted topics

#### `format_timestamp() -> str`

Format the turn timing as a readable timestamp.

**Returns:**
- `str`: Formatted timestamp string

#### `format_duration() -> str`

Format the duration in a human-readable format.

**Returns:**
- `str`: Formatted duration string

#### `is_valid() -> bool`

Check if the turn data is valid and complete.

**Returns:**
- `bool`: True if turn data is valid, False otherwise

## Exceptions

### SPORCError

Base exception class for SPORC-specific errors.

**Attributes:**
- `message` (str): Error message
- `code` (str, optional): Error code

### DatasetAccessError

Raised when there are issues accessing the dataset.

**Inherits from:** `SPORCError`

### AuthenticationError

Raised when authentication fails.

**Inherits from:** `SPORCError`

### SearchError

Raised when search operations fail.

**Inherits from:** `SPORCError`

## Data Structures

### Episode Statistics

```python
{
    'total_speakers': int,
    'total_turns': int,
    'avg_turn_duration': float,
    'speaker_participation': {
        'speaker_label': {
            'turn_count': int,
            'total_duration': float,
            'avg_duration': float,
            'percentage': float
        }
    }
}
```

### Quality Metrics

```python
{
    'overlap_prop_duration': float,
    'overlap_prop_turn_count': float,
    'avg_turn_duration': float,
    'turn_count': int,
    'word_count': int,
    'words_per_minute': float
}
```

### Conversation Flow Segment

```python
{
    'speaker': str,
    'start_time': float,
    'end_time': float,
    'text': str,
    'duration': float
}
```

### Dataset Statistics

```python
{
    'total_podcasts': int,
    'total_episodes': int,
    'total_duration_hours': float,
    'avg_episode_duration_minutes': float,
    'categories': List[str],
    'languages': List[str],
    'speaker_count_distribution': Dict[str, int],
    'duration_distribution': Dict[str, int]
}
```

## Constants

### Supported Languages

- `"en"`: English
- `"es"`: Spanish
- `"fr"`: French
- `"de"`: German
- `"it"`: Italian
- `"pt"`: Portuguese
- `"nl"`: Dutch
- `"sv"`: Swedish
- `"no"`: Norwegian
- `"da"`: Danish

### Podcast Categories

- `"education"`: Educational content
- `"science"`: Science and technology
- `"news"`: News and current events
- `"music"`: Music and entertainment
- `"comedy"`: Comedy and humor
- `"business"`: Business and finance
- `"health"`: Health and wellness
- `"sports"`: Sports and athletics
- `"politics"`: Politics and government
- `"religion"`: Religion and spirituality

### Quality Thresholds

- `EXCELLENT_OVERLAP`: 0.05 (5% overlap)
- `GOOD_OVERLAP`: 0.1 (10% overlap)
- `MODERATE_OVERLAP`: 0.2 (20% overlap)
- `POOR_OVERLAP`: 0.3 (30% overlap)

## Type Hints

```python
from typing import List, Dict, Optional, Iterator, Tuple, Any

# Common type aliases
SpeakerLabel = str
EpisodeTitle = str
PodcastTitle = str
Duration = float
TimeRange = Tuple[float, float]
SearchCriteria = Dict[str, Any]
```

## Performance Notes

1. **Memory Mode**: All data loaded into memory, fast access, high memory usage
2. **Streaming Mode**: Data loaded on-demand, slower access, low memory usage
3. **Selective Mode**: Filtered subset loaded into memory, balanced performance
4. **Search Operations**: O(1) in memory mode, O(n) in streaming mode
5. **Turn Access**: O(n) where n is the number of turns
6. **Statistics Calculation**: Cached for better performance in memory mode