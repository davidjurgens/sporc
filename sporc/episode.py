"""
Episode class for representing podcast episodes.
"""

from typing import List, Optional, Dict, Any, Iterator, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import json
from enum import Enum

from .turn import Turn
from .exceptions import NotFoundError


class TimeRangeBehavior(Enum):
    """
    Enum for specifying behavior when selecting turns within a time range.

    - STRICT: Only include turns that are completely within the time range
    - INCLUDE_PARTIAL: Include turns that overlap with the time range (default)
    - INCLUDE_FULL_TURNS: Include complete turns even if they extend beyond the time range
    """
    STRICT = "strict"
    INCLUDE_PARTIAL = "include_partial"
    INCLUDE_FULL_TURNS = "include_full_turns"


class TurnWindow:
    """
    Represents a window of turns with metadata about the window.
    """

    def __init__(self, turns: List[Turn], window_index: int, start_index: int, end_index: int,
                 total_windows: int, overlap_size: int = 0):
        self.turns = turns
        self.window_index = window_index
        self.start_index = start_index
        self.end_index = end_index
        self.total_windows = total_windows
        self.overlap_size = overlap_size

    @property
    def size(self) -> int:
        """Get the number of turns in this window."""
        return len(self.turns)

    @property
    def is_first(self) -> bool:
        """Check if this is the first window."""
        return self.window_index == 0

    @property
    def is_last(self) -> bool:
        """Check if this is the last window."""
        return self.window_index == self.total_windows - 1

    @property
    def has_overlap(self) -> bool:
        """Check if this window has overlap with the previous window."""
        return self.overlap_size > 0

    @property
    def overlap_turns(self) -> List[Turn]:
        """Get the turns that overlap with the previous window (if any)."""
        if self.overlap_size > 0 and not self.is_first:
            return self.turns[:self.overlap_size]
        return []

    @property
    def new_turns(self) -> List[Turn]:
        """Get the turns that are new in this window (not overlapping)."""
        if self.overlap_size > 0 and not self.is_first:
            return self.turns[self.overlap_size:]
        return self.turns

    @property
    def time_range(self) -> Tuple[float, float]:
        """Get the time range covered by this window."""
        if not self.turns:
            return (0.0, 0.0)
        return (self.turns[0].start_time, self.turns[-1].end_time)

    @property
    def duration(self) -> float:
        """Get the total duration of this window."""
        if not self.turns:
            return 0.0
        return self.turns[-1].end_time - self.turns[0].start_time

    def get_speaker_distribution(self) -> Dict[str, int]:
        """Get the distribution of speakers in this window."""
        speaker_counts = {}
        for turn in self.turns:
            for speaker in turn.speaker:
                speaker_counts[speaker] = speaker_counts.get(speaker, 0) + 1
        return speaker_counts

    def get_role_distribution(self) -> Dict[str, int]:
        """Get the distribution of speaker roles in this window."""
        role_counts = {}
        for turn in self.turns:
            role = turn.inferred_speaker_role or "unknown"
            role_counts[role] = role_counts.get(role, 0) + 1
        return role_counts

    def get_text(self, separator: str = " ") -> str:
        """Get the combined text of all turns in this window."""
        return separator.join(turn.text for turn in self.turns)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the window to a dictionary representation."""
        return {
            'window_index': self.window_index,
            'start_index': self.start_index,
            'end_index': self.end_index,
            'total_windows': self.total_windows,
            'overlap_size': self.overlap_size,
            'size': self.size,
            'is_first': self.is_first,
            'is_last': self.is_last,
            'has_overlap': self.has_overlap,
            'time_range': self.time_range,
            'duration': self.duration,
            'speaker_distribution': self.get_speaker_distribution(),
            'role_distribution': self.get_role_distribution(),
            'num_turns': len(self.turns),
            'num_overlap_turns': len(self.overlap_turns),
            'num_new_turns': len(self.new_turns),
        }

    def __str__(self) -> str:
        """String representation of the window."""
        return f"TurnWindow(index={self.window_index}, turns={self.size}, time={self.time_range[0]:.1f}-{self.time_range[1]:.1f}s)"

    def __repr__(self) -> str:
        """Detailed string representation of the window."""
        return (f"TurnWindow(turns={self.size}, window_index={self.window_index}, "
                f"start_index={self.start_index}, end_index={self.end_index}, "
                f"overlap_size={self.overlap_size})")


@dataclass
class Episode:
    """
    Represents a single podcast episode with metadata and conversation turns.
    """

    # Basic episode information
    title: str
    description: str
    mp3_url: str
    duration_seconds: float
    transcript: str

    # Podcast information
    podcast_title: str
    podcast_description: str
    rss_url: str

    # Categories
    category1: Optional[str] = None
    category2: Optional[str] = None
    category3: Optional[str] = None
    category4: Optional[str] = None
    category5: Optional[str] = None
    category6: Optional[str] = None
    category7: Optional[str] = None
    category8: Optional[str] = None
    category9: Optional[str] = None
    category10: Optional[str] = None

    # Host and guest information
    host_predicted_names: List[str] = field(default_factory=list)
    guest_predicted_names: List[str] = field(default_factory=list)
    neither_predicted_names: List[str] = field(default_factory=list)

    # Speaker information
    main_ep_speakers: List[str] = field(default_factory=list)
    host_speaker_labels: Dict[str, str] = field(default_factory=dict)
    guest_speaker_labels: Dict[str, str] = field(default_factory=dict)

    # Quality indicators
    overlap_prop_duration: float = 0.0
    overlap_prop_turn_count: float = 0.0
    avg_turn_duration: float = 0.0
    total_speaker_labels: int = 0

    # Metadata
    language: str = "en"
    explicit: bool = False
    image_url: Optional[str] = None
    episode_date_localized: Optional[int] = None
    oldest_episode_date: Optional[str] = None
    last_update: Optional[int] = None
    created_on: Optional[int] = None

    # Internal data
    _turns: List[Turn] = field(default_factory=list, repr=False)
    _turns_loaded: bool = False

    def __post_init__(self):
        """Validate episode data after initialization."""
        if not self.title.strip():
            raise ValueError("Episode title cannot be empty")
        if self.duration_seconds < 0:
            raise ValueError("Duration cannot be negative")
        if not self.mp3_url.strip():
            raise ValueError("MP3 URL cannot be empty")

    @property
    def categories(self) -> List[str]:
        """Get all non-None categories for this episode."""
        categories = []
        for i in range(1, 11):
            category = getattr(self, f'category{i}')
            if category is not None:
                categories.append(category)
        return categories

    @property
    def primary_category(self) -> Optional[str]:
        """Get the primary category (category1)."""
        return self.category1

    @property
    def host_names(self) -> List[str]:
        """Get the list of predicted host names."""
        return self.host_predicted_names

    @property
    def guest_names(self) -> List[str]:
        """Get the list of predicted guest names."""
        return self.guest_predicted_names

    @property
    def num_hosts(self) -> int:
        """Get the number of unique hosts."""
        return len(self.host_names)

    @property
    def num_guests(self) -> int:
        """Get the number of unique guests."""
        return len(self.guest_names)

    @property
    def num_main_speakers(self) -> int:
        """Get the number of main speakers."""
        return len(self.main_ep_speakers)

    @property
    def duration_minutes(self) -> float:
        """Get episode duration in minutes."""
        return self.duration_seconds / 60.0

    @property
    def duration_hours(self) -> float:
        """Get episode duration in hours."""
        return self.duration_seconds / 3600.0

    @property
    def episode_date(self) -> Optional[datetime]:
        """Get the episode date as a datetime object."""
        if self.episode_date_localized is None:
            return None

        try:
            # Handle case where episode_date_localized might be a string
            if isinstance(self.episode_date_localized, str):
                # Try to convert string to int/float
                try:
                    timestamp = float(self.episode_date_localized) if self.episode_date_localized else None
                except (ValueError, TypeError):
                    # If conversion fails, return None
                    return None
            else:
                timestamp = self.episode_date_localized

            # If timestamp is None, return None
            if timestamp is None:
                return None

            # Convert from milliseconds to seconds and create datetime
            return datetime.fromtimestamp(timestamp / 1000)
        except (ValueError, TypeError, OSError):
            # Handle any other conversion errors
            return None

    @property
    def is_long_form(self) -> bool:
        """Check if this is a long-form episode (>30 minutes)."""
        return self.duration_minutes > 30

    @property
    def is_short_form(self) -> bool:
        """Check if this is a short-form episode (<10 minutes)."""
        return self.duration_minutes < 10

    @property
    def has_guests(self) -> bool:
        """Check if this episode has guests."""
        return len(self.guest_names) > 0

    @property
    def is_solo(self) -> bool:
        """Check if this is a solo episode (single host, no guests)."""
        return self.num_hosts == 1 and self.num_guests == 0

    @property
    def is_interview(self) -> bool:
        """Check if this is an interview episode (host + guest)."""
        return self.num_hosts >= 1 and self.num_guests >= 1

    @property
    def is_panel(self) -> bool:
        """Check if this is a panel episode (multiple hosts/guests)."""
        return (self.num_hosts + self.num_guests) > 2

    def get_turns_by_time_range(self, start_time: float, end_time: float,
                               behavior: TimeRangeBehavior = TimeRangeBehavior.INCLUDE_PARTIAL) -> List[Turn]:
        """
        Get all turns that fall within a specific time range.

        Args:
            start_time: Start time in seconds
            end_time: End time in seconds
            behavior: How to handle turns that are partially within the time range
                - STRICT: Only include turns that are completely within the time range
                - INCLUDE_PARTIAL: Include turns that overlap with the time range (default)
                - INCLUDE_FULL_TURNS: Include complete turns even if they extend beyond the time range

        Returns:
            List of Turn objects within the time range according to the specified behavior
        """
        if not self._turns_loaded:
            raise RuntimeError("Turns not loaded. Call load_turns() first.")

        if start_time < 0:
            start_time = 0
        if end_time > self.duration_seconds:
            end_time = self.duration_seconds

        if behavior == TimeRangeBehavior.STRICT:
            # Only include turns that are completely within the time range
            return [
                turn for turn in self._turns
                if turn.start_time >= start_time and turn.end_time <= end_time
            ]
        elif behavior == TimeRangeBehavior.INCLUDE_PARTIAL:
            # Include turns that overlap with the time range (current behavior)
            return [
                turn for turn in self._turns
                if turn.overlaps_with(Turn(
                    speaker=["dummy"],
                    text="dummy text for overlap checking",
                    start_time=start_time,
                    end_time=end_time,
                    duration=end_time - start_time,
                    turn_count=0
                ))
            ]
        elif behavior == TimeRangeBehavior.INCLUDE_FULL_TURNS:
            # Include complete turns even if they extend beyond the time range
            # Find turns that start before the end time and end after the start time
            return [
                turn for turn in self._turns
                if turn.start_time < end_time and turn.end_time > start_time
            ]
        else:
            raise ValueError(f"Unknown behavior: {behavior}")

    def get_turns_by_time_range_with_trimming(self, start_time: float, end_time: float,
                                             behavior: TimeRangeBehavior = TimeRangeBehavior.INCLUDE_PARTIAL) -> List[Dict[str, Any]]:
        """
        Get turns within a time range with optional text trimming.

        This method returns turns with additional metadata about how the text
        was trimmed to fit within the specified time range.

        Args:
            start_time: Start time in seconds
            end_time: End time in seconds
            behavior: How to handle turns that are partially within the time range
                - STRICT: Only include turns that are completely within the time range
                - INCLUDE_PARTIAL: Include turns that overlap with the time range (default)
                - INCLUDE_FULL_TURNS: Include complete turns even if they extend beyond the time range

        Returns:
            List of dictionaries containing turn data with trimming information:
            {
                'turn': Turn object,
                'trimmed_text': str,  # Text trimmed to time range (if applicable)
                'original_text': str,  # Original turn text
                'trimmed_start': float,  # Start time within the turn (if trimmed)
                'trimmed_end': float,    # End time within the turn (if trimmed)
                'was_trimmed': bool      # Whether the text was trimmed
            }
        """
        if not self._turns_loaded:
            raise RuntimeError("Turns not loaded. Call load_turns() first.")

        if start_time < 0:
            start_time = 0
        if end_time > self.duration_seconds:
            end_time = self.duration_seconds

        turns = self.get_turns_by_time_range(start_time, end_time, behavior)
        result = []

        for turn in turns:
            turn_data = {
                'turn': turn,
                'trimmed_text': turn.text,
                'original_text': turn.text,
                'trimmed_start': turn.start_time,
                'trimmed_end': turn.end_time,
                'was_trimmed': False
            }

            # For STRICT behavior, we might want to trim text to the exact time range
            if behavior == TimeRangeBehavior.STRICT:
                # The turn should be completely within the range, so no trimming needed
                pass
            elif behavior == TimeRangeBehavior.INCLUDE_PARTIAL:
                # For partial inclusion, we could trim text to the time range
                # This is a simplified approach - in practice, you might want more sophisticated text trimming
                if turn.start_time < start_time or turn.end_time > end_time:
                    turn_data['was_trimmed'] = True
                    turn_data['trimmed_start'] = max(turn.start_time, start_time)
                    turn_data['trimmed_end'] = min(turn.end_time, end_time)
                    # Note: Actual text trimming would require word-level timing data
                    # This is a placeholder for the concept
            elif behavior == TimeRangeBehavior.INCLUDE_FULL_TURNS:
                # Include the full turn, no trimming
                pass

            result.append(turn_data)

        return result

    def get_turns_by_speaker(self, speaker_name: str) -> List[Turn]:
        """
        Get all turns by a specific speaker.

        Args:
            speaker_name: Speaker label (e.g., "SPEAKER_00") or inferred name

        Returns:
            List of Turn objects by the specified speaker
        """
        if not self._turns_loaded:
            raise RuntimeError("Turns not loaded. Call load_turns() first.")

        return [
            turn for turn in self._turns
            if (speaker_name in turn.speaker or
                turn.inferred_speaker_name == speaker_name)
        ]

    def get_turns_by_min_length(self, min_length: int) -> List[Turn]:
        """
        Get all turns with at least the specified number of words.

        Args:
            min_length: Minimum number of words required

        Returns:
            List of Turn objects with at least min_length words
        """
        if not self._turns_loaded:
            raise RuntimeError("Turns not loaded. Call load_turns() first.")

        return [turn for turn in self._turns if turn.word_count >= min_length]

    def get_turns_by_role(self, role: str) -> List[Turn]:
        """
        Get all turns by speakers with a specific role.

        Args:
            role: Speaker role ("host", "guest", etc.)

        Returns:
            List of Turn objects by speakers with the specified role
        """
        if not self._turns_loaded:
            raise RuntimeError("Turns not loaded. Call load_turns() first.")

        return [
            turn for turn in self._turns
            if turn.inferred_speaker_role == role
        ]

    def get_host_turns(self) -> List[Turn]:
        """Get all turns by hosts."""
        return self.get_turns_by_role("host")

    def get_guest_turns(self) -> List[Turn]:
        """Get all turns by guests."""
        return self.get_turns_by_role("guest")

    def sliding_window(self, window_size: int, overlap: int = 0,
                      start_index: Optional[int] = None, end_index: Optional[int] = None) -> Iterator[TurnWindow]:
        """
        Create a sliding window iterator over turns.

        Args:
            window_size: Number of turns in each window
            overlap: Number of turns to overlap between consecutive windows
            start_index: Starting turn index (default: 0)
            end_index: Ending turn index (default: last turn)

        Yields:
            TurnWindow objects containing the turns for each window

        Raises:
            RuntimeError: If turns are not loaded
            ValueError: If window_size is less than or equal to overlap
        """
        if not self._turns_loaded:
            raise RuntimeError("Turns not loaded. Call load_turns() first.")

        if window_size <= overlap:
            raise ValueError("Window size must be greater than overlap")

        if window_size <= 0:
            raise ValueError("Window size must be positive")

        if overlap < 0:
            raise ValueError("Overlap must be non-negative")

        # Set default start and end indices
        if start_index is None:
            start_index = 0
        if end_index is None:
            end_index = len(self._turns)

        # Validate indices
        if start_index < 0 or start_index >= len(self._turns):
            raise ValueError(f"Start index {start_index} is out of range")
        if end_index > len(self._turns) or end_index <= start_index:
            raise ValueError(f"End index {end_index} is invalid")

        # Calculate step size (how many new turns to add in each window)
        step_size = window_size - overlap

        # Calculate total number of windows
        total_turns = end_index - start_index
        if total_turns <= 0:
            return

        # Calculate how many complete windows we can fit
        if total_turns <= window_size:
            total_windows = 1
        else:
            total_windows = (total_turns - window_size) // step_size + 1

        # Generate windows
        for window_index in range(total_windows):
            window_start = start_index + window_index * step_size
            window_end = min(window_start + window_size, end_index)

            # Get turns for this window
            window_turns = self._turns[window_start:window_end]

            # Create TurnWindow object
            window = TurnWindow(
                turns=window_turns,
                window_index=window_index,
                start_index=window_start,
                end_index=window_end,
                total_windows=total_windows,
                overlap_size=overlap if window_index > 0 else 0
            )

            yield window

    def sliding_window_by_time(self, window_duration: float, overlap_duration: float = 0.0,
                              start_time: Optional[float] = None, end_time: Optional[float] = None) -> Iterator[TurnWindow]:
        """
        Create a sliding window iterator over turns based on time duration.

        Args:
            window_duration: Duration of each window in seconds
            overlap_duration: Duration of overlap between consecutive windows in seconds
            start_time: Starting time in seconds (default: 0)
            end_time: Ending time in seconds (default: episode duration)

        Yields:
            TurnWindow objects containing the turns for each time window

        Raises:
            RuntimeError: If turns are not loaded
            ValueError: If window_duration is less than or equal to overlap_duration
        """
        if not self._turns_loaded:
            raise RuntimeError("Turns not loaded. Call load_turns() first.")

        if window_duration <= overlap_duration:
            raise ValueError("Window duration must be greater than overlap duration")

        if window_duration <= 0:
            raise ValueError("Window duration must be positive")

        if overlap_duration < 0:
            raise ValueError("Overlap duration must be non-negative")

        # Set default start and end times
        if start_time is None:
            start_time = 0.0
        if end_time is None:
            end_time = self.duration_seconds

        # Validate times
        if start_time < 0 or start_time >= self.duration_seconds:
            raise ValueError(f"Start time {start_time} is out of range")
        if end_time > self.duration_seconds or end_time <= start_time:
            raise ValueError(f"End time {end_time} is invalid")

        # Calculate step size (how much time to advance in each window)
        step_size = window_duration - overlap_duration

        # Calculate total number of windows
        total_duration = end_time - start_time
        if total_duration <= 0:
            return

        total_windows = max(1, int((total_duration - window_duration) / step_size) + 1)

        # Generate windows
        for window_index in range(total_windows):
            window_start_time = start_time + window_index * step_size
            window_end_time = min(window_start_time + window_duration, end_time)

            # Find turns that fall within this time window
            window_turns = []
            for turn in self._turns:
                # Include turns that overlap with the time window
                if (turn.start_time < window_end_time and turn.end_time > window_start_time):
                    window_turns.append(turn)

            # Find the actual start and end indices for this window
            if window_turns:
                # Find the first turn that starts within or before the window
                start_idx = None
                for i, turn in enumerate(self._turns):
                    if turn.start_time >= window_start_time:
                        start_idx = i
                        break
                if start_idx is None:
                    start_idx = len(self._turns) - 1

                # Find the last turn that ends within or after the window
                end_idx = None
                for i in range(len(self._turns) - 1, -1, -1):
                    if self._turns[i].end_time <= window_end_time:
                        end_idx = i + 1
                        break
                if end_idx is None:
                    end_idx = len(self._turns)
            else:
                start_idx = end_idx = 0

            # Create TurnWindow object
            window = TurnWindow(
                turns=window_turns,
                window_index=window_index,
                start_index=start_idx,
                end_index=end_idx,
                total_windows=total_windows,
                overlap_size=0  # Time-based windows don't track turn overlap directly
            )

            yield window

    def get_window_statistics(self, window_size: int, overlap: int = 0) -> Dict[str, Any]:
        """
        Get statistics about sliding windows for this episode.

        Args:
            window_size: Number of turns in each window
            overlap: Number of turns to overlap between consecutive windows

        Returns:
            Dictionary with window statistics
        """
        if not self._turns_loaded:
            raise RuntimeError("Turns not loaded. Call load_turns() first.")

        if window_size <= overlap:
            raise ValueError("Window size must be greater than overlap")

        # Calculate statistics
        total_turns = len(self._turns)
        step_size = window_size - overlap
        total_windows = max(1, (total_turns - window_size) // step_size + 1)

        # Calculate average window duration
        total_duration = sum(turn.duration for turn in self._turns)
        avg_turn_duration = total_duration / total_turns if total_turns > 0 else 0
        avg_window_duration = avg_turn_duration * window_size

        return {
            'total_turns': total_turns,
            'window_size': window_size,
            'overlap': overlap,
            'step_size': step_size,
            'total_windows': total_windows,
            'avg_window_duration': avg_window_duration,
            'total_duration': total_duration,
            'avg_turn_duration': avg_turn_duration,
        }

    @property
    def turns(self) -> List[Turn]:
        """
        Get all turns for this episode, loading them on-demand if necessary.

        Returns:
            List of all Turn objects for this episode
        """
        if not self._turns_loaded:
            raise RuntimeError("Turns not loaded. Call load_turns() first.")
        return self._turns.copy()

    @property
    def turn_count(self) -> int:
        """
        Get the number of turns in this episode, loading them on-demand if necessary.

        Returns:
            Number of turns in this episode
        """
        if not self._turns_loaded:
            raise RuntimeError("Turns not loaded. Call load_turns() first.")
        return len(self._turns)

    @property
    def has_turns(self) -> bool:
        """
        Check if this episode has turns loaded or available.

        Returns:
            True if turns are loaded or can be loaded, False otherwise
        """
        if self._turns_loaded:
            return len(self._turns) > 0
        return False

    def get_all_turns(self) -> List[Turn]:
        """
        Get all turns for this episode.

        Returns:
            List of all Turn objects for this episode
        """
        if not self._turns_loaded:
            raise RuntimeError("Turns not loaded. Call load_turns() first.")
        return self._turns.copy()

    def get_turn_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the turns in this episode.

        Returns:
            Dictionary with turn statistics
        """
        if not self._turns_loaded:
            raise RuntimeError("Turns not loaded. Call load_turns() first.")

        if not self._turns:
            return {
                'total_turns': 0,
                'total_words': 0,
                'avg_turn_duration': 0.0,
                'avg_words_per_turn': 0.0,
                'speaker_distribution': {},
                'role_distribution': {},
            }

        total_words = sum(turn.word_count for turn in self._turns)
        total_duration = sum(turn.duration for turn in self._turns)

        # Speaker distribution
        speaker_counts = {}
        for turn in self._turns:
            for speaker in turn.speaker:
                speaker_counts[speaker] = speaker_counts.get(speaker, 0) + 1

        # Role distribution
        role_counts = {}
        for turn in self._turns:
            role = turn.inferred_speaker_role or "unknown"
            role_counts[role] = role_counts.get(role, 0) + 1

        return {
            'total_turns': len(self._turns),
            'total_words': total_words,
            'avg_turn_duration': total_duration / len(self._turns),
            'avg_words_per_turn': total_words / len(self._turns),
            'speaker_distribution': speaker_counts,
            'role_distribution': role_counts,
        }

    def load_turns(self, turns_data: List[Dict[str, Any]]) -> None:
        """
        Load turn data for this episode.

        Args:
            turns_data: List of turn dictionaries from the speaker turn dataset
        """
        self._turns = []
        for turn_data in turns_data:
            if turn_data.get('mp3url') == self.mp3_url:
                turn = Turn(
                    speaker=turn_data.get('speaker', []),
                    text=turn_data.get('turnText', ''),
                    start_time=turn_data.get('startTime', 0.0),
                    end_time=turn_data.get('endTime', 0.0),
                    duration=turn_data.get('duration', 0.0),
                    turn_count=turn_data.get('turnCount', 0),
                    mfcc1_sma3_mean=turn_data.get('mfcc1_sma3Mean'),
                    mfcc2_sma3_mean=turn_data.get('mfcc2_sma3Mean'),
                    mfcc3_sma3_mean=turn_data.get('mfcc3_sma3Mean'),
                    mfcc4_sma3_mean=turn_data.get('mfcc4_sma3Mean'),
                    f0_semitone_from_27_5hz_sma3nz_mean=turn_data.get('F0semitoneFrom27.5Hz_sma3nzMean'),
                    f1_frequency_sma3nz_mean=turn_data.get('F1frequency_sma3nzMean'),
                    inferred_speaker_role=turn_data.get('inferredSpeakerRole'),
                    inferred_speaker_name=turn_data.get('inferredSpeakerName'),
                    mp3_url=turn_data.get('mp3url'),
                )
                self._turns.append(turn)

        # Sort turns by start time
        self._turns.sort(key=lambda x: x.start_time)
        self._turns_loaded = True

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the episode to a dictionary representation.

        Returns:
            Dictionary representation of the episode
        """
        return {
            'title': self.title,
            'description': self.description,
            'mp3_url': self.mp3_url,
            'duration_seconds': self.duration_seconds,
            'duration_minutes': self.duration_minutes,
            'podcast_title': self.podcast_title,
            'categories': self.categories,
            'host_names': self.host_names,
            'guest_names': self.guest_names,
            'num_hosts': self.num_hosts,
            'num_guests': self.num_guests,
            'num_main_speakers': self.num_main_speakers,
            'is_long_form': self.is_long_form,
            'is_short_form': self.is_short_form,
            'has_guests': self.has_guests,
            'is_solo': self.is_solo,
            'is_interview': self.is_interview,
            'is_panel': self.is_panel,
            'episode_date': self.episode_date.isoformat() if self.episode_date else None,
            'quality_indicators': {
                'overlap_prop_duration': self.overlap_prop_duration,
                'overlap_prop_turn_count': self.overlap_prop_turn_count,
                'avg_turn_duration': self.avg_turn_duration,
                'total_speaker_labels': self.total_speaker_labels,
            },
            'turns_loaded': self._turns_loaded,
            'num_turns': len(self._turns) if self._turns_loaded else 0,
        }

    def __str__(self) -> str:
        """String representation of the episode."""
        return f"Episode('{self.title}', {self.duration_minutes:.1f}min, {self.num_main_speakers} speakers)"

    def __repr__(self) -> str:
        """Detailed string representation of the episode."""
        return (f"Episode(title='{self.title}', duration_seconds={self.duration_seconds}, "
                f"podcast_title='{self.podcast_title}', num_hosts={self.num_hosts}, "
                f"num_guests={self.num_guests})")

    def __len__(self) -> int:
        """Return the number of turns in this episode."""
        if not self._turns_loaded:
            raise RuntimeError("Turns not loaded. Call load_turns() first.")
        return len(self._turns)

    def __getitem__(self, index: int) -> Turn:
        """Get a turn by index."""
        if not self._turns_loaded:
            raise RuntimeError("Turns not loaded. Call load_turns() first.")
        return self._turns[index]

    def __iter__(self):
        """Iterate over turns in this episode."""
        if not self._turns_loaded:
            raise RuntimeError("Turns not loaded. Call load_turns() first.")
        return iter(self._turns)