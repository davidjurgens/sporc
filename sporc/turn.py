"""
Turn class for representing conversation turns in podcast episodes.
"""

from typing import List, Optional, Dict, Any
from dataclasses import dataclass


@dataclass
class Turn:
    """
    Represents a single conversation turn in a podcast episode.

    A turn is a segment of speech by one or more speakers, with associated
    metadata including timing, text, and audio features.
    """

    # Core turn information
    speaker: List[str]
    text: str
    start_time: float
    end_time: float
    duration: float
    turn_count: int

    # Audio features
    mfcc1_sma3_mean: Optional[float] = None
    mfcc2_sma3_mean: Optional[float] = None
    mfcc3_sma3_mean: Optional[float] = None
    mfcc4_sma3_mean: Optional[float] = None
    f0_semitone_from_27_5hz_sma3nz_mean: Optional[float] = None
    f1_frequency_sma3nz_mean: Optional[float] = None

    # Speaker inference
    inferred_speaker_role: Optional[str] = None
    inferred_speaker_name: Optional[str] = None

    # Episode reference
    mp3_url: Optional[str] = None

    def __post_init__(self):
        """Validate turn data after initialization."""
        if self.start_time < 0:
            raise ValueError("Start time cannot be negative")
        if self.end_time < self.start_time:
            raise ValueError("End time must be after start time")
        if self.duration < 0:
            raise ValueError("Duration cannot be negative")
        if not self.speaker:
            raise ValueError("Speaker list cannot be empty")
        if not self.text.strip():
            raise ValueError("Text cannot be empty")

    @property
    def is_overlapping(self) -> bool:
        """Check if this turn involves multiple speakers (overlapping)."""
        return len(self.speaker) > 1

    @property
    def primary_speaker(self) -> str:
        """Get the primary speaker (first in the list)."""
        return self.speaker[0]

    @property
    def is_host(self) -> bool:
        """Check if the primary speaker is inferred to be a host."""
        return self.inferred_speaker_role == "host"

    @property
    def is_guest(self) -> bool:
        """Check if the primary speaker is inferred to be a guest."""
        return self.inferred_speaker_role == "guest"

    @property
    def word_count(self) -> int:
        """Get the number of words in the turn text."""
        return len(self.text.split())

    @property
    def words_per_second(self) -> float:
        """Calculate words spoken per second."""
        if self.duration == 0:
            return 0.0
        return self.word_count / self.duration

    def contains_time(self, time: float) -> bool:
        """
        Check if a given time falls within this turn.

        Args:
            time: Time in seconds to check

        Returns:
            True if the time falls within this turn's time range
        """
        return self.start_time <= time <= self.end_time

    def overlaps_with(self, other: 'Turn') -> bool:
        """
        Check if this turn overlaps with another turn in time.

        Args:
            other: Another Turn object to check against

        Returns:
            True if the turns overlap in time
        """
        return not (self.end_time <= other.start_time or other.end_time <= self.start_time)

    def get_audio_features(self) -> Dict[str, float]:
        """
        Get all available audio features as a dictionary.

        Returns:
            Dictionary of audio feature names to values
        """
        features = {}
        if self.mfcc1_sma3_mean is not None:
            features['mfcc1_sma3_mean'] = self.mfcc1_sma3_mean
        if self.mfcc2_sma3_mean is not None:
            features['mfcc2_sma3_mean'] = self.mfcc2_sma3_mean
        if self.mfcc3_sma3_mean is not None:
            features['mfcc3_sma3_mean'] = self.mfcc3_sma3_mean
        if self.mfcc4_sma3_mean is not None:
            features['mfcc4_sma3_mean'] = self.mfcc4_sma3_mean
        if self.f0_semitone_from_27_5hz_sma3nz_mean is not None:
            features['f0_semitone_from_27_5hz_sma3nz_mean'] = self.f0_semitone_from_27_5hz_sma3nz_mean
        if self.f1_frequency_sma3nz_mean is not None:
            features['f1_frequency_sma3nz_mean'] = self.f1_frequency_sma3nz_mean
        return features

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the turn to a dictionary representation.

        Returns:
            Dictionary representation of the turn
        """
        return {
            'speaker': self.speaker,
            'text': self.text,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration': self.duration,
            'turn_count': self.turn_count,
            'is_overlapping': self.is_overlapping,
            'primary_speaker': self.primary_speaker,
            'inferred_speaker_role': self.inferred_speaker_role,
            'inferred_speaker_name': self.inferred_speaker_name,
            'word_count': self.word_count,
            'words_per_second': self.words_per_second,
            'audio_features': self.get_audio_features(),
            'mp3_url': self.mp3_url,
        }

    def __str__(self) -> str:
        """String representation of the turn."""
        return f"Turn({self.primary_speaker}, {self.start_time:.1f}s-{self.end_time:.1f}s, {self.word_count} words)"

    def __repr__(self) -> str:
        """Detailed string representation of the turn."""
        return (f"Turn(speaker={self.speaker}, start_time={self.start_time}, "
                f"end_time={self.end_time}, duration={self.duration}, "
                f"text='{self.text[:50]}{'...' if len(self.text) > 50 else ''}')")