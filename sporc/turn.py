"""
Turn class for representing conversation turns in podcast episodes.
"""

import math
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

    # Audio features. Means were published in 1.0; the standard deviations are
    # new in 1.1 and describe how much each measure varied across the turn.
    mfcc1_sma3_mean: Optional[float] = None
    mfcc2_sma3_mean: Optional[float] = None
    mfcc3_sma3_mean: Optional[float] = None
    mfcc4_sma3_mean: Optional[float] = None
    f0_semitone_from_27_5hz_sma3nz_mean: Optional[float] = None
    f1_frequency_sma3nz_mean: Optional[float] = None
    mfcc1_sma3_stdev: Optional[float] = None
    mfcc2_sma3_stdev: Optional[float] = None
    mfcc3_sma3_stdev: Optional[float] = None
    mfcc4_sma3_stdev: Optional[float] = None
    f0_semitone_from_27_5hz_sma3nz_stdev: Optional[float] = None
    f1_frequency_sma3nz_stdev: Optional[float] = None

    # Speaker inference. Empty for episodes diarized but never run through the
    # name and role classifier, which is most of what 1.1 added.
    inferred_speaker_role: Optional[str] = None
    inferred_speaker_name: Optional[str] = None

    # Words in the turn as counted when the dataset was built. Read through the
    # word_count property, which falls back to splitting the text when the
    # dataset did not supply one.
    stored_word_count: Optional[int] = None

    # False for turns carried over from 1.0 unchanged, because the inputs
    # needed to redo the word-to-speaker matching no longer exist.
    speakers_recomputed: Optional[bool] = None

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
        # An empty speaker list is a real state, not a broken record: where
        # diarization produced no segments for an episode, the transcript comes
        # through as a single turn attributed to nobody. Rejecting it would make
        # those episodes unreadable rather than merely unattributed.
        if self.speaker is None:
            raise ValueError("Speaker list cannot be None")
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
        """
        Number of words in the turn.

        Prefers the count the dataset carries, which 1.1 added, and falls back
        to splitting the text so turns built by hand or from an older layout
        still answer.
        """
        # NaN rather than None: joining the acoustics on goes through pandas,
        # which represents a missing integer as float('nan'), and that is not
        # None. Testing for None alone let NaN through and word counts summed
        # to NaN for whole episodes.
        wc = self.stored_word_count
        if wc is not None and not (isinstance(wc, float) and math.isnan(wc)):
            return int(wc)
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
        names = [
            'mfcc1_sma3_mean', 'mfcc2_sma3_mean', 'mfcc3_sma3_mean',
            'mfcc4_sma3_mean', 'f0_semitone_from_27_5hz_sma3nz_mean',
            'f1_frequency_sma3nz_mean',
            'mfcc1_sma3_stdev', 'mfcc2_sma3_stdev', 'mfcc3_sma3_stdev',
            'mfcc4_sma3_stdev', 'f0_semitone_from_27_5hz_sma3nz_stdev',
            'f1_frequency_sma3nz_stdev',
        ]
        return {n: getattr(self, n) for n in names
                if getattr(self, n) is not None}

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