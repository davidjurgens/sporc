"""
Turn class for representing conversation turns in podcast episodes.
"""

import math
from typing import List, Optional, Dict, Any
from dataclasses import dataclass

from .constants import PLACEHOLDER_SPEAKERS


@dataclass
class Turn:
    """
    Represents a single conversation turn in a podcast episode.

    A turn is a segment of speech by one or more speakers, with associated
    metadata including timing, text, and audio features.

    **Sentinels, not nulls.** :attr:`inferred_speaker_name` and
    :attr:`inferred_speaker_role` never hold null for a turn that was diarized
    but not attributed. They hold the strings ``NO_INFERRED_SPEAKER`` and
    ``NO_INFERRED_ROLE``, which are 81-90% of turns depending on the slice. So
    ``dropna()`` removes nothing, ``IS NOT NULL`` keeps every placeholder row,
    and grouping by speaker pools hundreds of different people into one
    "speaker" unless you filter them out. Use :attr:`has_inferred_speaker` and
    :attr:`has_inferred_role`, or :data:`sporc.PLACEHOLDER_SPEAKERS`, rather
    than writing the literal -- guessing it is how a filter comes to never fire.

    **Speakers are a list.** :attr:`speaker` holds every diarization label
    active in the turn, and about 45% of turns have more than one. It can also
    be empty. See :attr:`is_overlapping` and :attr:`primary_speaker`.
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

    # Speaker inference. These carry the sentinels NO_INFERRED_ROLE and
    # NO_INFERRED_SPEAKER, not null, for the ~81-90% of turns that were diarized
    # but never resolved to a person -- see the class docstring, and prefer
    # has_inferred_role / has_inferred_speaker to testing them by hand.
    inferred_speaker_role: Optional[str] = None
    inferred_speaker_name: Optional[str] = None

    # Timestamped tokens the transcript aligned to this turn. Read through the
    # token_count property. This is NOT word_count: the transcript tokenises
    # punctuation separately, so it runs about 21% higher than counting words,
    # and it is None for turns carried over from dataset 1.0.
    stored_token_count: Optional[int] = None

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
        """
        Whether more than one speaker is active in this turn.

        True for about 45% of turns in the corpus, so this is the common case
        rather than an edge case. Any within-speaker comparison needs the turns
        where it is False; averaging across the others mixes two people.
        """
        return len(self.speaker) > 1

    @property
    def primary_speaker(self) -> Optional[str]:
        """
        The first speaker in the list, or None if the turn has none.

        None rather than an error: an empty speaker list is a legal state (see
        ``__post_init__``), so raising here would make a documented condition
        unreadable. It also matches what the vectorised equivalent produces --
        ``df.speaker.str[0]`` yields NaN for an empty list.
        """
        return self.speaker[0] if self.speaker else None

    @property
    def is_host(self) -> bool:
        """Check if the primary speaker is inferred to be a host."""
        return self.inferred_speaker_role == "host"

    @property
    def is_guest(self) -> bool:
        """Check if the primary speaker is inferred to be a guest."""
        return self.inferred_speaker_role == "guest"

    @property
    def has_inferred_speaker(self) -> bool:
        """
        Whether :attr:`inferred_speaker_name` names an actual person.

        False for null and for the ``NO_INFERRED_SPEAKER`` sentinel, which is
        the value for most turns. Testing the field for null instead keeps every
        unattributed turn, which is the mistake this property exists to prevent.
        """
        name = self.inferred_speaker_name
        return bool(name) and name not in PLACEHOLDER_SPEAKERS

    @property
    def has_inferred_role(self) -> bool:
        """
        Whether :attr:`inferred_speaker_role` holds a resolved role.

        False for null and for the ``NO_INFERRED_ROLE`` sentinel. Note that
        :attr:`is_host` and :attr:`is_guest` are both False for such a turn, so
        ``not is_host`` does not mean "guest".
        """
        role = self.inferred_speaker_role
        return bool(role) and role not in PLACEHOLDER_SPEAKERS

    @property
    def word_count(self) -> int:
        """
        Number of words in the turn: whitespace-separated tokens of the text.

        This is the same definition used by ``turns/metrics.word_count`` and by
        ``episode_metrics.total_word_count``, so per-turn and per-episode counts
        agree, and it is defined for every turn.

        Deliberately not the dataset's ``turns/text.word_count`` column, which
        despite the name counts something else -- see :attr:`token_count`.
        """
        return len(self.text.split())

    @property
    def token_count(self) -> Optional[int]:
        """
        Timestamped tokens the transcript aligned to this turn, or None.

        The transcript gives every token a timestamp and counts punctuation as
        a token, so this runs about 21% above :attr:`word_count` -- the median
        ratio between the two across the corpus is 1.21. Use it when you want
        what the aligner saw; use word_count when you want words.

        None for the 18,250,545 turns (9.9%) carried over from dataset 1.0,
        which are exactly those with ``speakers_recomputed`` False. Version 1.0
        had no such column and the word lists it was derived from are gone.
        """
        # NaN rather than None: joining the acoustics on goes through pandas,
        # which represents a missing integer as float('nan'), and that is not
        # None, so an unguarded read propagates NaN into every sum downstream.
        tc = self.stored_token_count
        if tc is None or (isinstance(tc, float) and math.isnan(tc)):
            return None
        return int(tc)

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
            'has_inferred_role': self.has_inferred_role,
            'has_inferred_speaker': self.has_inferred_speaker,
            'word_count': self.word_count,
            'token_count': self.token_count,
            'words_per_second': self.words_per_second,
            'audio_features': self.get_audio_features(),
            'mp3_url': self.mp3_url,
        }

    def __str__(self) -> str:
        """String representation of the turn."""
        who = self.primary_speaker or "no speaker"
        return f"Turn({who}, {self.start_time:.1f}s-{self.end_time:.1f}s, {self.word_count} words)"

    def __repr__(self) -> str:
        """Detailed string representation of the turn."""
        return (f"Turn(speaker={self.speaker}, start_time={self.start_time}, "
                f"end_time={self.end_time}, duration={self.duration}, "
                f"text='{self.text[:50]}{'...' if len(self.text) > 50 else ''}')")