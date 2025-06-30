import pytest
from sporc.turn import Turn

@pytest.fixture
def basic_turn():
    return Turn(
        speaker=["SPEAKER_00"],
        text="Hello world this is a test",
        start_time=0.0,
        end_time=5.0,
        duration=5.0,
        turn_count=1,
        inferred_speaker_role="host",
        inferred_speaker_name="Bob",
        mfcc1_sma3_mean=0.1,
        mfcc2_sma3_mean=0.2,
        mfcc3_sma3_mean=0.3,
        mfcc4_sma3_mean=0.4,
        f0_semitone_from_27_5hz_sma3nz_mean=0.5,
        f1_frequency_sma3nz_mean=0.6,
        mp3_url="http://example.com/ep.mp3",
    )

@pytest.fixture
def overlapping_turn():
    return Turn(
        speaker=["SPEAKER_00", "SPEAKER_01"],
        text="Overlapping turn",
        start_time=2.0,
        end_time=7.0,
        duration=5.0,
        turn_count=2,
        inferred_speaker_role="guest",
        inferred_speaker_name="Alice",
        mp3_url="http://example.com/ep.mp3",
    )

def test_turn_properties_and_audio_features(basic_turn):
    assert basic_turn.is_overlapping is False
    assert basic_turn.primary_speaker == "SPEAKER_00"
    assert basic_turn.is_host is True
    assert basic_turn.is_guest is False
    assert basic_turn.word_count == 6
    assert basic_turn.words_per_second == 6 / 5.0
    features = basic_turn.get_audio_features()
    assert features["mfcc1_sma3_mean"] == 0.1
    assert features["mfcc2_sma3_mean"] == 0.2
    assert features["mfcc3_sma3_mean"] == 0.3
    assert features["mfcc4_sma3_mean"] == 0.4
    assert features["f0_semitone_from_27_5hz_sma3nz_mean"] == 0.5
    assert features["f1_frequency_sma3nz_mean"] == 0.6
    d = basic_turn.to_dict()
    assert d["speaker"] == ["SPEAKER_00"]
    assert d["primary_speaker"] == "SPEAKER_00"
    assert d["inferred_speaker_role"] == "host"
    assert d["inferred_speaker_name"] == "Bob"
    assert d["audio_features"]["mfcc1_sma3_mean"] == 0.1
    assert d["mp3_url"] == "http://example.com/ep.mp3"
    assert str(basic_turn).startswith("Turn(")
    assert "Hello world" in repr(basic_turn)  # Text is present if short

def test_turn_overlapping_and_contains_time(basic_turn, overlapping_turn):
    assert overlapping_turn.is_overlapping is True
    assert overlapping_turn.primary_speaker == "SPEAKER_00"
    assert overlapping_turn.is_host is False
    assert overlapping_turn.is_guest is True
    assert overlapping_turn.contains_time(3.0) is True
    assert overlapping_turn.contains_time(8.0) is False
    # Overlap logic
    assert basic_turn.overlaps_with(overlapping_turn) is True
    assert overlapping_turn.overlaps_with(basic_turn) is True
    # No overlap
    t2 = Turn(
        speaker=["SPEAKER_02"],
        text="No overlap",
        start_time=10.0,
        end_time=12.0,
        duration=2.0,
        turn_count=3,
    )
    assert basic_turn.overlaps_with(t2) is False
    assert t2.overlaps_with(basic_turn) is False

def test_turn_validation_errors():
    # Negative start time
    with pytest.raises(ValueError):
        Turn(speaker=["SPEAKER_00"], text="x", start_time=-1, end_time=1, duration=2, turn_count=1)
    # End time before start time
    with pytest.raises(ValueError):
        Turn(speaker=["SPEAKER_00"], text="x", start_time=2, end_time=1, duration=2, turn_count=1)
    # Negative duration
    with pytest.raises(ValueError):
        Turn(speaker=["SPEAKER_00"], text="x", start_time=0, end_time=1, duration=-1, turn_count=1)
    # Empty speaker list
    with pytest.raises(ValueError):
        Turn(speaker=[], text="x", start_time=0, end_time=1, duration=1, turn_count=1)
    # Empty text
    with pytest.raises(ValueError):
        Turn(speaker=["SPEAKER_00"], text="   ", start_time=0, end_time=1, duration=1, turn_count=1)

def test_turn_zero_duration():
    t = Turn(speaker=["SPEAKER_00"], text="oneword", start_time=0, end_time=0, duration=0, turn_count=1)
    assert t.words_per_second == 0.0
    assert t.word_count == 1