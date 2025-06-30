import pytest
from sporc.episode import Episode, TimeRangeBehavior
from sporc.turn import Turn
from datetime import datetime

@pytest.fixture
def sample_turns_data():
    return [
        {
            "speaker": ["SPEAKER_00"],
            "turnText": "Hello world",
            "startTime": 0.0,
            "endTime": 5.0,
            "duration": 5.0,
            "turnCount": 1,
            "inferredSpeakerRole": None,
            "inferredSpeakerName": None,
            "mp3url": "http://example.com/ep.mp3",
        },
        {
            "speaker": ["SPEAKER_01"],
            "turnText": "Guest intro",
            "startTime": 5.0,
            "endTime": 10.0,
            "duration": 5.0,
            "turnCount": 2,
            "inferredSpeakerRole": "guest",
            "inferredSpeakerName": "Alice",
            "mp3url": "http://example.com/ep.mp3",
        },
        {
            "speaker": ["SPEAKER_00"],
            "turnText": "Host reply",
            "startTime": 10.0,
            "endTime": 15.0,
            "duration": 5.0,
            "turnCount": 3,
            "inferredSpeakerRole": "host",
            "inferredSpeakerName": "Bob",
            "mp3url": "http://example.com/ep.mp3",
        },
    ]

@pytest.fixture
def sample_episode():
    return Episode(
        title="Test Episode",
        description="A test episode",
        mp3_url="http://example.com/ep.mp3",
        duration_seconds=1860.0,  # 31 minutes
        transcript="Transcript text",
        podcast_title="Test Podcast",
        podcast_description="Podcast desc",
        rss_url="http://example.com/rss.xml",
        category1="Tech",
        host_predicted_names=["Bob"],
        guest_predicted_names=["Alice"],
        main_ep_speakers=["Bob", "Alice"],
        episode_date_localized=1640995200000,
    )

def test_load_turns_and_accessors(sample_episode, sample_turns_data):
    with pytest.raises(RuntimeError):
        _ = sample_episode.turns
    sample_episode.load_turns(sample_turns_data)
    assert sample_episode._turns_loaded
    assert sample_episode.turn_count == 3
    assert sample_episode.has_turns is True
    assert sample_episode.get_all_turns() == sample_episode.turns
    assert sample_episode[0] == sample_episode.turns[0]
    assert list(iter(sample_episode)) == sample_episode.turns
    assert len(sample_episode) == 3

def test_get_turns_by_time_range(sample_episode, sample_turns_data):
    sample_episode.load_turns(sample_turns_data)
    strict = sample_episode.get_turns_by_time_range(0, 10, TimeRangeBehavior.STRICT)
    assert len(strict) == 2
    partial = sample_episode.get_turns_by_time_range(0, 10, TimeRangeBehavior.INCLUDE_PARTIAL)
    assert len(partial) == 2
    full = sample_episode.get_turns_by_time_range(0, 10, TimeRangeBehavior.INCLUDE_FULL_TURNS)
    assert len(full) == 2
    assert sample_episode.get_turns_by_time_range(1000, 2000) == []

def test_get_turns_by_time_range_with_trimming(sample_episode, sample_turns_data):
    sample_episode.load_turns(sample_turns_data)
    result = sample_episode.get_turns_by_time_range_with_trimming(0, 10)
    assert isinstance(result, list)
    assert all("turn" in d for d in result)
    for d in result:
        if d['turn'].start_time < 0 or d['turn'].end_time > 10:
            assert d['was_trimmed']

def test_get_turns_by_speaker_and_role(sample_episode, sample_turns_data):
    sample_episode.load_turns(sample_turns_data)
    assert len(sample_episode.get_turns_by_speaker("SPEAKER_00")) == 2
    assert len(sample_episode.get_turns_by_speaker("Alice")) == 1
    assert len(sample_episode.get_turns_by_role("host")) == 1
    assert len(sample_episode.get_turns_by_role("guest")) == 1
    assert len(sample_episode.get_host_turns()) == 1
    assert len(sample_episode.get_guest_turns()) == 1
    assert len(sample_episode.get_turns_by_min_length(2)) == 3
    assert len(sample_episode.get_turns_by_min_length(100)) == 0

def test_episode_properties_and_to_dict(sample_episode, sample_turns_data):
    sample_episode.load_turns(sample_turns_data)
    d = sample_episode.to_dict()
    assert d['title'] == "Test Episode"
    assert d['duration_minutes'] == 31.0
    assert d['num_main_speakers'] == 2
    assert d['is_long_form'] is True
    assert d['is_short_form'] is False
    assert d['has_guests'] is True
    assert d['is_solo'] is False
    assert d['is_interview'] is True
    assert d['is_panel'] is False
    assert isinstance(d['episode_date'], str)
    assert d['turns_loaded'] is True
    assert d['num_turns'] == 3

def test_episode_str_and_repr(sample_episode, sample_turns_data):
    sample_episode.load_turns(sample_turns_data)
    s = str(sample_episode)
    r = repr(sample_episode)
    assert "Test Episode" in s
    assert "Test Episode" in r
    assert "speakers" in s
    assert "num_hosts" in r

def test_episode_error_cases(sample_episode):
    # Not loaded
    with pytest.raises(RuntimeError):
        _ = sample_episode.turns
    with pytest.raises(RuntimeError):
        _ = sample_episode.turn_count
    with pytest.raises(RuntimeError):
        _ = sample_episode.get_all_turns()
    with pytest.raises(RuntimeError):
        _ = sample_episode[0]
    with pytest.raises(RuntimeError):
        list(iter(sample_episode))
    with pytest.raises(RuntimeError):
        sample_episode.get_turns_by_time_range(0, 10)
    with pytest.raises(RuntimeError):
        sample_episode.get_turns_by_time_range_with_trimming(0, 10)
    with pytest.raises(RuntimeError):
        sample_episode.get_turns_by_speaker("SPEAKER_00")
    with pytest.raises(RuntimeError):
        sample_episode.get_turns_by_min_length(1)
    with pytest.raises(RuntimeError):
        sample_episode.get_turns_by_role("host")
    with pytest.raises(RuntimeError):
        sample_episode.get_host_turns()
    with pytest.raises(RuntimeError):
        sample_episode.get_guest_turns()
    with pytest.raises(RuntimeError):
        len(sample_episode)
    with pytest.raises(RuntimeError):
        sample_episode.get_turn_statistics()