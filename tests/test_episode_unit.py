import pytest
from sporc.episode import Episode, TimeRangeBehavior
from sporc.turn import Turn
from datetime import datetime


def _make_turns():
    """Create Turn objects for testing."""
    return [
        Turn(
            speaker=["SPEAKER_00"],
            text="Hello world",
            start_time=0.0,
            end_time=5.0,
            duration=5.0,
            turn_count=1,
            inferred_speaker_role=None,
            inferred_speaker_name=None,
            mp3_url="http://example.com/ep.mp3",
        ),
        Turn(
            speaker=["SPEAKER_01"],
            text="Guest intro",
            start_time=5.0,
            end_time=10.0,
            duration=5.0,
            turn_count=2,
            inferred_speaker_role="guest",
            inferred_speaker_name="Alice",
            mp3_url="http://example.com/ep.mp3",
        ),
        Turn(
            speaker=["SPEAKER_00"],
            text="Host reply",
            start_time=10.0,
            end_time=15.0,
            duration=5.0,
            turn_count=3,
            inferred_speaker_role="host",
            inferred_speaker_name="Bob",
            mp3_url="http://example.com/ep.mp3",
        ),
    ]


def _load_turns(episode, turns):
    """Load Turn objects directly into an episode."""
    episode._turns = sorted(turns, key=lambda t: t.start_time)
    episode._turns_loaded = True


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

def test_load_turns_and_accessors(sample_episode):
    with pytest.raises(RuntimeError):
        _ = sample_episode.turns
    turns = _make_turns()
    _load_turns(sample_episode, turns)
    assert sample_episode._turns_loaded
    assert sample_episode.turn_count == 3
    assert sample_episode.has_turns is True
    assert sample_episode.get_all_turns() == sample_episode.turns
    assert sample_episode[0] == sample_episode.turns[0]
    assert list(iter(sample_episode)) == sample_episode.turns
    assert len(sample_episode) == 3

def test_get_turns_by_time_range(sample_episode):
    _load_turns(sample_episode, _make_turns())
    strict = sample_episode.get_turns_by_time_range(0, 10, TimeRangeBehavior.STRICT)
    assert len(strict) == 2
    partial = sample_episode.get_turns_by_time_range(0, 10, TimeRangeBehavior.INCLUDE_PARTIAL)
    assert len(partial) == 2
    full = sample_episode.get_turns_by_time_range(0, 10, TimeRangeBehavior.INCLUDE_FULL_TURNS)
    assert len(full) == 2
    assert sample_episode.get_turns_by_time_range(1000, 2000) == []

def test_get_turns_by_time_range_with_trimming(sample_episode):
    _load_turns(sample_episode, _make_turns())
    result = sample_episode.get_turns_by_time_range_with_trimming(0, 10)
    assert isinstance(result, list)
    assert all("turn" in d for d in result)
    for d in result:
        if d['turn'].start_time < 0 or d['turn'].end_time > 10:
            assert d['was_trimmed']

def test_get_turns_by_speaker_and_role(sample_episode):
    _load_turns(sample_episode, _make_turns())
    assert len(sample_episode.get_turns_by_speaker("SPEAKER_00")) == 2
    assert len(sample_episode.get_turns_by_speaker("Alice")) == 1
    assert len(sample_episode.get_turns_by_role("host")) == 1
    assert len(sample_episode.get_turns_by_role("guest")) == 1
    assert len(sample_episode.get_host_turns()) == 1
    assert len(sample_episode.get_guest_turns()) == 1
    assert len(sample_episode.get_turns_by_min_length(2)) == 3
    assert len(sample_episode.get_turns_by_min_length(100)) == 0

def test_episode_properties_and_to_dict(sample_episode):
    _load_turns(sample_episode, _make_turns())
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

def test_episode_str_and_repr(sample_episode):
    _load_turns(sample_episode, _make_turns())
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
