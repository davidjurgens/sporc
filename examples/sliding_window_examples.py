"""
Examples demonstrating sliding window functionality for podcast episodes.

This module shows how to use the sliding window approach to process turns
in chunks with configurable overlap, which is useful for:
- Conversation analysis in manageable chunks
- Context-aware processing with overlapping windows
- Time-based analysis of conversation flow
- Batch processing of large episodes
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sporc import SPORCDataset
from sporc.episode import Episode, TurnWindow
from sporc.turn import Turn


def create_test_episode():
    """Create a test episode with sample turns for demonstration."""
    # Create a test episode
    episode = Episode(
        title="Test Conversation Episode",
        description="A test episode for sliding window demonstration",
        mp3_url="test_conversation.mp3",
        duration_seconds=600.0,  # 10 minutes
        transcript="Test transcript for conversation analysis",
        podcast_title="Test Podcast",
        podcast_description="A test podcast for demonstration",
        rss_url="test.rss"
    )

    # Create sample turns with conversation data
    turns_data = []
    for i in range(20):  # 20 turns
        turn_data = {
            'mp3url': 'test_conversation.mp3',
            'speaker': [f'SPEAKER_{i % 3:02d}'],  # 3 speakers alternating
            'turnText': f"This is turn {i+1} in the conversation. Speaker {i % 3} is talking about topic {i // 3 + 1}.",
            'startTime': i * 30.0,  # 30 seconds per turn
            'endTime': (i + 1) * 30.0,
            'duration': 30.0,
            'turnCount': i,
            'inferredSpeakerName': f"Speaker_{i % 3}",
            'inferredSpeakerRole': "host" if i % 3 == 0 else "guest"
        }
        turns_data.append(turn_data)

    # Load turns into episode
    episode.load_turns(turns_data)
    return episode


def basic_sliding_window_example():
    """Demonstrate basic sliding window functionality."""
    print("=== Basic Sliding Window Example ===")

    # Create a test episode
    episode = create_test_episode()

    print(f"Processing episode: {episode.title}")
    print(f"Total turns: {episode.turn_count}")
    print(f"Episode duration: {episode.duration_minutes:.1f} minutes")

    # Create sliding windows with 5 turns each and 2 turns overlap
    window_size = 5
    overlap = 2

    print(f"\nSliding windows: {window_size} turns each, {overlap} turns overlap")
    print("-" * 60)

    for i, window in enumerate(episode.sliding_window(window_size, overlap)):
        print(f"Window {i+1}/{window.total_windows}:")
        print(f"  Turns: {window.start_index}-{window.end_index} ({window.size} turns)")
        print(f"  Time range: {window.time_range[0]:.1f}s - {window.time_range[1]:.1f}s")
        print(f"  Duration: {window.duration:.1f}s")
        print(f"  New turns: {len(window.new_turns)}")
        print(f"  Overlap turns: {len(window.overlap_turns)}")

        # Show speaker distribution
        speaker_dist = window.get_speaker_distribution()
        print(f"  Speakers: {list(speaker_dist.keys())}")

        # Show first few words of the window
        text = window.get_text()
        preview = text[:80] + "..." if len(text) > 80 else text
        print(f"  Preview: {preview}")
        print()


def time_based_sliding_window_example():
    """Demonstrate time-based sliding windows."""
    print("=== Time-Based Sliding Window Example ===")

    episode = create_test_episode()

    print(f"Processing episode: {episode.title}")
    print(f"Episode duration: {episode.duration_minutes:.1f} minutes")

    # Create 2-minute windows with 30-second overlap
    window_duration = 120  # 2 minutes in seconds
    overlap_duration = 30   # 30 seconds in seconds

    print(f"\nTime-based windows: {window_duration/60:.1f}min each, {overlap_duration/60:.1f}min overlap")
    print("-" * 60)

    for i, window in enumerate(episode.sliding_window_by_time(window_duration, overlap_duration)):
        print(f"Window {i+1}:")
        print(f"  Time range: {window.time_range[0]/60:.1f}min - {window.time_range[1]/60:.1f}min")
        print(f"  Duration: {window.duration/60:.1f} minutes")
        print(f"  Turns: {window.size}")

        # Show role distribution
        role_dist = window.get_role_distribution()
        print(f"  Roles: {role_dist}")

        # Show word count
        total_words = sum(turn.word_count for turn in window.turns)
        print(f"  Total words: {total_words}")
        print()


def overlapping_context_example():
    """Demonstrate how overlapping windows maintain context."""
    print("=== Overlapping Context Example ===")

    episode = create_test_episode()

    print(f"Processing episode: {episode.title}")

    # Create windows with significant overlap to show context preservation
    window_size = 8
    overlap = 5  # High overlap to maintain context

    print(f"\nHigh-overlap windows: {window_size} turns each, {overlap} turns overlap")
    print("-" * 60)

    windows = list(episode.sliding_window(window_size, overlap))

    for i, window in enumerate(windows[:3]):  # Show first 3 windows
        print(f"Window {i+1}:")
        print(f"  Turn range: {window.start_index}-{window.end_index}")
        print(f"  New turns: {len(window.new_turns)}")
        print(f"  Overlap turns: {len(window.overlap_turns)}")

        # Show the overlap turns (context from previous window)
        if window.overlap_turns:
            print("  Overlap context:")
            for turn in window.overlap_turns[:2]:  # Show first 2 overlap turns
                speaker = turn.inferred_speaker_name or turn.speaker[0] if turn.speaker else "Unknown"
                print(f"    {speaker}: {turn.text[:40]}...")

        # Show the new turns
        if window.new_turns:
            print("  New content:")
            for turn in window.new_turns[:2]:  # Show first 2 new turns
                speaker = turn.inferred_speaker_name or turn.speaker[0] if turn.speaker else "Unknown"
                print(f"    {speaker}: {turn.text[:40]}...")
        print()


def window_statistics_example():
    """Demonstrate window statistics and analysis."""
    print("=== Window Statistics Example ===")

    episode = create_test_episode()

    print(f"Episode: {episode.title}")
    print(f"Total turns: {episode.turn_count}")

    # Get statistics for different window configurations
    window_configs = [
        (5, 0),   # No overlap
        (5, 2),   # Small overlap
        (8, 3),   # Medium overlap
        (10, 5),  # Larger windows
    ]

    print("\nWindow Statistics:")
    print("-" * 60)

    for window_size, overlap in window_configs:
        stats = episode.get_window_statistics(window_size, overlap)
        print(f"Window size: {window_size}, Overlap: {overlap}")
        print(f"  Total windows: {stats['total_windows']}")
        print(f"  Step size: {stats['step_size']}")
        print(f"  Avg window duration: {stats['avg_window_duration']:.1f}s")
        print(f"  Avg turn duration: {stats['avg_turn_duration']:.1f}s")
        print()


def conversation_flow_analysis():
    """Analyze conversation flow using sliding windows."""
    print("=== Conversation Flow Analysis ===")

    episode = create_test_episode()

    print(f"Analyzing conversation flow in: {episode.title}")

    # Use 1-minute windows with 15-second overlap
    window_duration = 60  # 1 minute
    overlap_duration = 15  # 15 seconds

    print(f"\nAnalyzing {window_duration/60:.1f}min windows with {overlap_duration}s overlap")
    print("-" * 60)

    for i, window in enumerate(episode.sliding_window_by_time(window_duration, overlap_duration)):
        if i >= 5:  # Limit to first 5 windows for brevity
            break

        print(f"Window {i+1} ({window.time_range[0]/60:.1f}-{window.time_range[1]/60:.1f}min):")

        # Analyze speaker distribution
        speaker_dist = window.get_speaker_distribution()
        role_dist = window.get_role_distribution()

        print(f"  Speakers: {len(speaker_dist)} unique")
        print(f"  Roles: {role_dist}")

        # Calculate conversation metrics
        total_words = sum(turn.word_count for turn in window.turns)
        avg_words_per_turn = total_words / len(window.turns) if window.turns else 0

        print(f"  Total words: {total_words}")
        print(f"  Avg words per turn: {avg_words_per_turn:.1f}")
        print(f"  Conversation density: {len(window.turns) / (window.duration/60):.1f} turns/min")
        print()


def main():
    """Run all sliding window examples."""
    print("SPORC Sliding Window Examples")
    print("=" * 50)

    try:
        basic_sliding_window_example()
        print("\n" + "="*50 + "\n")

        time_based_sliding_window_example()
        print("\n" + "="*50 + "\n")

        overlapping_context_example()
        print("\n" + "="*50 + "\n")

        window_statistics_example()
        print("\n" + "="*50 + "\n")

        conversation_flow_analysis()

    except Exception as e:
        print(f"Error running examples: {e}")
        print("Make sure you have the SPORC dataset available and turns are loaded.")


if __name__ == "__main__":
    main()