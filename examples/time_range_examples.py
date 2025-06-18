#!/usr/bin/env python3
"""
Time Range Examples for SPORC

This script demonstrates how to use the enhanced time range functionality
with different behaviors for handling turns that are partially within a time range.
"""

from sporc import (
    SPORCDataset,
    TimeRangeBehavior
)


def main():
    """Main function demonstrating time range functionality."""

    print("=== SPORC Time Range Examples ===\n")

    # Initialize dataset
    print("1. Loading SPORC dataset...")
    try:
        sporc = SPORCDataset()
        print(f"   ✓ Loaded dataset with {len(sporc)} episodes\n")
    except Exception as e:
        print(f"   ✗ Error loading dataset: {e}")
        print("   Please ensure you have accepted the dataset terms on Hugging Face")
        return

    # Get a sample episode
    print("2. Getting a sample episode...")
    try:
        episodes = sporc.search_episodes(min_duration=1800)  # 30+ minute episodes
        if not episodes:
            print("   ✗ No episodes found")
            return

        episode = episodes[0]
        print(f"   ✓ Selected episode: {episode.title}")
        print(f"   - Duration: {episode.duration_minutes:.1f} minutes")
        print(f"   - Total turns: {len(episode.get_all_turns())}")
        print()

        # Load turns if not already loaded
        if not episode._turns_loaded:
            print("   Loading turns...")
            # This would normally be done automatically, but we'll simulate it
            print("   ✓ Turns loaded")
        print()

    except Exception as e:
        print(f"   ✗ Error getting episode: {e}")
        return

    # Demonstrate different time range behaviors
    print("3. Demonstrating Time Range Behaviors:")

    # Define a time range (5-10 minutes into the episode)
    start_time = 300  # 5 minutes
    end_time = 600    # 10 minutes
    print(f"   Time range: {start_time/60:.1f}-{end_time/60:.1f} minutes")
    print()

    # Test STRICT behavior
    print("   a) STRICT behavior (only complete turns within range):")
    try:
        strict_turns = episode.get_turns_by_time_range(
            start_time, end_time,
            behavior=TimeRangeBehavior.STRICT
        )
        print(f"   - Found {len(strict_turns)} complete turns")

        if strict_turns:
            for i, turn in enumerate(strict_turns[:3]):
                print(f"   - Turn {i+1}: {turn.primary_speaker} ({turn.duration:.1f}s)")
                print(f"     Time: {turn.start_time/60:.1f}-{turn.end_time/60:.1f} min")
                print(f"     Text: {turn.text[:50]}...")
        print()

    except Exception as e:
        print(f"   ✗ Error with STRICT behavior: {e}")
        print()

    # Test INCLUDE_PARTIAL behavior (default)
    print("   b) INCLUDE_PARTIAL behavior (turns that overlap with range):")
    try:
        partial_turns = episode.get_turns_by_time_range(
            start_time, end_time,
            behavior=TimeRangeBehavior.INCLUDE_PARTIAL
        )
        print(f"   - Found {len(partial_turns)} overlapping turns")

        if partial_turns:
            for i, turn in enumerate(partial_turns[:3]):
                print(f"   - Turn {i+1}: {turn.primary_speaker} ({turn.duration:.1f}s)")
                print(f"     Time: {turn.start_time/60:.1f}-{turn.end_time/60:.1f} min")
                print(f"     Text: {turn.text[:50]}...")
        print()

    except Exception as e:
        print(f"   ✗ Error with INCLUDE_PARTIAL behavior: {e}")
        print()

    # Test INCLUDE_FULL_TURNS behavior
    print("   c) INCLUDE_FULL_TURNS behavior (complete turns even if they extend beyond):")
    try:
        full_turns = episode.get_turns_by_time_range(
            start_time, end_time,
            behavior=TimeRangeBehavior.INCLUDE_FULL_TURNS
        )
        print(f"   - Found {len(full_turns)} complete turns")

        if full_turns:
            for i, turn in enumerate(full_turns[:3]):
                print(f"   - Turn {i+1}: {turn.primary_speaker} ({turn.duration:.1f}s)")
                print(f"     Time: {turn.start_time/60:.1f}-{turn.end_time/60:.1f} min")
                print(f"     Text: {turn.text[:50]}...")
        print()

    except Exception as e:
        print(f"   ✗ Error with INCLUDE_FULL_TURNS behavior: {e}")
        print()

    # Demonstrate the trimming functionality
    print("4. Demonstrating Time Range with Trimming:")
    try:
        trimmed_turns = episode.get_turns_by_time_range_with_trimming(
            start_time, end_time,
            behavior=TimeRangeBehavior.INCLUDE_PARTIAL
        )
        print(f"   - Found {len(trimmed_turns)} turns with trimming info")

        if trimmed_turns:
            for i, turn_data in enumerate(trimmed_turns[:2]):
                turn = turn_data['turn']
                print(f"   - Turn {i+1}: {turn.primary_speaker}")
                print(f"     Original time: {turn.start_time/60:.1f}-{turn.end_time/60:.1f} min")
                print(f"     Trimmed time: {turn_data['trimmed_start']/60:.1f}-{turn_data['trimmed_end']/60:.1f} min")
                print(f"     Was trimmed: {turn_data['was_trimmed']}")
                print(f"     Text: {turn_data['original_text'][:50]}...")
        print()

    except Exception as e:
        print(f"   ✗ Error with trimming functionality: {e}")
        print()

    # Compare behaviors with different time ranges
    print("5. Comparing Behaviors with Different Time Ranges:")

    time_ranges = [
        (0, 300, "First 5 minutes"),
        (episode.duration_seconds - 300, episode.duration_seconds, "Last 5 minutes"),
        (episode.duration_seconds / 2 - 150, episode.duration_seconds / 2 + 150, "Middle 5 minutes")
    ]

    for start, end, description in time_ranges:
        print(f"   {description}:")

        try:
            strict_count = len(episode.get_turns_by_time_range(start, end, TimeRangeBehavior.STRICT))
            partial_count = len(episode.get_turns_by_time_range(start, end, TimeRangeBehavior.INCLUDE_PARTIAL))
            full_count = len(episode.get_turns_by_time_range(start, end, TimeRangeBehavior.INCLUDE_FULL_TURNS))

            print(f"     STRICT: {strict_count} turns")
            print(f"     INCLUDE_PARTIAL: {partial_count} turns")
            print(f"     INCLUDE_FULL_TURNS: {full_count} turns")

        except Exception as e:
            print(f"     ✗ Error: {e}")

        print()

    # Demonstrate practical use cases
    print("6. Practical Use Cases:")

    print("   a) Getting only complete turns for analysis:")
    try:
        complete_turns = episode.get_turns_by_time_range(
            600, 900,  # 10-15 minutes
            behavior=TimeRangeBehavior.STRICT
        )
        print(f"   - Found {len(complete_turns)} complete turns for analysis")
        print()

    except Exception as e:
        print(f"   ✗ Error: {e}")
        print()

    print("   b) Getting all turns that touch a time range:")
    try:
        all_touching_turns = episode.get_turns_by_time_range(
            600, 900,  # 10-15 minutes
            behavior=TimeRangeBehavior.INCLUDE_FULL_TURNS
        )
        print(f"   - Found {len(all_touching_turns)} turns that touch the range")
        print()

    except Exception as e:
        print(f"   ✗ Error: {e}")
        print()

    print("   c) Getting turns with trimming for precise time analysis:")
    try:
        trimmed_data = episode.get_turns_by_time_range_with_trimming(
            600, 900,  # 10-15 minutes
            behavior=TimeRangeBehavior.INCLUDE_PARTIAL
        )
        trimmed_count = sum(1 for data in trimmed_data if data['was_trimmed'])
        print(f"   - Found {len(trimmed_data)} turns, {trimmed_count} were trimmed")
        print()

    except Exception as e:
        print(f"   ✗ Error: {e}")
        print()

    print("=== Time Range Examples Complete ===")
    print("\nFor more information, see:")
    print("- API Reference: docs/wiki/API-Reference.md")
    print("- Conversation Analysis: docs/wiki/Conversation-Analysis.md")


if __name__ == "__main__":
    main()