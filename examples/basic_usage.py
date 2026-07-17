#!/usr/bin/env python3
"""
Basic usage example for the SPORC package.

Loading the dataset, searching for podcasts and episodes, and reading
conversation turns.

By default this reads the small tutorial subset built by
``scripts/build_tutorial_subset.py``. Point ``SPORC_PARQUET_DIR`` at any other
Parquet layout — including the full corpus — to run it against that instead.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from sporc import SPORCDataset, SPORCError


def open_dataset():
    """Open the tutorial subset, or whatever SPORC_PARQUET_DIR points at."""
    data_dir = os.environ.get(
        "SPORC_PARQUET_DIR",
        os.path.join(os.path.dirname(__file__), "..", "subsets", "tutorial"),
    )
    return SPORCDataset(parquet_dir=os.path.abspath(data_dir))


def first_diarized_episode(sporc):
    """A diarized episode to demonstrate turn analysis on.

    Only about a third of SPoRC episodes carry speaker turns, so pick one that
    does rather than assuming the first episode has any.
    """
    for episode in sporc.iterate_episodes():
        if episode.has_turn_data and episode.turns:
            return episode
    return None


def main():
    """Main function demonstrating basic SPORC usage."""

    print("=== SPORC Basic Usage Example ===\n")

    try:
        print("1. Loading SPORC dataset...")
        sporc = open_dataset()
        print(f"   Loaded {len(sporc)} episodes\n")

        print("2. Dataset statistics:")
        stats = sporc.get_dataset_statistics()
        print(f"   - Total podcasts: {stats['total_podcasts']}")
        print(f"   - Total episodes: {stats['total_episodes']}")
        print(f"   - Total duration: {stats['total_duration_hours']:.1f} hours")
        print(f"   - Average episode length: {stats['avg_episode_duration_minutes']:.1f} minutes")
        print()

        # Look a podcast up by title. Rather than hard-code a name that may not
        # be in your layout, take one from the catalog and search for it.
        podcasts = sporc.get_all_podcasts()
        print("3. Looking up a podcast by title...")
        sample_title = podcasts[0].title
        podcast = sporc.search_podcast(sample_title)
        print(f"   Found: {podcast.title}")
        print(f"   - Episodes: {podcast.num_episodes}")
        print(f"   - Hosts: {', '.join(podcast.host_names) or '(none labelled)'}")
        print(f"   - Categories: {', '.join(podcast.categories) or '(none)'}")
        print()

        print("4. A few episodes from that podcast:")
        for i, episode in enumerate(podcast.episodes[:3], 1):
            print(f"   {i}. {episode.title}  ({episode.duration_minutes:.1f} min, "
                  f"{episode.num_main_speakers} speakers)")
        print()

        # Metadata search. search_episodes takes keyword criteria and returns
        # every match; unknown criteria are ignored rather than erroring.
        print("5. Searching episodes by criteria (>= 5 min, <= 3 speakers)...")
        episodes = sporc.search_episodes(min_duration=300, max_speakers=3)
        print(f"   Found {len(episodes)} episodes matching")
        print()

        # Turn analysis needs an episode that actually has turns.
        print("6. Reading conversation turns...")
        episode = first_diarized_episode(sporc)
        if episode is None:
            print("   No diarized episodes in this layout; skipping turn analysis.")
        else:
            turns = episode.get_all_turns()
            print(f"   '{episode.title}' has {len(turns)} turns")
            for i, turn in enumerate(turns[:3], 1):
                print(f"   Turn {i}: {turn.primary_speaker} "
                      f"({turn.duration:.1f}s, {turn.word_count} words)")
                print(f"     {turn.text[:80]}...")
            print()

            print("7. Slicing turns by time and speaker:")
            early = episode.get_turns_by_time_range(0, 120)
            print(f"   - {len(early)} turns in the first 2 minutes")
            speaker = turns[0].primary_speaker
            print(f"   - {len(episode.get_turns_by_speaker(speaker))} turns by {speaker}")
            print(f"   - {len(episode.get_turns_by_min_length(50))} turns of 50+ words")
            print()

        print("8. Per-podcast statistics:")
        stats = podcast.get_episode_statistics()
        print(f"   - Episodes: {stats['num_episodes']}")
        print(f"   - Total duration: {stats['total_duration_hours']:.1f} hours")
        print(f"   - Average length: {stats['avg_episode_duration_minutes']:.1f} minutes")
        print()

        print("=== Example completed successfully! ===")

    except SPORCError as e:
        print(f"SPORC Error: {e}")
        print("\nBuild the tutorial subset first (see examples/notebooks/README.md), "
              "or set SPORC_PARQUET_DIR to a Parquet layout you have.")
        sys.exit(1)


if __name__ == "__main__":
    main()
