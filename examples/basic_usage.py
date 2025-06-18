#!/usr/bin/env python3
"""
Basic usage example for the SPORC package.

This example demonstrates the core functionality of the SPORC package,
including loading the dataset, searching for podcasts and episodes,
and analyzing conversation turns.
"""

import sys
import os

# Add the parent directory to the path so we can import the sporc package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from sporc import SPORCDataset, SPORCError


def main():
    """Main function demonstrating basic SPORC usage."""

    print("=== SPORC Basic Usage Example ===\n")

    try:
        # Initialize the dataset
        print("1. Loading SPORC dataset...")
        sporc = SPORCDataset()
        print(f"   ✓ Loaded dataset with {len(sporc)} episodes\n")

        # Get dataset statistics
        print("2. Dataset Statistics:")
        stats = sporc.get_dataset_statistics()
        print(f"   - Total podcasts: {stats['total_podcasts']}")
        print(f"   - Total episodes: {stats['total_episodes']}")
        print(f"   - Total duration: {stats['total_duration_hours']:.1f} hours")
        print(f"   - Average episode length: {stats['avg_episode_duration_minutes']:.1f} minutes")
        print()

        # Search for a specific podcast
        print("3. Searching for a specific podcast...")
        try:
            podcast = sporc.search_podcast("SingOut SpeakOut")
            print(f"   ✓ Found podcast: {podcast.title}")
            print(f"   - Description: {podcast.description[:100]}...")
            print(f"   - Number of episodes: {podcast.num_episodes}")
            print(f"   - Hosts: {', '.join(podcast.host_names)}")
            print(f"   - Categories: {', '.join(podcast.categories)}")
            print()

            # Show some episodes from this podcast
            print("4. Sample episodes from the podcast:")
            for i, episode in enumerate(podcast.episodes[:3]):
                print(f"   Episode {i+1}: {episode.title}")
                print(f"   - Duration: {episode.duration_minutes:.1f} minutes")
                print(f"   - Speakers: {episode.num_main_speakers}")
                print(f"   - Has guests: {episode.has_guests}")
                print()

        except Exception as e:
            print(f"   ✗ Error finding podcast: {e}")
            print()

        # Search for episodes with specific criteria
        print("5. Searching for episodes with specific criteria...")
        episodes = sporc.search_episodes(
            min_duration=300,  # At least 5 minutes
            max_speakers=3,    # Maximum 3 speakers
            category="education"
        )
        print(f"   ✓ Found {len(episodes)} episodes matching criteria")

        if episodes:
            episode = episodes[0]
            print(f"   Sample episode: {episode.title}")
            print(f"   - Duration: {episode.duration_minutes:.1f} minutes")
            print(f"   - Speakers: {episode.num_main_speakers}")
            print(f"   - Categories: {', '.join(episode.categories)}")
            print()

            # Analyze conversation turns
            print("6. Analyzing conversation turns...")
            turns = episode.get_all_turns()
            print(f"   ✓ Loaded {len(turns)} conversation turns")

            if turns:
                # Show first few turns
                print("   First 3 turns:")
                for i, turn in enumerate(turns[:3]):
                    print(f"   Turn {i+1}:")
                    print(f"   - Speaker: {turn.primary_speaker}")
                    print(f"   - Duration: {turn.duration:.1f} seconds")
                    print(f"   - Words: {turn.word_count}")
                    print(f"   - Text: {turn.text[:100]}...")
                    print()

                # Get turns by time range
                print("7. Getting turns from first 2 minutes...")
                early_turns = episode.get_turns_by_time_range(0, 120)
                print(f"   ✓ Found {len(early_turns)} turns in first 2 minutes")

                # Get turns by speaker
                if turns:
                    speaker = turns[0].primary_speaker
                    speaker_turns = episode.get_turns_by_speaker(speaker)
                    print(f"   ✓ Found {len(speaker_turns)} turns by {speaker}")

                # Get long turns
                long_turns = episode.get_turns_by_min_length(50)
                print(f"   ✓ Found {len(long_turns)} turns with 50+ words")
                print()

        # Search for episodes by host
        print("8. Searching for episodes by host...")
        host_episodes = sporc.search_episodes(host_name="Simon")
        print(f"   ✓ Found {len(host_episodes)} episodes hosted by Simon")

        if host_episodes:
            print(f"   Sample episode: {host_episodes[0].title}")
            print()

        # Get all podcasts
        print("9. Getting all podcasts...")
        all_podcasts = sporc.get_all_podcasts()
        print(f"   ✓ Found {len(all_podcasts)} total podcasts")

        # Show podcast statistics
        if all_podcasts:
            podcast = all_podcasts[0]
            print(f"   Sample podcast: {podcast.title}")
            stats = podcast.get_episode_statistics()
            print(f"   - Total episodes: {stats['num_episodes']}")
            print(f"   - Total duration: {stats['total_duration_hours']:.1f} hours")
            print(f"   - Average episode length: {stats['avg_episode_duration_minutes']:.1f} minutes")
            print(f"   - Episode types: {stats['episode_types']}")
            print()

        print("=== Example completed successfully! ===")

    except SPORCError as e:
        print(f"SPORC Error: {e}")
        print("\nMake sure you have:")
        print("1. Accepted the dataset terms on Hugging Face")
        print("2. Set up Hugging Face authentication")
        print("3. Installed all required dependencies")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()