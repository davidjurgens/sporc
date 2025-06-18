#!/usr/bin/env python3
"""
Streaming usage example for the SPORC package.

This example demonstrates how to use the SPORC package in streaming mode
to process large datasets efficiently with limited memory.
"""

import sys
import os
import time
import psutil
from collections import defaultdict

# Add the parent directory to the path so we can import the sporc package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from sporc import SPORCDataset, SPORCError


def get_memory_usage():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


def demonstrate_streaming_vs_memory():
    """Demonstrate the difference between streaming and memory modes."""
    print("=== SPORC Streaming vs Memory Mode Comparison ===\n")

    # Test streaming mode
    print("1. Testing Streaming Mode:")
    start_time = time.time()
    start_memory = get_memory_usage()

    try:
        sporc_streaming = SPORCDataset(streaming=True)
        streaming_load_time = time.time() - start_time
        streaming_load_memory = get_memory_usage() - start_memory

        print(f"   ✓ Loaded in {streaming_load_time:.2f} seconds")
        print(f"   ✓ Memory usage: {streaming_load_memory:.1f} MB")

        # Get statistics (requires full iteration)
        print("   Calculating statistics...")
        stats_start = time.time()
        stats = sporc_streaming.get_dataset_statistics()
        stats_time = time.time() - stats_start

        print(f"   ✓ Statistics calculated in {stats_time:.2f} seconds")
        print(f"   ✓ Total episodes: {stats['total_episodes']}")
        print(f"   ✓ Total podcasts: {stats['total_podcasts']}")

    except Exception as e:
        print(f"   ✗ Error in streaming mode: {e}")
        return

    print()

    # Test memory mode (if we have enough memory)
    print("2. Testing Memory Mode:")
    start_time = time.time()
    start_memory = get_memory_usage()

    try:
        sporc_memory = SPORCDataset(streaming=False)
        memory_load_time = time.time() - start_time
        memory_load_memory = get_memory_usage() - start_memory

        print(f"   ✓ Loaded in {memory_load_time:.2f} seconds")
        print(f"   ✓ Memory usage: {memory_load_memory:.1f} MB")

        # Get statistics (instant)
        stats_start = time.time()
        stats = sporc_memory.get_dataset_statistics()
        stats_time = time.time() - stats_start

        print(f"   ✓ Statistics calculated in {stats_time:.2f} seconds")
        print(f"   ✓ Total episodes: {stats['total_episodes']}")
        print(f"   ✓ Total podcasts: {stats['total_podcasts']}")

    except Exception as e:
        print(f"   ✗ Error in memory mode: {e}")
        print("   (This is expected if you don't have enough RAM)")

    print("\n=== Comparison Summary ===")
    print(f"Streaming mode: {streaming_load_time:.2f}s load, {streaming_load_memory:.1f}MB memory")
    if 'memory_load_time' in locals():
        print(f"Memory mode: {memory_load_time:.2f}s load, {memory_load_memory:.1f}MB memory")
        print(f"Memory mode uses {memory_load_memory/streaming_load_memory:.1f}x more memory")
        print(f"Memory mode loads {memory_load_time/streaming_load_time:.1f}x faster")


def demonstrate_streaming_iteration():
    """Demonstrate streaming iteration capabilities."""
    print("\n=== Streaming Iteration Examples ===\n")

    try:
        sporc = SPORCDataset(streaming=True)

        # Example 1: Iterate over episodes
        print("1. Iterating over episodes:")
        episode_count = 0
        total_duration = 0

        for episode in sporc.iterate_episodes():
            episode_count += 1
            total_duration += episode.duration_seconds

            if episode_count <= 3:
                print(f"   Episode {episode_count}: {episode.title}")
                print(f"   Duration: {episode.duration_minutes:.1f} minutes")
                print(f"   Speakers: {episode.num_main_speakers}")
                print()
            elif episode_count == 4:
                print(f"   ... and {episode_count - 3} more episodes")
                print()

        print(f"   Processed {episode_count} episodes")
        print(f"   Total duration: {total_duration/3600:.1f} hours")
        print(f"   Current memory usage: {get_memory_usage():.1f} MB")

        # Example 2: Iterate over podcasts
        print("\n2. Iterating over podcasts:")
        podcast_count = 0

        for podcast in sporc.iterate_podcasts():
            podcast_count += 1
            print(f"   Podcast {podcast_count}: {podcast.title}")
            print(f"   Episodes: {podcast.num_episodes}")
            print(f"   Duration: {podcast.total_duration_hours:.1f} hours")

            if podcast_count >= 3:
                print(f"   ... and more podcasts")
                break
            print()

        print(f"   Processed {podcast_count} podcasts")
        print(f"   Current memory usage: {get_memory_usage():.1f} MB")

    except Exception as e:
        print(f"Error: {e}")


def demonstrate_streaming_search():
    """Demonstrate search capabilities in streaming mode."""
    print("\n=== Streaming Search Examples ===\n")

    try:
        sporc = SPORCDataset(streaming=True)

        # Search for long episodes
        print("1. Searching for long episodes (30+ minutes):")
        start_time = time.time()
        long_episodes = sporc.search_episodes(min_duration=1800)
        search_time = time.time() - start_time

        print(f"   Found {len(long_episodes)} long episodes in {search_time:.2f} seconds")

        if long_episodes:
            episode = long_episodes[0]
            print(f"   Example: {episode.title}")
            print(f"   Duration: {episode.duration_minutes:.1f} minutes")
            print(f"   Podcast: {episode.podcast_title}")

        # Search for education episodes
        print("\n2. Searching for education episodes:")
        start_time = time.time()
        education_episodes = sporc.search_episodes(category="education")
        search_time = time.time() - start_time

        print(f"   Found {len(education_episodes)} education episodes in {search_time:.2f} seconds")

        if education_episodes:
            episode = education_episodes[0]
            print(f"   Example: {episode.title}")
            print(f"   Categories: {', '.join(episode.categories)}")

        # Search for episodes with specific host
        print("\n3. Searching for episodes by host:")
        start_time = time.time()
        host_episodes = sporc.search_episodes(host_name="Simon")
        search_time = time.time() - start_time

        print(f"   Found {len(host_episodes)} episodes hosted by Simon in {search_time:.2f} seconds")

        if host_episodes:
            episode = host_episodes[0]
            print(f"   Example: {episode.title}")
            print(f"   Hosts: {', '.join(episode.host_names)}")

    except Exception as e:
        print(f"Error: {e}")


def demonstrate_memory_efficient_processing():
    """Demonstrate memory-efficient processing patterns."""
    print("\n=== Memory-Efficient Processing Patterns ===\n")

    try:
        sporc = SPORCDataset(streaming=True)

        # Pattern 1: Process episodes in batches
        print("1. Processing episodes in batches:")
        batch_size = 10
        episode_count = 0
        category_counts = defaultdict(int)

        for episode in sporc.iterate_episodes():
            episode_count += 1

            # Process episode
            for category in episode.categories:
                category_counts[category] += 1

            # Print progress every batch_size episodes
            if episode_count % batch_size == 0:
                print(f"   Processed {episode_count} episodes")
                print(f"   Memory usage: {get_memory_usage():.1f} MB")
                print(f"   Top categories so far: {dict(sorted(category_counts.items(), key=lambda x: x[1], reverse=True)[:3])}")
                print()

            # Limit for demo
            if episode_count >= 50:
                break

        print(f"   Final results: {episode_count} episodes processed")
        print(f"   Final memory usage: {get_memory_usage():.1f} MB")

        # Pattern 2: Filter and process
        print("\n2. Filtering and processing:")
        long_episode_count = 0
        total_long_duration = 0

        for episode in sporc.iterate_episodes():
            # Filter for long episodes
            if episode.duration_minutes >= 30:
                long_episode_count += 1
                total_long_duration += episode.duration_minutes

                # Process only long episodes
                print(f"   Long episode {long_episode_count}: {episode.title}")
                print(f"   Duration: {episode.duration_minutes:.1f} minutes")
                print(f"   Memory usage: {get_memory_usage():.1f} MB")

                if long_episode_count >= 5:
                    break

        print(f"   Found {long_episode_count} long episodes")
        print(f"   Total long episode duration: {total_long_duration:.1f} minutes")

    except Exception as e:
        print(f"Error: {e}")


def main():
    """Main function demonstrating streaming usage."""
    print("=== SPORC Streaming Usage Example ===\n")

    print(f"Initial memory usage: {get_memory_usage():.1f} MB\n")

    try:
        # Demonstrate streaming vs memory mode
        demonstrate_streaming_vs_memory()

        # Demonstrate streaming iteration
        demonstrate_streaming_iteration()

        # Demonstrate streaming search
        demonstrate_streaming_search()

        # Demonstrate memory-efficient processing
        demonstrate_memory_efficient_processing()

        print(f"\n=== Final Memory Usage: {get_memory_usage():.1f} MB ===")
        print("=== Streaming Example Completed Successfully! ===")

    except SPORCError as e:
        print(f"SPORC Error: {e}")
        print("\nMake sure you have:")
        print("1. Accepted the dataset terms on Hugging Face")
        print("2. Set up Hugging Face authentication")
        print("3. Installed all required dependencies")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()