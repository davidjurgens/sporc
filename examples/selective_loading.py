#!/usr/bin/env python3
"""
Selective loading example for the SPORC package.

This example demonstrates how to use the selective loading feature to filter
and load specific podcast subsets into memory for O(1) operations.
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


def demonstrate_selective_loading():
    """Demonstrate selective loading with various filtering criteria."""
    print("=== SPORC Selective Loading Examples ===\n")

    print(f"Initial memory usage: {get_memory_usage():.1f} MB\n")

    try:
        # Example 1: Load education podcasts
        print("1. Loading Education Podcasts:")
        start_time = time.time()
        start_memory = get_memory_usage()

        sporc = SPORCDataset(streaming=True)
        sporc.load_podcast_subset(categories=['education'])

        load_time = time.time() - start_time
        load_memory = get_memory_usage() - start_memory

        print(f"   ✓ Loaded in {load_time:.2f} seconds")
        print(f"   ✓ Memory usage: {load_memory:.1f} MB")
        print(f"   ✓ Loaded {len(sporc)} episodes from {len(sporc.get_all_podcasts())} podcasts")

        # Demonstrate fast access
        education_podcasts = sporc.get_all_podcasts()
        print(f"   ✓ Education podcasts: {[p.title for p in education_podcasts[:3]]}...")

        # Fast search within the subset
        long_education_episodes = sporc.search_episodes(min_duration=1800)  # 30+ minutes
        print(f"   ✓ Found {len(long_education_episodes)} long education episodes")

        print()

        # Example 2: Load podcasts by specific hosts
        print("2. Loading Podcasts by Host:")
        start_time = time.time()
        start_memory = get_memory_usage()

        sporc_hosts = SPORCDataset(streaming=True)
        sporc_hosts.load_podcast_subset(hosts=['Simon Shapiro', 'John Doe'])

        load_time = time.time() - start_time
        load_memory = get_memory_usage() - start_memory

        print(f"   ✓ Loaded in {load_time:.2f} seconds")
        print(f"   ✓ Memory usage: {load_memory:.1f} MB")
        print(f"   ✓ Loaded {len(sporc_hosts)} episodes from {len(sporc_hosts.get_all_podcasts())} podcasts")

        # Demonstrate fast access
        host_podcasts = sporc_hosts.get_all_podcasts()
        print(f"   ✓ Host podcasts: {[p.title for p in host_podcasts[:3]]}...")

        print()

        # Example 3: Load substantial podcasts
        print("3. Loading Substantial Podcasts (10+ episodes):")
        start_time = time.time()
        start_memory = get_memory_usage()

        sporc_substantial = SPORCDataset(streaming=True)
        sporc_substantial.load_podcast_subset(min_episodes=10)

        load_time = time.time() - start_time
        load_memory = get_memory_usage() - start_memory

        print(f"   ✓ Loaded in {load_time:.2f} seconds")
        print(f"   ✓ Memory usage: {load_memory:.1f} MB")
        print(f"   ✓ Loaded {len(sporc_substantial)} episodes from {len(sporc_substantial.get_all_podcasts())} podcasts")

        # Demonstrate fast access
        substantial_podcasts = sporc_substantial.get_all_podcasts()
        print(f"   ✓ Substantial podcasts: {[p.title for p in substantial_podcasts[:3]]}...")

        print()

        # Example 4: Load English podcasts with substantial content
        print("4. Loading English Podcasts with 5+ Hours of Content:")
        start_time = time.time()
        start_memory = get_memory_usage()

        sporc_english = SPORCDataset(streaming=True)
        sporc_english.load_podcast_subset(language='en', min_total_duration=5.0)

        load_time = time.time() - start_time
        load_memory = get_memory_usage() - start_memory

        print(f"   ✓ Loaded in {load_time:.2f} seconds")
        print(f"   ✓ Memory usage: {load_memory:.1f} MB")
        print(f"   ✓ Loaded {len(sporc_english)} episodes from {len(sporc_english.get_all_podcasts())} podcasts")

        # Demonstrate fast access
        english_podcasts = sporc_english.get_all_podcasts()
        print(f"   ✓ English podcasts: {[p.title for p in english_podcasts[:3]]}...")

        print()

        # Example 5: Complex filtering
        print("5. Complex Filtering (Education/Science, 5+ episodes, 2+ hours, English):")
        start_time = time.time()
        start_memory = get_memory_usage()

        sporc_complex = SPORCDataset(streaming=True)
        sporc_complex.load_podcast_subset(
            categories=['education', 'science'],
            min_episodes=5,
            min_total_duration=2.0,  # 2+ hours
            language='en'
        )

        load_time = time.time() - start_time
        load_memory = get_memory_usage() - start_memory

        print(f"   ✓ Loaded in {load_time:.2f} seconds")
        print(f"   ✓ Memory usage: {load_memory:.1f} MB")
        print(f"   ✓ Loaded {len(sporc_complex)} episodes from {len(sporc_complex.get_all_podcasts())} podcasts")

        # Demonstrate fast access
        complex_podcasts = sporc_complex.get_all_podcasts()
        print(f"   ✓ Curated podcasts: {[p.title for p in complex_podcasts[:3]]}...")

        print()

    except Exception as e:
        print(f"Error: {e}")


def demonstrate_performance_comparison():
    """Compare performance between streaming and selective modes."""
    print("=== Performance Comparison: Streaming vs Selective ===\n")

    try:
        # Test streaming mode search
        print("1. Streaming Mode Search (O(n) operations):")
        start_time = time.time()
        start_memory = get_memory_usage()

        sporc_streaming = SPORCDataset(streaming=True)

        # Search for education episodes
        education_episodes = sporc_streaming.search_episodes(category="education")
        search_time = time.time() - start_time
        search_memory = get_memory_usage() - start_memory

        print(f"   ✓ Search time: {search_time:.2f} seconds")
        print(f"   ✓ Memory usage: {search_memory:.1f} MB")
        print(f"   ✓ Found {len(education_episodes)} education episodes")

        # Search again (will be slow again)
        start_time = time.time()
        long_education_episodes = sporc_streaming.search_episodes(
            category="education",
            min_duration=1800
        )
        second_search_time = time.time() - start_time

        print(f"   ✓ Second search time: {second_search_time:.2f} seconds")
        print(f"   ✓ Found {len(long_education_episodes)} long education episodes")

        print()

        # Test selective mode search
        print("2. Selective Mode Search (O(1) operations after loading):")
        start_time = time.time()
        start_memory = get_memory_usage()

        sporc_selective = SPORCDataset(streaming=True)
        sporc_selective.load_podcast_subset(categories=['education'])

        load_time = time.time() - start_time
        load_memory = get_memory_usage() - start_memory

        print(f"   ✓ Load time: {load_time:.2f} seconds")
        print(f"   ✓ Memory usage: {load_memory:.1f} MB")
        print(f"   ✓ Loaded {len(sporc_selective)} education episodes")

        # Fast search within subset
        start_time = time.time()
        education_episodes = sporc_selective.search_episodes(category="education")
        search_time = time.time() - start_time

        print(f"   ✓ Search time: {search_time:.4f} seconds (O(1))")
        print(f"   ✓ Found {len(education_episodes)} education episodes")

        # Another fast search
        start_time = time.time()
        long_education_episodes = sporc_selective.search_episodes(
            category="education",
            min_duration=1800
        )
        second_search_time = time.time() - start_time

        print(f"   ✓ Second search time: {second_search_time:.4f} seconds (O(1))")
        print(f"   ✓ Found {len(long_education_episodes)} long education episodes")

        print()

        # Performance summary
        print("=== Performance Summary ===")
        print(f"Streaming mode:")
        print(f"  - First search: {search_time:.2f}s")
        print(f"  - Second search: {second_search_time:.2f}s")
        print(f"  - Memory usage: {search_memory:.1f}MB")
        print()
        print(f"Selective mode:")
        print(f"  - Load time: {load_time:.2f}s")
        print(f"  - First search: {search_time:.4f}s")
        print(f"  - Second search: {second_search_time:.4f}s")
        print(f"  - Memory usage: {load_memory:.1f}MB")
        print()
        print(f"Selective mode provides {search_time/second_search_time:.1f}x faster searches")
        print(f"after initial loading cost of {load_time:.2f}s")

    except Exception as e:
        print(f"Error: {e}")


def demonstrate_workflow_examples():
    """Demonstrate practical workflow examples using selective loading."""
    print("\n=== Practical Workflow Examples ===\n")

    try:
        # Workflow 1: Research on education podcasts
        print("1. Education Podcast Research Workflow:")

        sporc = SPORCDataset(streaming=True)
        sporc.load_podcast_subset(
            categories=['education'],
            min_episodes=5,
            language='en'
        )

        print(f"   Loaded {len(sporc)} episodes from {len(sporc.get_all_podcasts())} education podcasts")

        # Analyze episode types
        solo_episodes = sporc.search_episodes(max_speakers=1)
        interview_episodes = sporc.search_episodes(min_speakers=2, max_speakers=2)
        panel_episodes = sporc.search_episodes(min_speakers=3)

        print(f"   Solo episodes: {len(solo_episodes)}")
        print(f"   Interview episodes: {len(interview_episodes)}")
        print(f"   Panel episodes: {len(panel_episodes)}")

        # Analyze duration patterns
        short_episodes = sporc.search_episodes(max_duration=900)  # < 15 minutes
        medium_episodes = sporc.search_episodes(min_duration=900, max_duration=2700)  # 15-45 minutes
        long_episodes = sporc.search_episodes(min_duration=2700)  # > 45 minutes

        print(f"   Short episodes (<15min): {len(short_episodes)}")
        print(f"   Medium episodes (15-45min): {len(medium_episodes)}")
        print(f"   Long episodes (>45min): {len(long_episodes)}")

        print()

        # Workflow 2: Host analysis
        print("2. Host Analysis Workflow:")

        sporc_hosts = SPORCDataset(streaming=True)
        sporc_hosts.load_podcast_subset(
            hosts=['Simon Shapiro', 'John Doe', 'Jane Smith'],
            min_episodes=3
        )

        print(f"   Loaded {len(sporc_hosts)} episodes from selected hosts")

        # Analyze host-specific patterns
        for podcast in sporc_hosts.get_all_podcasts():
            print(f"   Host: {podcast.host_names}")
            print(f"   Podcast: {podcast.title}")
            print(f"   Episodes: {podcast.num_episodes}")
            print(f"   Total duration: {podcast.total_duration_hours:.1f} hours")
            print()

        print()

        # Workflow 3: Content analysis
        print("3. Content Analysis Workflow:")

        sporc_content = SPORCDataset(streaming=True)
        sporc_content.load_podcast_subset(
            min_total_duration=10.0,  # 10+ hours of content
            min_episodes=20
        )

        print(f"   Loaded {len(sporc_content)} episodes from substantial podcasts")

        # Analyze content patterns
        stats = sporc_content.get_dataset_statistics()
        print(f"   Total duration: {stats['total_duration_hours']:.1f} hours")
        print(f"   Average episode length: {stats['avg_episode_duration_minutes']:.1f} minutes")
        print(f"   Top categories: {dict(sorted(stats['category_distribution'].items(), key=lambda x: x[1], reverse=True)[:5])}")

        print()

    except Exception as e:
        print(f"Error: {e}")


def main():
    """Main function demonstrating selective loading."""
    print("=== SPORC Selective Loading Example ===\n")

    print(f"Initial memory usage: {get_memory_usage():.1f} MB\n")

    try:
        # Demonstrate selective loading with various criteria
        demonstrate_selective_loading()

        # Compare performance between modes
        demonstrate_performance_comparison()

        # Demonstrate practical workflows
        demonstrate_workflow_examples()

        print(f"\n=== Final Memory Usage: {get_memory_usage():.1f} MB ===")
        print("=== Selective Loading Example Completed Successfully! ===")

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