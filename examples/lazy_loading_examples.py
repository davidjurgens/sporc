"""
Examples demonstrating lazy loading of turn data in the SPORC dataset.

This example shows how to use lazy loading to reduce memory usage and improve
performance when working with large datasets.
"""

from sporc import SPORCDataset
import time
import psutil
import os

def print_memory_usage(label: str):
    """Print current memory usage."""
    process = psutil.Process(os.getpid())
    memory_mb = process.memory_info().rss / 1024 / 1024
    print(f"{label}: {memory_mb:.1f} MB")

def example_1_basic_lazy_loading():
    """
    Example 1: Basic lazy loading setup and usage.

    This demonstrates the fundamental lazy loading workflow.
    """
    print("=== Example 1: Basic Lazy Loading ===")

    print_memory_usage("Before loading dataset")

    # Initialize with lazy loading
    sporc = SPORCDataset(
        local_data_dir="data",
        load_turns_eagerly=False,  # Enable lazy loading
        streaming=False
    )

    print_memory_usage("After loading dataset (without turns)")

    # Get episodes (turns not loaded yet)
    episodes = sporc.search_episodes(min_duration=1800)
    print(f"Found {len(episodes)} episodes > 30 minutes")

    # Load turns for a specific episode
    episode = episodes[0]
    print(f"\nLoading turns for: {episode.title}")

    start_time = time.time()
    sporc.load_turns_for_episode(episode)
    load_time = time.time() - start_time

    print(f"Loaded {len(episode)} turns in {load_time:.3f} seconds")
    print_memory_usage("After loading turns for one episode")

    # Access turn data
    stats = episode.get_turn_statistics()
    print(f"Episode statistics:")
    print(f"  Total turns: {stats['total_turns']}")
    print(f"  Total words: {stats['total_words']}")
    print(f"  Average turn length: {stats['avg_turn_length']:.1f} words")

def example_2_efficient_indexed_loading():
    """
    Example 2: Efficient indexed loading for local files.

    This demonstrates how the indexing system improves performance.
    """
    print("\n=== Example 2: Efficient Indexed Loading ===")

    print_memory_usage("Before loading dataset")

    # Initialize with lazy loading
    sporc = SPORCDataset(
        local_data_dir="data",
        load_turns_eagerly=False,
        streaming=False
    )

    print_memory_usage("After loading dataset (without turns)")

    # Check index status
    index_status = sporc.get_index_status()
    print(f"Index status: {index_status}")

    # Get episodes for testing
    episodes = sporc.search_episodes(min_duration=1800)[:3]
    print(f"Selected {len(episodes)} episodes for testing")

    # Load turns efficiently using the index
    print("\nLoading turns using efficient indexed access...")
    start_time = time.time()
    sporc.preload_turns_for_episodes(episodes)
    load_time = time.time() - start_time

    print(f"Loaded turns for {len(episodes)} episodes in {load_time:.3f} seconds")
    print_memory_usage("After loading turns for multiple episodes")

    # Show results
    for episode in episodes:
        stats = episode.get_turn_statistics()
        print(f"  {episode.title}: {stats['total_turns']} turns, {stats['total_words']} words")

def example_3_batch_loading_comparison():
    """
    Example 3: Comparison between individual and batch loading.

    This demonstrates the efficiency of batch loading multiple episodes.
    """
    print("\n=== Example 3: Batch Loading Comparison ===")

    print_memory_usage("Before loading dataset")

    # Initialize with lazy loading
    sporc = SPORCDataset(
        local_data_dir="data",
        load_turns_eagerly=False,
        streaming=False
    )

    print_memory_usage("After loading dataset (without turns)")

    # Get episodes for testing
    episodes = sporc.search_episodes(min_duration=1800)[:5]
    print(f"Selected {len(episodes)} episodes for testing")

    # Method 1: Load episodes individually
    print("\nMethod 1: Loading episodes individually")
    start_time = time.time()
    for episode in episodes:
        sporc.load_turns_for_episode(episode)
    individual_time = time.time() - start_time
    print(f"Individual loading time: {individual_time:.3f} seconds")

    # Reset episodes (clear loaded turns)
    for episode in episodes:
        episode._turns = []
        episode._turns_loaded = False

    # Method 2: Load episodes in batch
    print("\nMethod 2: Loading episodes in batch")
    start_time = time.time()
    sporc.preload_turns_for_episodes(episodes)
    batch_time = time.time() - start_time
    print(f"Batch loading time: {batch_time:.3f} seconds")

    print(f"\nPerformance improvement: {individual_time/batch_time:.1f}x faster with batch loading")

def example_4_selective_podcast_loading():
    """
    Example 4: Loading turns for specific podcasts efficiently.

    This shows how to load turns for entire podcasts using the index.
    """
    print("\n=== Example 4: Selective Podcast Loading ===")

    print_memory_usage("Before loading dataset")

    # Initialize with lazy loading
    sporc = SPORCDataset(
        local_data_dir="data",
        load_turns_eagerly=False,
        streaming=False
    )

    print_memory_usage("After loading dataset (without turns)")

    # Get a podcast
    podcasts = sporc.get_all_podcasts()
    if podcasts:
        podcast = podcasts[0]
        print(f"Found podcast: {podcast.title} with {len(podcast.episodes)} episodes")

        # Load turns for the entire podcast efficiently
        print("Loading turns for entire podcast...")
        start_time = time.time()
        sporc.load_turns_for_podcast(podcast)
        load_time = time.time() - start_time

        print(f"Loaded turns in {load_time:.3f} seconds")
        print_memory_usage("After loading podcast turns")

        # Analyze the podcast
        total_turns = 0
        total_words = 0

        for episode in podcast.episodes:
            stats = episode.get_turn_statistics()
            total_turns += stats['total_turns']
            total_words += stats['total_words']
            print(f"  {episode.title}: {stats['total_turns']} turns, {stats['total_words']} words")

        print(f"\nPodcast total: {total_turns} turns, {total_words} words")
    else:
        print("No podcasts found in dataset")

def example_5_async_index_building():
    """
    Example 5: Asynchronous index building.

    This demonstrates building the index in the background while using the dataset.
    """
    print("\n=== Example 5: Asynchronous Index Building ===")

    print_memory_usage("Before loading dataset")

    # Initialize with lazy loading
    sporc = SPORCDataset(
        local_data_dir="data",
        load_turns_eagerly=False,
        streaming=False
    )

    print_memory_usage("After loading dataset (without turns)")

    # Start index building in background
    print("Starting index building in background...")
    sporc.build_turn_index_async()

    # Use the dataset while index is building
    print("Using dataset while index builds in background...")

    # Explore episodes
    episodes = sporc.search_episodes(min_duration=1800)[:3]
    for episode in episodes:
        print(f"Episode: {episode.title} ({episode.duration_minutes:.1f} min)")

    # Check index status periodically
    for i in range(5):
        time.sleep(2)  # Wait 2 seconds
        index_status = sporc.get_index_status()
        print(f"Index status after {i*2+2}s: built={index_status['index_built']}, episodes={index_status['episodes_indexed']}")

        if index_status['index_built']:
            print("Index building completed!")
            break

    print_memory_usage("After background index building")

def example_6_memory_efficiency_comparison():
    """
    Example 6: Memory efficiency comparison between approaches.

    This demonstrates the memory savings of indexed lazy loading.
    """
    print("\n=== Example 6: Memory Efficiency Comparison ===")

    # Test 1: Eager loading (loads all turns)
    print("Test 1: Eager loading (loads all turns)")
    print_memory_usage("Before eager loading")

    sporc_eager = SPORCDataset(
        local_data_dir="data",
        load_turns_eagerly=True,  # Eager loading
        streaming=False
    )

    print_memory_usage("After eager loading")

    # Test 2: Lazy loading with index (loads only metadata + index)
    print("\nTest 2: Lazy loading with index")
    print_memory_usage("Before lazy loading")

    sporc_lazy = SPORCDataset(
        local_data_dir="data",
        load_turns_eagerly=False,  # Lazy loading
        streaming=False
    )

    print_memory_usage("After lazy loading (metadata + index)")

    # Test 3: Load turns for specific episodes
    episodes = sporc_lazy.search_episodes(min_duration=1800)[:3]
    sporc_lazy.preload_turns_for_episodes(episodes)

    print_memory_usage("After loading turns for 3 episodes")

    # Calculate memory savings
    eager_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
    lazy_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024

    print(f"\nMemory comparison:")
    print(f"  Eager loading: {eager_memory:.1f} MB")
    print(f"  Lazy loading (3 episodes): {lazy_memory:.1f} MB")
    print(f"  Memory savings: {((eager_memory - lazy_memory) / eager_memory * 100):.1f}%")

def example_7_large_dataset_workflow():
    """
    Example 7: Workflow for large datasets using efficient lazy loading.

    This demonstrates a complete workflow optimized for large datasets.
    """
    print("\n=== Example 7: Large Dataset Workflow ===")

    print_memory_usage("Start")

    # Step 1: Initialize with lazy loading
    sporc = SPORCDataset(
        local_data_dir="data",
        load_turns_eagerly=False,
        streaming=False
    )
    print_memory_usage("After dataset initialization")

    # Step 2: Build index (if not already built)
    index_status = sporc.get_index_status()
    if not index_status['index_built']:
        print("Building turn index...")
        sporc._build_turn_index()
        print_memory_usage("After building index")

    # Step 3: Explore and filter episodes
    print("Exploring dataset...")
    all_episodes = sporc.get_all_episodes()
    print(f"Total episodes: {len(all_episodes)}")

    # Filter episodes by criteria
    long_episodes = [ep for ep in all_episodes if ep.duration_minutes > 60]
    interview_episodes = [ep for ep in all_episodes if ep.num_guests > 0]

    print(f"Long episodes: {len(long_episodes)}")
    print(f"Interview episodes: {len(interview_episodes)}")
    print_memory_usage("After exploration and filtering")

    # Step 4: Select episodes for analysis
    episodes_to_analyze = long_episodes[:10]  # First 10 long episodes
    print(f"Selected {len(episodes_to_analyze)} episodes for analysis")

    # Step 5: Load turns efficiently using index
    print("Loading turns for selected episodes...")
    start_time = time.time()
    sporc.preload_turns_for_episodes(episodes_to_analyze)
    load_time = time.time() - start_time

    print(f"Loaded turns in {load_time:.3f} seconds")
    print_memory_usage("After loading turns for selected episodes")

    # Step 6: Perform analysis
    print("Performing analysis...")
    total_turns = 0
    total_words = 0

    for episode in episodes_to_analyze:
        stats = episode.get_turn_statistics()
        total_turns += stats['total_turns']
        total_words += stats['total_words']
        print(f"  {episode.title}: {stats['total_turns']} turns, {stats['total_words']} words")

    print(f"\nAnalysis complete:")
    print(f"  Total turns analyzed: {total_turns}")
    print(f"  Total words analyzed: {total_words}")
    print(f"  Average turns per episode: {total_turns/len(episodes_to_analyze):.1f}")
    print(f"  Average words per episode: {total_words/len(episodes_to_analyze):.1f}")

    print_memory_usage("After analysis")

if __name__ == "__main__":
    print("SPORC Dataset Lazy Loading Examples")
    print("=" * 45)

    try:
        example_1_basic_lazy_loading()
        example_2_efficient_indexed_loading()
        example_3_batch_loading_comparison()
        example_4_selective_podcast_loading()
        example_5_async_index_building()
        example_6_memory_efficiency_comparison()
        example_7_large_dataset_workflow()

    except Exception as e:
        print(f"Error running examples: {e}")
        print("Make sure you have the SPORC dataset files in the 'data' directory")