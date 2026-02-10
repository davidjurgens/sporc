#!/usr/bin/env python3
"""
Command-line interface for the SPORC package.
"""

import argparse
import sys
from typing import Optional

from . import SPORCDataset, SPORCError


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="SPORC: Structured Podcast Open Research Corpus CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Get dataset statistics
  sporc stats

  # Get stats from a local parquet directory
  sporc stats --parquet-dir /path/to/parquet

  # Search for a podcast
  sporc search-podcast "SingOut SpeakOut"

  # Search for episodes with criteria
  sporc search-episodes --min-duration 1800 --category education
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Get dataset statistics")
    stats_parser.add_argument(
        "--parquet-dir", help="Local directory with parquet files (downloads from HF if omitted)"
    )

    # Search podcast command
    search_podcast_parser = subparsers.add_parser(
        "search-podcast", help="Search for a podcast by name"
    )
    search_podcast_parser.add_argument("name", help="Podcast name to search for")
    search_podcast_parser.add_argument(
        "--parquet-dir", help="Local directory with parquet files (downloads from HF if omitted)"
    )

    # Search episodes command
    search_episodes_parser = subparsers.add_parser(
        "search-episodes", help="Search for episodes with criteria"
    )
    search_episodes_parser.add_argument(
        "--min-duration", type=int, help="Minimum duration in seconds"
    )
    search_episodes_parser.add_argument(
        "--max-duration", type=int, help="Maximum duration in seconds"
    )
    search_episodes_parser.add_argument(
        "--min-speakers", type=int, help="Minimum number of speakers"
    )
    search_episodes_parser.add_argument(
        "--max-speakers", type=int, help="Maximum number of speakers"
    )
    search_episodes_parser.add_argument(
        "--host-name", help="Host name to search for"
    )
    search_episodes_parser.add_argument(
        "--category", help="Category to search for"
    )
    search_episodes_parser.add_argument(
        "--subcategory", help="Subcategory to search for"
    )
    search_episodes_parser.add_argument(
        "--limit", type=int, default=10, help="Maximum number of results to show"
    )
    search_episodes_parser.add_argument(
        "--parquet-dir", help="Local directory with parquet files (downloads from HF if omitted)"
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    try:
        if args.command == "stats":
            handle_stats(args)
        elif args.command == "search-podcast":
            handle_search_podcast(args)
        elif args.command == "search-episodes":
            handle_search_episodes(args)
        else:
            print(f"Unknown command: {args.command}")
            sys.exit(1)

    except SPORCError as e:
        print(f"SPORC Error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


def handle_stats(args: argparse.Namespace) -> None:
    """Handle the stats command."""
    print("Loading SPORC dataset...")

    sporc = SPORCDataset(parquet_dir=getattr(args, 'parquet_dir', None))

    # Get statistics
    stats = sporc.get_dataset_statistics()

    print("\n=== SPORC Dataset Statistics ===")
    print(f"Total podcasts: {stats['total_podcasts']}")
    print(f"Total episodes: {stats['total_episodes']}")
    print(f"Total duration: {stats['total_duration_hours']:.1f} hours")
    print(f"Average episode length: {stats['avg_episode_duration_minutes']:.1f} minutes")

    print("\nTop categories:")
    for category, count in sorted(
        stats['category_distribution'].items(),
        key=lambda x: x[1],
        reverse=True
    )[:10]:
        print(f"  {category}: {count} episodes")

    print("\nLanguage distribution:")
    for language, count in sorted(
        stats['language_distribution'].items(),
        key=lambda x: x[1],
        reverse=True
    )[:5]:
        print(f"  {language}: {count} episodes")

    print("\nSpeaker distribution:")
    for speakers, count in sorted(stats['speaker_distribution'].items()):
        print(f"  {speakers} speakers: {count} episodes")

    if 'episode_types' in stats:
        print("\nEpisode types:")
        for episode_type, count in stats['episode_types'].items():
            print(f"  {episode_type}: {count} episodes")


def handle_search_podcast(args: argparse.Namespace) -> None:
    """Handle the search-podcast command."""
    print(f"Searching for podcast: {args.name}")

    sporc = SPORCDataset(parquet_dir=getattr(args, 'parquet_dir', None))

    # Search for podcast
    podcast = sporc.search_podcast(args.name)

    print(f"\n=== Podcast: {podcast.title} ===")
    print(f"Description: {podcast.description}")
    print(f"Number of episodes: {podcast.num_episodes}")
    print(f"Total duration: {podcast.total_duration_hours:.1f} hours")
    print(f"Hosts: {', '.join(podcast.host_names)}")
    print(f"Categories: {', '.join(podcast.categories)}")

    print(f"\nEpisodes:")
    for i, episode in enumerate(podcast.episodes[:5]):  # Show first 5 episodes
        print(f"  {i+1}. {episode.title}")
        print(f"     Duration: {episode.duration_minutes:.1f} minutes")
        print(f"     Speakers: {episode.num_main_speakers}")

    if len(podcast.episodes) > 5:
        print(f"  ... and {len(podcast.episodes) - 5} more episodes")


def handle_search_episodes(args: argparse.Namespace) -> None:
    """Handle the search-episodes command."""
    print("Searching for episodes...")

    # Build search criteria
    criteria = {}
    if args.min_duration:
        criteria['min_duration'] = args.min_duration
    if args.max_duration:
        criteria['max_duration'] = args.max_duration
    if args.min_speakers:
        criteria['min_speakers'] = args.min_speakers
    if args.max_speakers:
        criteria['max_speakers'] = args.max_speakers
    if args.host_name:
        criteria['host_name'] = args.host_name
    if args.category:
        criteria['category'] = args.category
    if args.subcategory:
        criteria['subcategory'] = args.subcategory

    sporc = SPORCDataset(parquet_dir=getattr(args, 'parquet_dir', None))

    # Search for episodes
    episodes = sporc.search_episodes(**criteria)

    print(f"\nFound {len(episodes)} episodes matching criteria: {criteria}")

    if episodes:
        print(f"\nShowing first {args.limit} episodes:")
        for i, episode in enumerate(episodes[:args.limit]):
            print(f"\n{i+1}. {episode.title}")
            print(f"   Podcast: {episode.podcast_title}")
            print(f"   Duration: {episode.duration_minutes:.1f} minutes")
            print(f"   Speakers: {episode.num_main_speakers}")
            print(f"   Hosts: {', '.join(episode.host_names)}")
            print(f"   Categories: {', '.join(episode.categories)}")

        if len(episodes) > args.limit:
            print(f"\n... and {len(episodes) - args.limit} more episodes")


if __name__ == "__main__":
    main()
