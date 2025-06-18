#!/usr/bin/env python3
"""
Advanced analysis example for the SPORC package.

This example demonstrates more sophisticated analysis capabilities,
including conversation flow analysis, speaker interaction patterns,
and data quality assessment.
"""

import sys
import os
from collections import Counter, defaultdict
from typing import Dict, List, Tuple

# Add the parent directory to the path so we can import the sporc package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from sporc import SPORCDataset, SPORCError


def analyze_conversation_flow(episode):
    """Analyze the conversation flow and speaker interaction patterns."""
    print(f"\n=== Conversation Flow Analysis for '{episode.title}' ===")

    turns = episode.get_all_turns()
    if not turns:
        print("No turns available for analysis.")
        return

    # Speaker transition analysis
    speaker_transitions = []
    for i in range(len(turns) - 1):
        current_speaker = turns[i].primary_speaker
        next_speaker = turns[i + 1].primary_speaker
        speaker_transitions.append((current_speaker, next_speaker))

    transition_counts = Counter(speaker_transitions)

    print(f"Total turns: {len(turns)}")
    print(f"Unique speakers: {len(set(turn.primary_speaker for turn in turns))}")
    print(f"Most common speaker transitions:")
    for (speaker1, speaker2), count in transition_counts.most_common(5):
        print(f"  {speaker1} → {speaker2}: {count} times")

    # Turn duration analysis
    durations = [turn.duration for turn in turns]
    avg_duration = sum(durations) / len(durations)
    max_duration = max(durations)
    min_duration = min(durations)

    print(f"\nTurn duration statistics:")
    print(f"  Average: {avg_duration:.1f} seconds")
    print(f"  Maximum: {max_duration:.1f} seconds")
    print(f"  Minimum: {min_duration:.1f} seconds")

    # Speaker dominance analysis
    speaker_durations = defaultdict(float)
    speaker_turn_counts = defaultdict(int)

    for turn in turns:
        speaker = turn.primary_speaker
        speaker_durations[speaker] += turn.duration
        speaker_turn_counts[speaker] += 1

    print(f"\nSpeaker dominance (by speaking time):")
    for speaker, duration in sorted(speaker_durations.items(), key=lambda x: x[1], reverse=True):
        percentage = (duration / episode.duration_seconds) * 100
        print(f"  {speaker}: {duration:.1f}s ({percentage:.1f}%) - {speaker_turn_counts[speaker]} turns")


def analyze_data_quality(episode):
    """Analyze the quality of diarization and speaker identification."""
    print(f"\n=== Data Quality Analysis for '{episode.title}' ===")

    print(f"Episode duration: {episode.duration_minutes:.1f} minutes")
    print(f"Number of main speakers: {episode.num_main_speakers}")
    print(f"Total speaker labels: {episode.total_speaker_labels}")

    # Quality indicators
    print(f"\nQuality indicators:")
    print(f"  Overlap proportion (duration): {episode.overlap_prop_duration:.3f}")
    print(f"  Overlap proportion (turn count): {episode.overlap_prop_turn_count:.3f}")
    print(f"  Average turn duration: {episode.avg_turn_duration:.1f} seconds")

    # Quality assessment
    quality_score = 0
    quality_notes = []

    if episode.overlap_prop_duration < 0.1:
        quality_score += 1
        quality_notes.append("Low speaker overlap (good)")
    else:
        quality_notes.append("High speaker overlap (may indicate diarization issues)")

    if episode.overlap_prop_turn_count < 0.2:
        quality_score += 1
        quality_notes.append("Low turn overlap (good)")
    else:
        quality_notes.append("High turn overlap (may indicate diarization issues)")

    if episode.avg_turn_duration > 10:
        quality_score += 1
        quality_notes.append("Reasonable turn durations")
    else:
        quality_notes.append("Very short turn durations (may indicate over-segmentation)")

    if episode.num_main_speakers == episode.total_speaker_labels:
        quality_score += 1
        quality_notes.append("Consistent speaker labeling")
    else:
        quality_notes.append("Inconsistent speaker labeling")

    print(f"\nQuality score: {quality_score}/4")
    print("Quality notes:")
    for note in quality_notes:
        print(f"  - {note}")


def analyze_content_patterns(episode):
    """Analyze content patterns and linguistic features."""
    print(f"\n=== Content Analysis for '{episode.title}' ===")

    turns = episode.get_all_turns()
    if not turns:
        print("No turns available for analysis.")
        return

    # Word count analysis
    total_words = sum(turn.word_count for turn in turns)
    avg_words_per_turn = total_words / len(turns)

    print(f"Total words: {total_words}")
    print(f"Average words per turn: {avg_words_per_turn:.1f}")

    # Speaking rate analysis
    speaking_rates = []
    for turn in turns:
        if turn.duration > 0:
            rate = turn.words_per_second
            speaking_rates.append(rate)

    if speaking_rates:
        avg_rate = sum(speaking_rates) / len(speaking_rates)
        print(f"Average speaking rate: {avg_rate:.2f} words/second")

    # Long vs short turns analysis
    long_turns = [turn for turn in turns if turn.word_count > 50]
    short_turns = [turn for turn in turns if turn.word_count <= 10]

    print(f"Long turns (>50 words): {len(long_turns)} ({len(long_turns)/len(turns)*100:.1f}%)")
    print(f"Short turns (≤10 words): {len(short_turns)} ({len(short_turns)/len(turns)*100:.1f}%)")

    # Role-based analysis
    host_turns = episode.get_host_turns()
    guest_turns = episode.get_guest_turns()

    if host_turns:
        host_avg_words = sum(turn.word_count for turn in host_turns) / len(host_turns)
        print(f"Host average words per turn: {host_avg_words:.1f}")

    if guest_turns:
        guest_avg_words = sum(turn.word_count for turn in guest_turns) / len(guest_turns)
        print(f"Guest average words per turn: {guest_avg_words:.1f}")


def analyze_podcast_patterns(podcast):
    """Analyze patterns across all episodes in a podcast."""
    print(f"\n=== Podcast Pattern Analysis for '{podcast.title}' ===")

    episodes = podcast.episodes
    if not episodes:
        print("No episodes available for analysis.")
        return

    # Episode type distribution
    solo_count = len([ep for ep in episodes if ep.is_solo])
    interview_count = len([ep for ep in episodes if ep.is_interview])
    panel_count = len([ep for ep in episodes if ep.is_panel])

    print(f"Episode type distribution:")
    print(f"  Solo episodes: {solo_count} ({solo_count/len(episodes)*100:.1f}%)")
    print(f"  Interview episodes: {interview_count} ({interview_count/len(episodes)*100:.1f}%)")
    print(f"  Panel episodes: {panel_count} ({panel_count/len(episodes)*100:.1f}%)")

    # Duration patterns
    durations = [ep.duration_minutes for ep in episodes]
    avg_duration = sum(durations) / len(durations)
    min_duration = min(durations)
    max_duration = max(durations)

    print(f"\nDuration patterns:")
    print(f"  Average episode length: {avg_duration:.1f} minutes")
    print(f"  Range: {min_duration:.1f} - {max_duration:.1f} minutes")

    # Speaker count patterns
    speaker_counts = [ep.num_main_speakers for ep in episodes]
    avg_speakers = sum(speaker_counts) / len(speaker_counts)

    print(f"  Average speakers per episode: {avg_speakers:.1f}")

    # Guest patterns
    episodes_with_guests = [ep for ep in episodes if ep.has_guests]
    print(f"  Episodes with guests: {len(episodes_with_guests)} ({len(episodes_with_guests)/len(episodes)*100:.1f}%)")

    # Quality patterns
    high_quality_episodes = [
        ep for ep in episodes
        if ep.overlap_prop_duration < 0.1 and ep.overlap_prop_turn_count < 0.2
    ]
    print(f"  High-quality episodes: {len(high_quality_episodes)} ({len(high_quality_episodes)/len(episodes)*100:.1f}%)")


def main():
    """Main function demonstrating advanced SPORC analysis."""

    print("=== SPORC Advanced Analysis Example ===\n")

    try:
        # Initialize the dataset
        print("Loading SPORC dataset...")
        sporc = SPORCDataset()
        print(f"✓ Loaded dataset with {len(sporc)} episodes\n")

        # Get dataset overview
        stats = sporc.get_dataset_statistics()
        print("Dataset Overview:")
        print(f"  Total podcasts: {stats['total_podcasts']}")
        print(f"  Total episodes: {stats['total_episodes']}")
        print(f"  Total duration: {stats['total_duration_hours']:.1f} hours")
        print(f"  Average episode length: {stats['avg_episode_duration_minutes']:.1f} minutes")

        # Category distribution
        print(f"\nTop categories:")
        for category, count in sorted(stats['category_distribution'].items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"  {category}: {count} episodes")

        # Find a good podcast for analysis
        print(f"\nSearching for podcasts with multiple episodes...")
        podcasts = sporc.get_all_podcasts()
        multi_episode_podcasts = [p for p in podcasts if p.num_episodes >= 3]

        if multi_episode_podcasts:
            podcast = multi_episode_podcasts[0]
            print(f"Selected podcast: {podcast.title} ({podcast.num_episodes} episodes)")

            # Analyze podcast patterns
            analyze_podcast_patterns(podcast)

            # Analyze a specific episode
            if podcast.episodes:
                episode = podcast.episodes[0]
                print(f"\nAnalyzing episode: {episode.title}")

                # Load turns if not already loaded
                if not episode._turns_loaded:
                    print("Loading turn data...")
                    # This would normally be done automatically, but we'll simulate it
                    print("(Turn data loading simulated)")

                # Perform analyses
                analyze_conversation_flow(episode)
                analyze_data_quality(episode)
                analyze_content_patterns(episode)

        # Search for specific patterns
        print(f"\n=== Pattern Search ===")

        # Find long-form interviews
        long_interviews = sporc.search_episodes(
            min_duration=1800,  # At least 30 minutes
            min_speakers=2,     # At least 2 speakers
            max_overlap_prop_duration=0.1  # Good quality
        )
        print(f"Long-form interviews (30+ min, 2+ speakers, good quality): {len(long_interviews)}")

        # Find solo episodes
        solo_episodes = sporc.search_episodes(
            max_speakers=1,
            min_duration=300  # At least 5 minutes
        )
        print(f"Solo episodes (1 speaker, 5+ minutes): {len(solo_episodes)}")

        # Find education podcasts
        education_episodes = sporc.search_episodes(category="education")
        print(f"Education episodes: {len(education_episodes)}")

        print(f"\n=== Advanced Analysis Completed ===")

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