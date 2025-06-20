#!/usr/bin/env python3
"""
Test script to verify that progress logging is now at DEBUG level.
"""

import logging
from sporc import SPORCDataset

# Set up logging to show the difference
print("Testing with default INFO level logging:")
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# This should show only INFO and above messages, not DEBUG progress messages
print("\n--- Testing dataset iteration with INFO level ---")
try:
    dataset = SPORCDataset(streaming=True)
    podcast_count = 0
    for podcast in dataset.iterate_podcasts():
        podcast_count += 1
        if podcast_count >= 2:  # Just test a couple
            break
    print(f"Successfully iterated over {podcast_count} podcasts")
except Exception as e:
    print(f"Error: {e}")

print("\n" + "="*50)
print("Testing with DEBUG level logging:")
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

# This should show DEBUG progress messages
print("\n--- Testing dataset iteration with DEBUG level ---")
try:
    dataset = SPORCDataset(streaming=True)
    podcast_count = 0
    for podcast in dataset.iterate_podcasts():
        podcast_count += 1
        if podcast_count >= 2:  # Just test a couple
            break
    print(f"Successfully iterated over {podcast_count} podcasts")
except Exception as e:
    print(f"Error: {e}")

print("\n✓ Test completed! Progress logging should now be at DEBUG level.")