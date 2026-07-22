#!/bin/bash
# Both search databases, rebuilt from the deduplicated turns.
set -e
cd "$(dirname "$0")"
echo "=== turns_search.duckdb ==="
python3 stage_search.py
echo "=== turns_text.duckdb ==="
python3 stage_search_text.py
echo "=== ALL DONE ==="
