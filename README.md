# SPORC: Structured Podcast Open Research Corpus

[![Documentation Status](https://readthedocs.org/projects/sporc/badge/?version=latest)](https://sporc.readthedocs.io/en/latest/)
[![PyPI](https://img.shields.io/pypi/v/sporc)](https://pypi.org/project/sporc/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A Python package for working with the [SPoRC dataset](https://huggingface.co/datasets/blitt/SPoRC)
— 228,099 podcasts and 1,124,058 episodes of transcripts, speaker turns, and
acoustic features.

The dataset itself (schema, columns, layout, terms of use) is documented on the
[dataset card](https://huggingface.co/datasets/blitt/SPoRC). This README covers
the Python package.

📖 **Full documentation:** [sporc.readthedocs.io](https://sporc.readthedocs.io) —
guides and a complete API reference.
🎓 **Tutorials:** [eight worked notebooks](examples/notebooks/) built around real
research questions.

## Installation

The dataset is gated, so before installing:

1. **Accept the terms** at [huggingface.co/datasets/blitt/SPoRC](https://huggingface.co/datasets/blitt/SPoRC)
   — log in and click "Agree".
2. **Authenticate** locally:
   ```bash
   pip install huggingface_hub
   hf auth login
   ```
   (On older `huggingface_hub` this command was `huggingface-cli login`.)

Then:

```bash
pip install sporc
```

Or from source:

```bash
git clone https://github.com/davidjurgens/sporc.git
cd sporc
pip install -e .
```

Full-text search additionally needs DuckDB: `pip install sporc[duckdb]`.

## Quick Start

```python
from sporc import SPORCDataset

# Downloads the metadata catalogs (~195 MB). Episode and turn data is fetched
# per podcast, as you touch it.
sporc = SPORCDataset()

# search_podcast matches by exact title first, then falls back to a substring
# match — so prefer the full, exact title (or a podcast_id) to avoid surprises.
podcast = sporc.search_podcast("The NPR Politics Podcast")
print(podcast.title, podcast.num_episodes, "episodes")

for episode in podcast.episodes:
    print(f"{episode.title} — {episode.duration_minutes:.0f} min")

    # Only ~65% of episodes were diarized; check before using turns.
    if episode.has_turn_data:
        for turn in episode.turns[:3]:
            print(f"  [{turn.inferred_speaker_role}] {turn.text[:60]}...")
```

## Data Access

The corpus is ~57 GB and partitioned by podcast, so **the podcast is the unit of
transfer**. A median podcast is ~75 KB, which means the one-time ~195 MB of
catalogs dominates any study of less than a few thousand podcasts:

| Study size | Data fetched |
|---|---|
| 10 podcasts | ~750 KB |
| 200 podcasts | ~15 MB |
| 1,000 podcasts | ~75 MB |
| whole corpus | ~31 GB |

```python
# Default: catalogs now, per-podcast data on demand.
sporc = SPORCDataset()

# Fetch a known slice up front. Accepts podcast ids, podcast titles, episode
# ids, or a path to a .json / newline-delimited .txt file of them.
sporc = SPORCDataset(subset=["The NPR Politics Podcast", "Stuff You Should Know"])
sporc = SPORCDataset(subset="my_podcast_ids.txt")

# Pin a run to that slice: anything outside it raises DataNotLocalError
# rather than quietly downloading more.
sporc = SPORCDataset(subset="my_ids.txt", allow_downloads=False)

# Use a local copy of the layout; never downloads anything.
sporc = SPORCDataset(parquet_dir="/path/to/sporc_parquet")

# Download the whole corpus up front (~31 GB, ~685k files).
sporc = SPORCDataset(lazy=False)
```

`prefetch()` does the same job after construction, and reports what it resolved:

```python
sporc.prefetch(["The NPR Politics Podcast"])
# {'podcasts': 1, 'files': 4, 'unresolved': []}
```

### Selecting a subset efficiently

**Selection is metadata-only.** These run off the already-downloaded catalogs
and fetch nothing, so you can narrow to exactly the episodes you want and only
then pull data:

```python
hits = sporc.filter_episodes_by_metrics(min_word_count=5000, limit=200)
hits = sporc.search_by_speaker_name("Ira Glass", role="host")

sporc.prefetch({"episode_ids": [h["episode_id"] for h in hits]})
```

Two things worth knowing:

- **Pass `max_episodes` to `search_episodes`.** Matching is metadata-only, but
  building each `Episode` reads that podcast's partition. `category="comedy"`
  matches 62,622 episodes across 14,668 podcasts; `max_episodes=10` reads 10
  partitions instead.
- **`filter_episodes_by_metrics` implies turn data.** `episode_metrics` is
  derived from turns, so it only covers the 372,604 diarized episodes and never
  wastes a fetch on an episode without them. Filtering the catalog directly,
  `num_main_speakers > 0` marks the same set.

### Time span

SPoRC is a **two-month snapshot**: every episode was published between
**1 May and 30 June 2020** (median 28 May). It is not a longitudinal archive, so
"trends over time" means trends across eight weeks — though the window does
straddle a sharp, dateable event, which makes before/after designs unusually
clean.

### Turn coverage

Only **731,101 of 1,124,058 episodes (65%)** were diarized into speaker turns.
The rest have a transcript but are marked `SPEAKER_DATA_UNAVAILABLE` upstream.
Dataset version 1.1 roughly doubled this: 1.0 had 372,604 (33%), and 358,497
episodes that had been diarized but never merged were added.
An empty `episode.turns` is therefore usually a gap in the corpus rather than a
fact about the episode:

```python
if episode.has_turn_data:
    analyze(episode.turns)
```

### Text search

`search_turns`, `search_episodes_by_text` and `concordance` use the DuckDB
full-text index when present, and otherwise scan the turn partitions on disk:

| Local data | Scan (pyarrow) | 26 GB DuckDB index |
|---|---|---|
| 250 episodes (5 MB) | 0.37s | n/a — needs the full 26 GB |
| 1,000 episodes (17 MB) | **1.1s** | " |
| 5,000 episodes (83 MB) | 5.4s | " |
| all 78M turns | impractical | 1.7s open, ~5s/query (50s cold) |

The index earns its 26 GB only for whole-corpus work; below ~5,000 episodes,
scanning beats its cold start alone.

```python
sporc = SPORCDataset(include_search_db=True)   # whole corpus; pip install duckdb

results = sporc.search_turns("artificial intelligence")      # fts (BM25)
results = sporc.search_turns("climate", mode="exact")        # substring
results = sporc.concordance("like", context_words=5)         # KWIC
```

Without the index, `mode="fts"` ranks by term frequency rather than BM25 and
covers only local data; the package warns, naming how many podcasts it scanned.

### Building teaching subsets

`scripts/make_subset.py` cuts a self-contained mini-SPoRC, filtering the
catalogs *and* episode partitions together so counts, searches and statistics
are all true of the subset:

```bash
# Ten disjoint 1k-episode subsets (~57 MB each)
for i in $(seq 1 10); do
  python scripts/make_subset.py --data-dir /path/to/sporc_parquet \
      --out subsets/subset_$i --episodes 1000 --seed $i \
      --exclude-used subsets/used.txt
done
```

Subsets are diarized-only by default, so every episode has turns; pass
`--include-undiarized` to mirror the real corpus's ~65% coverage. Learners point
at the directory and nothing downloads:

```python
sporc = SPORCDataset(parquet_dir="subsets/subset_1")
```

## Tutorials

Eight standalone notebooks in [`examples/notebooks/`](examples/notebooks/), each
framed around a real research question. They run against a small pre-built
tutorial subset (not the full 57 GB corpus) — see the
[notebooks README](examples/notebooks/README.md) for setup. **Start with 01**; it
establishes the corpus caveats the rest depend on.

| # | Notebook | Question |
|---|---|---|
| 01 | [Corpus cartography](examples/notebooks/01_corpus_cartography.ipynb) | What is actually in SPoRC? |
| 02 | [NER co-mention networks](examples/notebooks/02_ner_comention_networks.ipynb) | Who gets talked about together? |
| 03 | [Host/guest networks](examples/notebooks/03_host_guest_networks.ipynb) | Which shows share guests? |
| 04 | [Repeat-guest language](examples/notebooks/04_repeat_guest_language.ipynb) | Do repeat guests reuse material? |
| 05 | [Stance over time](examples/notebooks/05_stance_over_time.ipynb) | How did talk change around 25 May 2020? |
| 06 | [Topic modeling (MALLET)](examples/notebooks/06_topic_modeling_mallet.ipynb) | What is podcasting about? |
| 07 | [Sociophonetics: caught/cot](examples/notebooks/07_sociophonetics_caught_cot.ipynb) | Does this speaker merge *caught* and *cot*? |
| 08 | [Conversational dynamics](examples/notebooks/08_conversational_dynamics.ipynb) | Who talks longest, and where do people overlap? |

> If you only read one, read **07** — it starts from a question the corpus
> *cannot* answer and shows what to do about it.

Shorter, single-topic example scripts live in [`examples/`](examples/)
(`basic_usage.py`, `category_examples.py`, `sliding_window_examples.py`,
`time_range_examples.py`, `advanced_analysis.py`).

## Core Classes

### `SPORCDataset`

Search and retrieval over the corpus.

```python
sporc.search_podcast("The NPR Politics Podcast")  # -> Podcast
sporc.search_episodes(min_duration=1800, category="science", max_episodes=20)
sporc.search_episodes_by_subcategory("Technology", max_episodes=10)
sporc.get_dataset_statistics()                    # counts, distributions

for episode in sporc.iterate_episodes(max_episodes=100, sampling_mode="random"):
    ...

# Precomputed metrics (diarized episodes only)
sporc.get_episode_metrics(episode_id)
sporc.filter_episodes_by_metrics(min_turn_count=50, max_host_proportion=0.6)
sporc.get_turn_metrics(podcast_id, episode_id)
```

### `Podcast`

```python
podcast.title, podcast.num_episodes, podcast.primary_category
podcast.total_duration_hours, podcast.avg_episode_duration_minutes
podcast.host_names, podcast.categories

podcast.get_episodes_by_host("Jad Abumrad")
podcast.get_episodes_by_duration_range(600, 1800)
podcast.interview_episodes, podcast.solo_episodes, podcast.panel_episodes
```

### `Episode`

```python
episode.title, episode.duration_minutes, episode.transcript
episode.host_names, episode.guest_names, episode.num_main_speakers
episode.primary_category, episode.categories

episode.has_turn_data        # False when the corpus has no turns for it
episode.turns                # lazily loaded
episode.turn_count
episode.get_turns_by_time_range(0, 180)
episode.get_host_turns(), episode.get_guest_turns()
episode.get_turn_statistics()
```

### `Turn`

```python
turn.text, turn.speaker, turn.duration
turn.start_time, turn.end_time
turn.inferred_speaker_role   # "host" | "guest" | "NO_INFERRED_ROLE"
turn.inferred_speaker_name
turn.word_count, turn.words_per_second
turn.is_host, turn.is_guest
turn.get_audio_features()    # MFCCs, F0, F1
```

**The acoustics are thin.** `get_audio_features()` returns six numbers —
`mfcc1..4_sma3_mean`, one F0 mean, one F1 mean — each averaged over the *whole
turn*. There is no F2, no frame-level contour, and **no word-level timing
anywhere in SPoRC**. Vowel-level questions cannot be answered from these fields;
see [Phonetics](#phonetics) for the way around it.

**Role labels are sparse.** In a 174k-turn sample, 90.6% of turns are
`NO_INFERRED_ROLE`, with only 7.4% `host` and 1.9% `guest`. So `get_host_turns()`,
`search_turns(speaker_role=...)` and role distributions cover a small slice of an
episode, and an unlabelled turn does not mean nobody spoke. Treat role-based
counts as a lower bound rather than a partition of the conversation.

## Sliding Windows

Process long episodes in chunks, by turn count or by time:

```python
for window in episode.sliding_window(window_size=10, overlap=2):
    print(f"{window.size} turns, {window.duration/60:.1f} min")
    print(window.get_text())
    print(window.get_role_distribution())

for window in episode.sliding_window_by_time(window_duration=300, overlap_duration=60):
    ...
```

## Phonetics

SPoRC has no word timings, and its acoustics are six per-turn means with no F2 —
so vowel-level work is impossible from the corpus alone. But every turn carries an
`mp3_url` plus its own `start_time`/`end_time`, which is enough to fetch that turn
and derive alignment properly.

`sporc.phonetics` does that. It is optional and its dependencies are heavy, so it
is opt-in and imported lazily:

```bash
pip install sporc[phonetics]     # torch, torchaudio, transformers, parselmouth
                                 # plus an ffmpeg binary on PATH
```

```python
from sporc.phonetics import find_word_tokens, lobanov_normalize

# search -> fetch turn audio -> forced-align -> measure the vowel
tokens = find_word_tokens(sporc, "caught", limit=50)
df = lobanov_normalize(tokens)      # per-speaker z-scores; raw formants
                                    # mostly measure vocal-tract length
```

Lower-level pieces, if you want the steps:

```python
from sporc.phonetics import (fetch_turn_audio, align_turn, measure_formants,
                             stressed_vowel_index)

audio, sr = fetch_turn_audio(turn)                      # range-fetches ONLY the turn
words = align_turn(turn, audio=audio, sample_rate=sr, level="word")

# level="phone" aligns one word's clip, not a whole turn: forced alignment makes
# the phones explain all the audio you hand it, so slice the word out first.
hit = next(w for w in words if w.word.lower() == "caught")
clip = audio[int(hit.start * sr):int(hit.end * sr)]
phones = align_turn(turn, audio=clip, sample_rate=sr, level="phone", word="caught")

# Ask which phone is the stressed vowel rather than counting positions: it is
# the second phone in "caught", but the third in "across".
v = phones[stressed_vowel_index([p.arpabet for p in phones])]
measure_formants(clip, sr, v.start, v.end)              # F1/F2/F3
```

Worth knowing before you rely on it:

- **Only the turn is downloaded.** ffmpeg seeks into the remote mp3 with an HTTP
  range request: ~1–2 s and a few hundred KB per turn, not a 100 MB episode.
- **Aligning costs more than fetching.** Finding a word means aligning the whole
  turn, at ~0.45x realtime on CPU — so cost scales with the *turn*, not the word.
  Turns run long (median ~64 s for a common word, longest in the corpus 3,240 s),
  so `find_word_tokens` skips turns over `max_turn_duration=30` by default and
  logs what it dropped. A `limit=50` call is minutes, not seconds.
- **The audio is external.** `mp3_url` points at the publisher's CDN, not
  HuggingFace. Links rot; some hosts need redirects resolved first, which
  `fetch_turn_audio` does.
- **A speaker is a name.** `lobanov_normalize` groups by inferred name across
  episodes and shows, and drops `NO_INFERRED_SPEAKER` and raw `SPEAKER_00`
  labels rather than pooling them — the placeholder is the corpus's most common
  speaker value and would otherwise average unrelated people into one voice.
  Two real people sharing a name still pool; SPoRC has no canonical speaker id.
- **`estimate_word_audio()` is not this.** It interpolates by character offset and
  is deprecated for phonetic use.

`examples/notebooks/07_sociophonetics_caught_cot.ipynb` works the whole thing
through on the caught/cot merger.

## Error Handling

```python
from sporc import SPORCDataset, SPORCError, NotFoundError, DataNotLocalError

try:
    sporc = SPORCDataset(subset="ids.txt", allow_downloads=False)
    podcast = sporc.search_podcast("Some Podcast")
except NotFoundError:
    ...          # no such podcast in the catalog
except DataNotLocalError:
    ...          # needed data is not local and downloads are disabled
except SPORCError:
    ...          # base class for all package errors
```

## Development

```bash
pip install -e ".[dev]"

pytest                              # full suite
pytest -m "not slow and not integration"
pytest --cov=sporc

black sporc/ tests/
isort sporc/ tests/
flake8 sporc/ tests/
mypy sporc/
```

## Citation

If you use SPoRC in your research, please cite:

```bibtex
@inproceedings{litterer-etal-2025-mapping,
    title = "Mapping the Podcast Ecosystem with the Structured Podcast Research Corpus",
    author = "Litterer, Benjamin Roger and Jurgens, David and Card, Dallas",
    booktitle = "Proceedings of the 63rd Annual Meeting of the Association for Computational Linguistics (Volume 1: Long Papers)",
    month = jul,
    year = "2025",
    address = "Vienna, Austria",
    publisher = "Association for Computational Linguistics",
    url = "https://aclanthology.org/2025.acl-long.1222/",
    doi = "10.18653/v1/2025.acl-long.1222",
    pages = "25132--25154",
}
```

## License

MIT — see [LICENSE](LICENSE). The dataset itself is released for research and
educational use only; see the [dataset card](https://huggingface.co/datasets/blitt/SPoRC)
for its terms.

## Support

- **Documentation:** [sporc.readthedocs.io](https://sporc.readthedocs.io)
- **Tutorials:** [`examples/notebooks/`](examples/notebooks/)
- **Questions, bugs, feature requests:** [open an issue](https://github.com/davidjurgens/sporc/issues)
