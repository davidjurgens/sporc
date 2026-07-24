# SPoRC

**Structured Podcast Open Research Corpus** — a Python package for working with
the [SPoRC dataset](https://huggingface.co/datasets/blitt/SPoRC): 228,099
podcasts and 1,124,058 episodes of transcripts, speaker turns, and acoustic
features.

```python
from sporc import SPORCDataset

sporc = SPORCDataset(subset=["The NPR Politics Podcast"])
podcast = sporc.search_podcast("The NPR Politics Podcast")

for episode in podcast.episodes:
    print(f"{episode.title} — {episode.duration_minutes:.0f} min")
```

## Start here

<div class="grid cards" markdown>

- :material-download: **[Installation](installation.md)** — accept the dataset
  terms, authenticate with `hf auth login`, and `pip install sporc`.
- :material-rocket-launch: **[Quick start](quickstart.md)** — load a real
  podcast and read its episodes and turns in a dozen lines.
- :material-database: **[Working with the data](data-access.md)** — the corpus is
  ~57 GB and partitioned by podcast; learn to fetch only the slice you need.
- :material-school: **[Tutorials](tutorials.md)** — eight notebooks built around
  real research questions, from corpus cartography to sociophonetics.

</div>

## What's in the corpus

- **A two-month snapshot.** Every episode was published between **1 May and
  30 June 2020**. It is not a longitudinal archive — but the window straddles a
  sharp, dateable event, which makes before/after designs unusually clean.
- **Transcripts everywhere, turns for ~65%.** 731,101 of 1,124,058 episodes were
  diarized into speaker turns; the rest have a transcript but no turns. Always
  gate on `episode.has_turn_data`.
- **Thin, per-turn acoustics.** Six averaged numbers per turn (four MFCCs, one
  F0, one F1) — no word-level timing. The [phonetics](guides/phonetics.md) module
  re-derives alignment from source audio when you need it.

The dataset itself (schema, columns, layout, terms of use) is documented on the
[dataset card](https://huggingface.co/datasets/blitt/SPoRC). These pages document
the Python package.

## How the docs are organised

- **Getting started** — install, a working quick start, and the data-transfer
  model you need to understand before pulling anything large.
- **[Tutorials](tutorials.md)** — end-to-end notebooks.
- **Guides** — task-focused walkthroughs: searching, data loading, categories,
  conversation analysis, sliding windows, phonetics, and an FAQ.
- **[API reference](reference/index.md)** — generated from the docstrings, so it
  always matches the installed version.

## Citation

If you use SPoRC in your research, please cite:

```bibtex
@inproceedings{litterer-etal-2025-mapping,
    title = "Mapping the Podcast Ecosystem with the Structured Podcast Research Corpus",
    author = "Litterer, Benjamin Roger and Jurgens, David and Card, Dallas",
    booktitle = "Proceedings of the 63rd Annual Meeting of the Association for Computational Linguistics (Volume 1: Long Papers)",
    year = "2025",
    address = "Vienna, Austria",
    publisher = "Association for Computational Linguistics",
    url = "https://aclanthology.org/2025.acl-long.1222/",
    doi = "10.18653/v1/2025.acl-long.1222",
    pages = "25132--25154",
}
```
