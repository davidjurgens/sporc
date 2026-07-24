# API reference

These pages are generated directly from the docstrings in the `sporc` package,
so they always match the installed version.

The public API is small and centres on four classes:

| Class | What it is |
|---|---|
| [`SPORCDataset`](sporcdataset.md) | Entry point — search and retrieval over the corpus. |
| [`Podcast`](podcast.md) | A show: its episodes, hosts, categories, and aggregates. |
| [`Episode`](episode.md) | A single episode: transcript, speakers, turns, metrics. |
| [`Turn`](turn.md) | One speaker turn: text, timing, role, acoustics. |

Optional and lower-level surfaces:

- [Phonetics](phonetics.md) — word alignment and formant measurement from source
  audio (`sporc.phonetics`, install `sporc[phonetics]`).
- [Exceptions & constants](misc.md) — the error hierarchy and the Apple Podcast
  category helpers.

Everything importable from the top level is re-exported in `sporc/__init__.py`.
