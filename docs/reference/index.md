# API reference

These pages are generated directly from the docstrings in the `sporc` package,
so they always match the installed version.

There are two ways into the data. The object model is the right shape for
questions about one episode; the DataFrame API is the right shape for anything
above that, and is two orders of magnitude faster there.

| Class | What it is |
|---|---|
| [`SPORCDataset`](sporcdataset.md) | Entry point — search, retrieval, and the DataFrame accessors. |
| [`Podcast`](podcast.md) | A show: its episodes, hosts, categories, and aggregates. |
| [`Episode`](episode.md) | A single episode: transcript, speakers, turns, metrics. |
| [`Turn`](turn.md) | One speaker turn: text, timing, role, acoustics. |
| [DataFrames & schema](frames.md) | `turns_frame`, `window_frame`, `catalog`, and what every column means. |

Optional and lower-level surfaces:

- [Phonetics](phonetics.md) — word alignment and formant measurement from source
  audio (`sporc.phonetics`, install `sporc[phonetics]`).
- [Exceptions & constants](misc.md) — the error hierarchy, the speaker
  sentinels, and the Apple Podcast category helpers.

Everything importable from the top level is re-exported in `sporc/__init__.py`.
