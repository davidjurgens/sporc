# Tutorials

Eight standalone Jupyter notebooks, each framed around a real research question.
They live in [`examples/notebooks/`](https://github.com/davidjurgens/sporc/tree/main/examples/notebooks)
and ship **executed** — you can read them (with their figures) on GitHub without
running anything.

They are ordered: **notebook 01 establishes the corpus caveats the rest depend
on**, so start there.

| # | Notebook | Question | Extra deps |
|---|---|---|---|
| 01 | Corpus cartography | What is actually in SPoRC? | — |
| 02 | NER co-mention networks | Who gets talked about together? | spaCy + `en_core_web_sm` |
| 03 | Host/guest networks | Which shows share guests? | networkx |
| 04 | Repeat-guest language | Do repeat guests reuse material? | scikit-learn |
| 05 | Stance over time | How did talk change around 25 May 2020? | nltk (VADER) |
| 06 | Topic modeling (MALLET) | What is podcasting about? | MALLET + Java |
| 07 | Sociophonetics: caught/cot | Does this speaker merge *caught* and *cot*? | `sporc[phonetics]` + ffmpeg |
| 08 | Conversational dynamics | Who talks longest, and where do people overlap? | — |

!!! tip "If you only read one"
    Read **07**. It starts from a question the corpus *cannot* answer directly —
    there are no word timings in SPoRC — and shows how to re-derive them from
    source audio.

## Running the notebooks

The notebooks read `subsets/tutorial`, a small self-contained layout — **not** the
57 GB corpus. Build it once (a one-time ~195 MB of catalogs plus a few MB per
podcast):

```bash
python scripts/build_tutorial_subset.py
```

Then:

```bash
cd examples/notebooks
jupyter lab
```

Full setup — including the spaCy/`spacy-transformers` conflict fix, the MALLET
and phonetics prerequisites, and the notebook build workflow — is in the
[notebooks README](https://github.com/davidjurgens/sporc/blob/main/examples/notebooks/README.md).

## Three facts that govern every notebook

1. **SPoRC is two months.** Every episode is from 1 May – 30 June 2020. No
   long-run trends; but a sharp dateable event sits mid-window, which notebook 05
   uses.
2. **~65% of episodes have speaker turns.** `len(episode.turns) == 0` is
   ambiguous — gate on `episode.has_turn_data` instead.
3. **~90% of turns have no inferred role.** Anything conditioned on host/guest
   role is a lower bound, not a partition.

## Shorter example scripts

Single-topic scripts in [`examples/`](https://github.com/davidjurgens/sporc/tree/main/examples):
`basic_usage.py`, `category_examples.py`, `sliding_window_examples.py`,
`time_range_examples.py`, and `advanced_analysis.py`.
