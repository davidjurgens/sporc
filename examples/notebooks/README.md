# SPoRC tutorial notebooks

Eight short tutorials built around real research questions. Each is standalone,
but they are ordered: **01 establishes caveats the rest depend on**, so start
there.

| # | Notebook | Question | Needs |
|---|---|---|---|
| 01 | `01_corpus_cartography` | What is actually in SPoRC? | — |
| 02 | `02_ner_comention_networks` | Who gets talked about together? | spacy + `en_core_web_sm` |
| 03 | `03_host_guest_networks` | Which shows share guests? | networkx |
| 04 | `04_repeat_guest_language` | Do repeat guests reuse material? | scikit-learn |
| 05 | `05_stance_over_time` | How did talk change around 25 May 2020? | nltk (vader) |
| 06 | `06_topic_modeling_mallet` | What is podcasting about? | MALLET + Java |
| 07 | `07_sociophonetics_caught_cot` | Does this speaker merge *caught* and *cot*? | `sporc[phonetics]` + ffmpeg |
| 08 | `08_conversational_dynamics` | Who talks longest, and where do people overlap? | — |

If you only read one, read 07. It starts from a question the corpus **cannot**
answer and shows what to do about that.

## Setup

### 1. Install this checkout — not the PyPI release

PyPI's newest `sporc` is **0.2.0**, which has a different (pre-1.0) API. `pip
install sporc` gets you the wrong package, and because it installs a real
`sporc/` directory it will **shadow** a source checkout for any process not
started from the repo root. Every notebook checks its own import and fails loudly
if this happened to you.

```bash
cd /path/to/sporc
pip install -e .
python -c "import sporc; print(sporc.__version__, sporc.__file__)"   # want 1.0.0
```

### If spaCy fails to load

`spacy.load(...)` dying with `cannot import name 'BatchEncoding' from
'transformers.tokenization_utils'` means `spacy-transformers` is installed
alongside `transformers>=5`. Every `spacy-transformers` release (including the
newest, 1.4.0) pins `transformers<4.53.3`, and it registers a `transformer`
factory that `spacy.load()` imports for **any** model — so one incompatible
plugin breaks `en_core_web_sm`, which doesn't use transformers at all.

Nothing in these notebooks needs `spacy-transformers`:

```bash
pip uninstall spacy-transformers        # simplest
# or, if you need it:  pip install 'transformers<4.53'
```

Notebook 02 detects this case and prints the fix rather than the raw ImportError.

Notebook 07 additionally needs the optional extra and an ffmpeg binary:

```bash
pip install -e ".[phonetics]"
conda install -c conda-forge ffmpeg     # or: apt install ffmpeg
python -c "import nltk; nltk.download('cmudict'); nltk.download('vader_lexicon')"
python -m spacy download en_core_web_sm
```

### 2. Build the tutorial subset

The notebooks read `subsets/tutorial`, a small self-contained layout — not the
57 GB corpus. Building it costs a one-time **~195 MB** of catalogs plus a few MB
per podcast:

```bash
python scripts/build_tutorial_subset.py
```

That script downloads the catalogs, picks the podcasts these tutorials need,
fetches only those partitions, and runs `make_subset.py` over the result. The
chosen ids are pinned in `subsets/tutorial_ids.txt`, so the subset is
reproducible.

**Why the subset is hand-picked rather than random.** Notebook 04 needs guests
who appear on two *different* shows. In a random sample of podcasts, essentially
none do — the corpus has 228k podcasts and a guest tour touches a handful. The
selection therefore unions a random diarized sample with podcasts chosen to
contain validated repeat guests. `scripts/make_subset.py --podcast-ids` exists
for exactly this.

You need a HuggingFace account with the [dataset terms](https://huggingface.co/datasets/blitt/SPoRC)
accepted, and `huggingface-cli login`.

### 3. Run

The notebooks ship executed, so you can read them here or on GitHub without
running anything. To run them yourself:

```bash
cd examples/notebooks
jupyter lab
```

## Three facts that govern every notebook

1. **SPoRC is two months.** Every episode is from 1 May – 30 June 2020. No
   long-run trends; but a sharp dateable event sits mid-window, which notebook 05
   uses.
2. **Only 33% of episodes have speaker turns.** `len(episode.turns) == 0` is
   ambiguous — gate on `episode.has_turn_data` instead.
3. **~90% of turns have no inferred role.** Anything conditioned on host/guest
   role is a lower bound, not a partition.

Notebook 01 demonstrates all three.

## Editing the notebooks

Notebook JSON is unreviewable in a diff, so each tutorial's real source is a
plain Python file under `src/`:

```
src/nb01_cartography.py   ->   01_corpus_cartography.ipynb
```

Edit the source, then rebuild, and commit both:

```bash
python _build.py            # all
python _build.py 07         # just one
python _build.py --check    # are the .ipynb current with src/?
python _build.py --execute  # rebuild and re-run, refreshing outputs
```

The committed `.ipynb` are **executed**: they carry their outputs and figures, so
they read on GitHub without building the subset first. A plain build keeps the
outputs of any cell whose source didn't change, so fixing a typo won't throw away
a figure that took twelve minutes to produce; `--execute` is what re-runs them,
and it needs `subsets/tutorial` on disk.

Rebuilding is a no-op in git when nothing changed, so a diff on a `.ipynb` means
a real edit. `--check` compares sources only, and catches the case that motivated
it: a source gaining a section that never reached the notebook, which is easy to
miss and shipped once.

`_viz.py` holds the shared house style. Its palette is a validated set — the hue
*order* is what keeps it colour-vision-safe, so don't re-order it or extend past
eight. Fold a long tail into "Other" with `fold_other()`.

## Runtime

Most notebooks finish in a couple of minutes. Three cost real time, and the
reasons are worth knowing before you widen them:

- **02** runs spaCy over turns. The full subset is ~13.9M words, so it samples
  300 episodes (`N_EPISODES`).
- **06** fits MALLET over sliding windows; the full subset is ~4.1M tokens, so it
  samples 600 episodes.
- **07** fetches and aligns real audio. Cost tracks the *turn's* length, not the
  word's, at roughly 0.45x realtime on CPU — which is why it caps turn duration
  and works two podcasts at a time. Budget ~12 minutes.
