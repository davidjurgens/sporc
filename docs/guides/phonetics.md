# Phonetics

SPoRC ships no word timings. Its acoustic data is six per-turn means —
`mfcc1..4_sma3_mean`, `f0_semitone_from_27_5hz_sma3nz_mean`, and
`f1_frequency_sma3nz_mean` — each averaged over a turn that spans many words, and
there is no F2 at all. Vowel-level questions (the caught/cot merger, vowel-space
plots, formant trajectories) cannot be answered from those fields.

They *can* be answered from the audio the corpus points at. Every turn carries an
`mp3_url` plus its own `start_time` and `end_time`, which is enough to fetch that
one turn and re-derive alignment properly:

```
turn text + turn audio  ->  forced alignment  ->  word and phone timings
phone timings + audio    ->  Praat             ->  F1/F2/F3 at the vowel
```

The `sporc.phonetics` module implements that chain.

## Installation

The dependencies (torch, torchaudio, transformers, parselmouth) are heavy
relative to the rest of the package, so everything is imported lazily and the
extra is opt-in. You also need an `ffmpeg` binary on your `PATH`.

```bash
pip install "sporc[phonetics]"     # torch, torchaudio, transformers, parselmouth
# plus ffmpeg, e.g.:
conda install -c conda-forge ffmpeg   # or: apt install ffmpeg
```

The first phone-level run also needs CMUdict, which the module will tell you to
download if it is missing:

```bash
python -c "import nltk; nltk.download('cmudict')"
```

## The high-level path

`find_word_tokens()` runs the whole chain — find turns containing a word, fetch
each turn's audio, align, and measure the stressed vowel — and
`lobanov_normalize()` turns the raw formants into per-speaker z-scores.

```python
from sporc import SPORCDataset
from sporc.phonetics import find_word_tokens, lobanov_normalize

sporc = SPORCDataset(parquet_dir="subsets/tutorial")

tokens = find_word_tokens(sporc, "caught", limit=50)   # list[FormantMeasurement]
df = lobanov_normalize(tokens)                          # DataFrame with f1_z/f2_z
```

Each `FormantMeasurement` records the word, the vowel CMUdict expected, the
Wells lexical set (`"THOUGHT"`/`"LOT"`/`None`, fixed by the word's history rather
than by its measured or dictionary vowel), `f1`/`f2`/`f3`, the vowel duration,
and the inferred speaker, episode, and podcast.

!!! tip "The lexical set is the point"
    Whether a word belongs to the caught (`THOUGHT`) or cot (`LOT`) class is a
    property of the word's history, not of how anyone pronounces it. `caught`
    is `K AA1 T` in CMUdict — the merger is already in the dictionary — so
    classifying tokens by their dictionary vowel would answer the question
    before measuring anything. `lexical_set(word)` returns the fixed
    etymological class; the formants are what tell you whether a given speaker
    keeps the two apart.

## The lower-level steps

If you want the individual stages — for example to plot intermediate timings, or
to work with turns you already have in hand — the same chain is exposed as
separate functions.

```python
from sporc.phonetics import (
    fetch_turn_audio, align_turn, stressed_vowel_index, measure_formants,
)

# 1. Range-fetch ONLY this turn's audio (not the whole episode).
audio, sr = fetch_turn_audio(turn)            # returns an (audio, sample_rate) pair

# 2. Word-align the whole turn's text against its audio.
words = align_turn(turn, audio=audio, sample_rate=sr, level="word")

# 3. Slice the target word out and phone-align just that clip.
#    level="phone" makes the phones explain ALL the audio you hand it, so passing
#    a whole turn returns a confident, meaningless alignment — slice first.
hit = next(w for w in words if w.word.lower() == "caught")
clip = audio[int(hit.start * sr):int(hit.end * sr)]
phones = align_turn(turn, audio=clip, sample_rate=sr, level="phone", word="caught")

# 4. Find the stressed vowel and measure its formants.
#    Ask which phone is the stressed vowel rather than counting positions: it is
#    the 2nd phone in "caught" but the 3rd in "across".
v = phones[stressed_vowel_index([p.arpabet for p in phones])]
measure_formants(clip, sr, v.start, v.end)    # {"f1", "f2", "f3", "time"}
```

`measure_word_in_turn(turn, word)` packages steps 1–4 for a single turn and
returns one `FormantMeasurement` (or `None` if the word is not found), and it is
what `find_word_tokens()` calls per hit.

!!! note "`fetch_turn_audio` returns a pair"
    `align_turn` takes the array and the rate as separate arguments, so the
    idiom is `audio, sr = fetch_turn_audio(turn)` and then
    `align_turn(turn, audio=audio, sample_rate=sr)`. Passing the pair through as
    a single argument fails inside numpy, not at the call site.

Supporting helpers include `pronounce(word)` (CMUdict ARPAbet lookup, with a
`variant` argument for words that have more than one listed pronunciation),
`lexical_set(word)`, and `resolve_audio_url(url)` (follows the tracking
redirects that ffmpeg does not always handle).

## Caveats that matter for anything you publish

!!! warning "Only the turn is downloaded — but alignment, not the download, is the cost"
    `fetch_turn_audio` makes ffmpeg seek into the remote mp3 with an HTTP range
    request, so a turn costs ~1–2 s and a few hundred KB rather than a 100 MB
    episode download. Locating a word, however, means word-aligning the turn's
    *whole* text against its whole audio, and the CTC forward pass runs at
    roughly **0.45x realtime on CPU** — cost scales with the turn, not with the
    target word. Turns run long (median ~64 s for a common word, mean ~160 s,
    longest in the corpus 3,240 s ≈ 24 minutes to align). `find_word_tokens`
    therefore skips turns longer than `max_turn_duration=30` seconds by default
    and logs what it dropped; a `limit=50` call is minutes, not seconds. Raising
    or disabling the cap biases the sample toward conversational shows, because
    a solo monologue is often one enormous turn.

!!! warning "The audio is external and mutable"
    `mp3_url` points at the publisher's CDN, not at HuggingFace. Links rot, and
    some hosts (podtrac, libsyn) return 404 unless their redirects are resolved
    first — which `fetch_turn_audio` does via `resolve_audio_url`.
    `find_word_tokens(..., skip_errors=True)` (the default) simply skips turns
    whose audio is unreachable.

!!! warning "A speaker is an inferred name"
    `lobanov_normalize` groups tokens by inferred speaker name — matched across
    episodes and across podcasts, so one person on three shows is one speaker
    with three times the tokens — and it **drops** tokens labelled
    `NO_INFERRED_SPEAKER` (the corpus's "attribution found nobody" marker, and
    its single most common speaker value) and raw `SPEAKER_00` diarization
    labels (which are per-episode and identify no one across episodes) rather
    than pooling them. Pooling the placeholder would average dozens of
    unrelated vocal tracts into one "voice." Two distinct people who share a
    name still pool — SPoRC has no canonical speaker id today. Because tokens
    accumulate per name, probe more *words* rather than more podcasts to reach
    `min_tokens`.

!!! danger "`estimate_word_audio()` is not this"
    `SPORCDataset.estimate_word_audio()` interpolates word timing from character
    offsets. It is deprecated for phonetic use — it does not align anything, so
    its boundaries are not accurate enough to measure a vowel from. Use the
    `sporc.phonetics` chain instead.

Two more that are worth knowing before drawing conclusions: CTC alignment is
*peaky* (each phone is emitted at essentially one 20 ms frame, so vowel intervals
are derived from the gap between neighbouring spikes and are good to a few tens
of ms, not hand-corrected Praat boundaries), and raw formants vary more between
speakers than between vowel classes (comparing raw F1/F2 across speakers measures
vocal-tract length, which is exactly why `lobanov_normalize` exists).

## Worked example and reference

- **Notebook:** `examples/notebooks/07_sociophonetics_caught_cot.ipynb` works the
  whole chain through on the caught/cot merger, end to end.
- **API:** see the [Phonetics API reference](../reference/phonetics.md) for every
  public function and dataclass, and the [Tutorials](../tutorials.md) index for
  the other notebooks.
