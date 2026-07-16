"""
Word-level alignment and formant measurement from source audio.

SPoRC carries no word timings. Its acoustic data is six per-turn means
(``mfcc1..4_sma3_mean``, ``f0_semitone_from_27_5hz_sma3nz_mean``,
``f1_frequency_sma3nz_mean``), averaged over a turn that spans many words, and
there is no F2 at all. Vowel-level questions -- the caught/cot merger, vowel
space, formant trajectories -- are therefore unanswerable from the corpus as
distributed.

They are answerable from the audio the corpus points at. Every turn carries an
``mp3_url``, a ``start_time`` and an ``end_time``, which is enough to fetch that
turn's audio and re-derive alignment properly:

    turn text + turn audio  -> forced alignment -> word and phone timings
    phone timings + audio   -> Praat            -> F1/F2/F3 at the vowel

This module implements that chain. It is optional and its dependencies are heavy
relative to the rest of the package, so everything is imported lazily and the
extra is opt-in::

    pip install sporc[phonetics]      # plus an ffmpeg binary on PATH

Typical use::

    from sporc import SPORCDataset
    from sporc.phonetics import find_word_tokens, lobanov_normalize

    sporc = SPORCDataset(parquet_dir="subsets/tutorial")
    tokens = find_word_tokens(sporc, "caught", limit=50)
    df = lobanov_normalize(tokens)

Notes and limits, which matter for anything you publish:

* **Only the turn's audio is fetched.** ``ffmpeg`` seeks into the remote mp3 with
  an HTTP range request, so a 10-second turn costs ~1-2 s and a few hundred KB
  rather than a 100 MB episode download.
* **Audio is external and mutable.** ``mp3_url`` points at the publisher's CDN,
  not at HuggingFace. Links rot, and some hosts (podtrac, libsyn) 404 unless
  redirects are resolved first, which :func:`fetch_turn_audio` does.
* **CTC alignment is peaky.** The aligner emits each phone at essentially one
  20 ms frame, so a token span is a *spike*, not a segment boundary. Vowel
  intervals here are derived from the gap between neighbouring spikes; see
  :func:`_spans_to_intervals`. Treat the boundaries as good to a few tens of ms,
  not as hand-corrected Praat boundaries.
* **Formants vary more between speakers than between vowel classes.** Comparing
  raw F1/F2 across speakers measures vocal-tract length, not phonology. Use
  :func:`lobanov_normalize` and enough tokens per speaker before concluding
  anything.
"""

import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Sequence, Union

from .exceptions import SPORCError

logger = logging.getLogger(__name__)

#: CTC model used for word-level (grapheme) alignment.
WORD_ALIGN_MODEL = "WAV2VEC2_ASR_BASE_960H"

#: CTC model used for phone-level alignment. Outputs eSpeak-style IPA.
PHONE_ALIGN_MODEL = "facebook/wav2vec2-lv-60-espeak-cv-ft"

#: Sample rate the aligners expect.
SAMPLE_RATE = 16000

_UA = "Mozilla/5.0 (X11; Linux x86_64)"

# ARPAbet (CMUdict) -> the IPA symbols PHONE_ALIGN_MODEL's vocabulary uses.
# CMUdict is the g2p source rather than espeak/phonemizer: the phone-model's own
# tokenizer hard-requires the phonemizer backend, but we do not need it -- we
# know the words we are looking for, and CMUdict gives their phones directly,
# including which vowel carries stress.
ARPABET_TO_IPA = {
    "AA": "ɑ", "AE": "æ", "AH": "ʌ", "AO": "ɔ", "AW": "aʊ", "AY": "aɪ",
    "B": "b", "CH": "tʃ", "D": "d", "DH": "ð", "EH": "ɛ", "ER": "ɚ",
    "EY": "eɪ", "F": "f", "G": "ɡ", "HH": "h", "IH": "ɪ", "IY": "i",
    "JH": "dʒ", "K": "k", "L": "l", "M": "m", "N": "n", "NG": "ŋ",
    "OW": "oʊ", "OY": "ɔɪ", "P": "p", "R": "ɹ", "S": "s", "SH": "ʃ",
    "T": "t", "TH": "θ", "UH": "ʊ", "UW": "u", "V": "v", "W": "w",
    "Y": "j", "Z": "z", "ZH": "ʒ",
}

#: ARPAbet vowels. Stress digits are stripped before lookup.
ARPABET_VOWELS = frozenset({
    "AA", "AE", "AH", "AO", "AW", "AY", "EH", "ER", "EY",
    "IH", "IY", "OW", "OY", "UH", "UW",
})

# Wells' lexical sets for the low back vowels -- the caught/cot merger.
#
# Membership is a property of the WORD's history, not of how any speaker or
# dictionary pronounces it. This distinction is the whole analysis: CMUdict
# gives "caught" as K AA1 T (the LOT vowel) as its first variant, because the
# merger is already in the dictionary. Classifying tokens by their CMUdict vowel
# would therefore label caught as LOT and answer the question before measuring
# anything. The sets below are fixed by etymology; whether a speaker keeps them
# apart is what the formants are for.
#
# Deliberately conservative: words whose class varies regionally in ways
# unrelated to this merger (on, gone, water, wash) are left out.
THOUGHT_WORDS = frozenset({
    "caught", "taught", "bought", "thought", "brought", "fought", "sought",
    "talk", "talks", "talked", "talking", "walk", "walks", "walked", "walking",
    "chalk", "law", "laws", "lawyer", "saw", "jaw", "raw", "draw", "drawn",
    "dawn", "lawn", "hawk", "salt", "fault", "all", "ball", "call", "called",
    "calling", "fall", "hall", "small", "tall", "wall", "always", "also",
    "although", "cause", "because", "pause", "applause", "author", "autumn",
    "august", "awesome", "awful", "daughter", "naughty", "long", "song",
    "wrong", "strong", "along", "belong", "cough", "off", "office", "coffee",
    "often", "soft", "lost", "cost", "cross", "across", "boss", "loss", "toss",
})

LOT_WORDS = frozenset({
    "cot", "hot", "lot", "lots", "dot", "got", "not", "pot", "rot", "shot",
    "spot", "stock", "sock", "rock", "block", "clock", "knock", "lock", "mock",
    "job", "jobs", "mob", "rob", "sob", "top", "stop", "stopped", "drop",
    "chop", "hop", "shop", "cop", "cops", "box", "fox", "body", "copy",
    "hobby", "lobby", "problem", "problems", "product", "project", "common",
    "comic", "doctor", "dollar", "dollars", "follow", "hollow", "model",
    "modern", "november", "october", "popular", "possible", "positive",
    "promise", "proper", "quality", "robot", "rocket", "solid", "volume",
    "politics", "political", "economy", "opposite", "obvious", "college",
})

#: Word -> lexical set, for the words this module knows about.
LEXICAL_SETS: Dict[str, str] = {
    **{w: "THOUGHT" for w in THOUGHT_WORDS},
    **{w: "LOT" for w in LOT_WORDS},
}


def lexical_set(word: str) -> Optional[str]:
    """
    The Wells lexical set a word belongs to, or None if unknown.

    ``"THOUGHT"`` (the caught class) or ``"LOT"`` (the cot class). Determined by
    the word, never by its measured or dictionary vowel -- see
    :data:`THOUGHT_WORDS`.
    """
    return LEXICAL_SETS.get(word.lower().strip())

_MODELS: Dict[str, Any] = {}


class PhoneticsError(SPORCError):
    """Raised when audio cannot be fetched, decoded, or aligned."""


@dataclass
class WordTiming:
    """A word located in a turn's audio, in seconds from the turn's start."""

    word: str
    start: float
    end: float
    score: float

    @property
    def duration(self) -> float:
        return self.end - self.start


@dataclass
class PhoneTiming:
    """
    A phone located in a turn's audio, in seconds from the turn's start.

    ``start``/``end`` are derived from neighbouring CTC spikes rather than read
    off one span, because CTC emits a phone at a single frame. They are estimates
    good to a few tens of milliseconds.
    """

    phone: str
    arpabet: str
    start: float
    end: float
    score: float
    word: Optional[str] = None

    @property
    def duration(self) -> float:
        return self.end - self.start

    @property
    def is_vowel(self) -> bool:
        return self.arpabet.rstrip("012") in ARPABET_VOWELS

    @property
    def stress(self) -> Optional[int]:
        m = re.search(r"([012])$", self.arpabet)
        return int(m.group(1)) if m else None


@dataclass
class FormantMeasurement:
    """F1/F2/F3 at a vowel, with the context needed to interpret them."""

    word: str
    #: The vowel CMUdict expects, ARPAbet with stress stripped (e.g. "AO").
    #: This is the aligner's target, not a finding: what the speaker actually
    #: produced is what f1/f2 measure.
    vowel: str
    #: Wells lexical set of ``word`` ("THOUGHT"/"LOT"/None), fixed by the word's
    #: history rather than by ``vowel``. See :func:`lexical_set`.
    lexical_set: Optional[str]
    f1: float
    f2: float
    f3: Optional[float]
    time: float                # seconds into the turn
    vowel_duration: float
    speaker: Optional[str] = None
    episode_id: Optional[str] = None
    podcast_id: Optional[str] = None
    turn_text: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ----------------------------------------------------------------------
# Turn field access
# ----------------------------------------------------------------------

def _turn_fields(turn: Any) -> Dict[str, Any]:
    """
    Pull the fields we need off a Turn object or a search-result dict.

    ``search_turns()`` returns dicts keyed ``turn_text``; ``Episode.turns``
    yields Turn objects with ``.text``. Both are accepted so callers can use
    whichever they already have.
    """
    if isinstance(turn, dict):
        text = turn.get("turn_text", turn.get("text", ""))
        speaker = turn.get("speaker_name", turn.get("inferred_speaker_name"))
        return {
            "text": str(text or ""),
            "start_time": float(turn.get("start_time") or 0.0),
            "end_time": float(turn.get("end_time") or 0.0),
            "mp3_url": turn.get("mp3_url"),
            "speaker": speaker,
            "episode_id": turn.get("episode_id"),
            "podcast_id": turn.get("podcast_id"),
        }
    return {
        "text": str(getattr(turn, "text", "") or ""),
        "start_time": float(getattr(turn, "start_time", 0.0) or 0.0),
        "end_time": float(getattr(turn, "end_time", 0.0) or 0.0),
        "mp3_url": getattr(turn, "mp3_url", None),
        "speaker": (getattr(turn, "inferred_speaker_name", None)
                    or (getattr(turn, "speaker", None) or [None])[0]),
        "episode_id": None,
        "podcast_id": None,
    }


# ----------------------------------------------------------------------
# Audio
# ----------------------------------------------------------------------

def _require_ffmpeg() -> str:
    exe = shutil.which("ffmpeg")
    if not exe:
        raise PhoneticsError(
            "ffmpeg is required to fetch turn audio but was not found on PATH. "
            "Install it (conda install -c conda-forge ffmpeg, or apt install "
            "ffmpeg)."
        )
    return exe


def resolve_audio_url(url: str, timeout: float = 20.0) -> str:
    """
    Follow redirects to the URL ffmpeg should actually open.

    Podcast mp3 links are usually tracking redirects (podtrac, chartable,
    pdst.fm) that resolve to a CDN. ffmpeg does not always follow them and
    reports a bare 404, so the hop is done here first.
    """
    import requests

    try:
        r = requests.get(url, headers={"User-Agent": _UA}, stream=True,
                         timeout=timeout, allow_redirects=True)
        r.close()
    except Exception as e:
        raise PhoneticsError(f"Could not resolve {url}: {e}") from e
    if r.status_code >= 400:
        raise PhoneticsError(f"{url} returned HTTP {r.status_code}")
    return r.url


def fetch_turn_audio(turn: Any, pad: float = 0.3,
                     sample_rate: int = SAMPLE_RATE,
                     timeout: float = 120.0):
    """
    Fetch the audio for a single turn as a mono float32 numpy array.

    Only the turn's own span is downloaded: ffmpeg seeks into the remote file
    with an HTTP range request, so cost scales with turn length, not episode
    length.

    Args:
        turn: A :class:`~sporc.turn.Turn`, or a dict with ``mp3_url``,
            ``start_time`` and ``end_time``.
        pad: Seconds of context to include on each side. A little padding keeps
            the aligner from clipping the first and last phones.
        sample_rate: Target sample rate. The aligners expect 16 kHz.
        timeout: Seconds to allow ffmpeg.

    Returns:
        ``(audio, sample_rate)`` where audio is a 1-D float32 numpy array whose
        t=0 corresponds to ``turn.start_time - pad``.

    Raises:
        PhoneticsError: if the url is missing, unreachable, or undecodable.
    """
    import soundfile as sf

    exe = _require_ffmpeg()
    f = _turn_fields(turn)
    url = f["mp3_url"]
    if not url:
        raise PhoneticsError(
            "This turn has no mp3_url, so its audio cannot be fetched. Turns "
            "read via search_turns() omit it; pass a Turn object from "
            "Episode.turns, or supply mp3_url in the dict."
        )
    start = max(0.0, f["start_time"] - pad)
    dur = (f["end_time"] - f["start_time"]) + 2 * pad
    if dur <= 0:
        raise PhoneticsError(f"Turn has non-positive duration ({dur:.3f}s)")

    final = resolve_audio_url(url)
    with tempfile.TemporaryDirectory() as td:
        out = os.path.join(td, "seg.wav")
        cmd = [exe, "-nostdin", "-loglevel", "error", "-user_agent", _UA,
               "-ss", f"{start:.3f}", "-i", final, "-t", f"{dur:.3f}",
               "-ac", "1", "-ar", str(sample_rate), "-y", out]
        try:
            p = subprocess.run(cmd, capture_output=True, text=True,
                               timeout=timeout)
        except subprocess.TimeoutExpired as e:
            raise PhoneticsError(f"ffmpeg timed out fetching {url}") from e
        if p.returncode != 0 or not os.path.exists(out):
            raise PhoneticsError(
                f"ffmpeg could not fetch {url}: {p.stderr.strip()[:200]}"
            )
        # soundfile, not torchaudio.load: torchaudio >=2.9 delegates decoding to
        # torchcodec, which is a separate install.
        audio, sr = sf.read(out, dtype="float32")
    return audio, sr


# ----------------------------------------------------------------------
# Grapheme-to-phoneme
# ----------------------------------------------------------------------

def _cmudict():
    if "cmudict" not in _MODELS:
        try:
            from nltk.corpus import cmudict
            _MODELS["cmudict"] = cmudict.dict()
        except LookupError as e:
            raise PhoneticsError(
                "CMUdict is not downloaded. Run: "
                "python -c \"import nltk; nltk.download('cmudict')\""
            ) from e
    return _MODELS["cmudict"]


def pronounce(word: str, variant: int = 0) -> Optional[List[str]]:
    """
    Look a word's ARPAbet pronunciation up in CMUdict.

    Returns the phones with stress digits intact (``["K", "AO1", "T"]``), or
    None if the word is not in the dictionary.

    Some words have several pronunciations; ``caught`` is listed as both
    ``K AO1 T`` and ``K AA1 T``, which is the merger written into the
    dictionary. ``variant`` selects among them.
    """
    prons = _cmudict().get(word.lower())
    if not prons:
        return None
    return list(prons[min(variant, len(prons) - 1)])


def stressed_vowel_index(phones: Sequence[str]) -> Optional[int]:
    """
    Index of the primary-stressed vowel in an ARPAbet phone sequence.

    Falls back to secondary stress, then to the first vowel. This is what makes
    multi-syllable targets work: in ``across`` (``AH0 K R AO1 S``) the vowel of
    interest is the second one, not the first.
    """
    for want in ("1", "2"):
        for i, ph in enumerate(phones):
            if ph.rstrip("012") in ARPABET_VOWELS and ph.endswith(want):
                return i
    for i, ph in enumerate(phones):
        if ph.rstrip("012") in ARPABET_VOWELS:
            return i
    return None


# ----------------------------------------------------------------------
# Alignment
# ----------------------------------------------------------------------

def _word_model():
    if "word" not in _MODELS:
        import torchaudio
        bundle = getattr(torchaudio.pipelines, WORD_ALIGN_MODEL)
        _MODELS["word"] = (bundle.get_model().eval(), bundle.get_labels())
    return _MODELS["word"]


def _phone_model():
    if "phone" not in _MODELS:
        from huggingface_hub import hf_hub_download
        from transformers import AutoModelForCTC
        vocab = json.load(open(hf_hub_download(PHONE_ALIGN_MODEL, "vocab.json")))
        model = AutoModelForCTC.from_pretrained(PHONE_ALIGN_MODEL).eval()
        _MODELS["phone"] = (model, vocab)
    return _MODELS["phone"]


def _spans_to_intervals(spans, ratio: float, n_samples: int,
                        sample_rate: int) -> List[tuple]:
    """
    Turn peaky CTC spans into contiguous intervals.

    CTC is peaky: each token is emitted at essentially one frame, so
    ``merge_tokens`` returns ~20 ms spans regardless of how long the sound
    actually lasts. Taking those widths literally would report every vowel as
    ~20 ms. Each token is instead treated as running from its own spike to the
    next token's spike, which recovers durations in the right range.

    Returns a list of ``(start_seconds, end_seconds)``, one per span.
    """
    out = []
    total = n_samples / sample_rate
    for i, sp in enumerate(spans):
        start = sp.start * ratio
        end = spans[i + 1].start * ratio if i + 1 < len(spans) else total
        if end <= start:                       # degenerate; fall back to the span
            end = max(sp.end * ratio, start + 1.0 / sample_rate)
        out.append((start, min(end, total)))
    return out


def align_turn(turn: Any, audio=None, sample_rate: int = SAMPLE_RATE,
               level: str = "word", word: Optional[str] = None,
               variant: int = 0):
    """
    Align a turn's text to its audio, returning word or phone timings.

    Args:
        turn: A Turn or a dict (see :func:`fetch_turn_audio`).
        audio: Pre-fetched audio for this turn. Fetched if omitted.
        sample_rate: Sample rate of ``audio``.
        level: ``"word"`` aligns the whole turn's text and returns
            :class:`WordTiming`. ``"phone"`` aligns one word's CMUdict phones
            and returns :class:`PhoneTiming`; ``word`` must then be given.
        word: The word to phone-align. Required when ``level="phone"``.
        variant: CMUdict pronunciation variant, for ``level="phone"``.

    Returns:
        A list of :class:`WordTiming` or :class:`PhoneTiming`, in time order,
        with times in seconds from the start of ``audio``.

    Raises:
        PhoneticsError: if alignment is impossible (word not in CMUdict, no
            alignable text, audio too short for the target).
    """
    import numpy as np
    import torch
    import torchaudio

    if audio is None:
        audio, sample_rate = fetch_turn_audio(turn, sample_rate=sample_rate)
    f = _turn_fields(turn)

    if level == "word":
        model, labels = _word_model()
        table = {c: i for i, c in enumerate(labels)}
        words = [w for w in re.findall(r"[A-Za-z']+", f["text"].upper())
                 if w and all(c in table for c in w)]
        if not words:
            raise PhoneticsError("Turn has no alignable text")
        tokens = [table[c] for w in words for c in w]
        x = torch.from_numpy(np.asarray(audio, dtype="float32")).unsqueeze(0)
        with torch.inference_mode():
            emission, _ = model(x)
        logits = emission
    elif level == "phone":
        if not word:
            raise PhoneticsError("level='phone' requires word=...")
        phones = pronounce(word, variant=variant)
        if not phones:
            raise PhoneticsError(f"{word!r} is not in CMUdict")
        ipa = []
        model, vocab = _phone_model()
        for ph in phones:
            sym = ARPABET_TO_IPA.get(ph.rstrip("012"))
            if sym is None or sym not in vocab:
                raise PhoneticsError(
                    f"phone {ph!r} of {word!r} has no token in {PHONE_ALIGN_MODEL}"
                )
            ipa.append(sym)
        tokens = [vocab[s] for s in ipa]
        a = np.asarray(audio, dtype="float32")
        a = (a - a.mean()) / (a.std() + 1e-7)     # this model expects normalized input
        x = torch.from_numpy(a).unsqueeze(0)
        with torch.inference_mode():
            logits = torch.log_softmax(model(x).logits, dim=-1)
    else:
        raise ValueError(f"level must be 'word' or 'phone', not {level!r}")

    targets = torch.tensor([tokens], dtype=torch.int32)
    if logits.size(1) < len(tokens):
        raise PhoneticsError(
            f"audio is too short ({logits.size(1)} frames) for "
            f"{len(tokens)} tokens"
        )
    ali, scores = torchaudio.functional.forced_align(logits, targets, blank=0)
    spans = torchaudio.functional.merge_tokens(ali[0], scores[0].exp())
    ratio = x.size(1) / logits.size(1) / sample_rate

    if level == "word":
        intervals = _spans_to_intervals(spans, ratio, x.size(1), sample_rate)
        out, i = [], 0
        for w in words:
            chunk = intervals[i:i + len(w)]
            sc = spans[i:i + len(w)]
            i += len(w)
            if not chunk:
                continue
            out.append(WordTiming(
                word=w, start=chunk[0][0], end=chunk[-1][1],
                score=float(np.mean([s.score for s in sc])),
            ))
        return out

    intervals = _spans_to_intervals(spans, ratio, x.size(1), sample_rate)
    return [
        PhoneTiming(phone=ipa[i], arpabet=phones[i], start=intervals[i][0],
                    end=intervals[i][1], score=float(spans[i].score), word=word)
        for i in range(min(len(phones), len(intervals)))
    ]


# ----------------------------------------------------------------------
# Formants
# ----------------------------------------------------------------------

def measure_formants(audio, sample_rate: int, start: float, end: float,
                     max_formant: float = 5500.0,
                     window: float = 0.04) -> Dict[str, Optional[float]]:
    """
    Measure F1/F2/F3 at the midpoint of ``[start, end]`` with Praat (Burg).

    Args:
        audio: 1-D float array.
        sample_rate: Its sample rate.
        start, end: Vowel interval, seconds into ``audio``.
        max_formant: Praat's formant ceiling. The convention is 5500 Hz for
            female and 5000 Hz for male vocal tracts; the wrong ceiling biases
            every value, and SPoRC carries no speaker gender, so this is the
            caller's judgement.
        window: Analysis window centred on the midpoint, in seconds.

    Returns:
        ``{"f1", "f2", "f3", "time"}``. Values are None where Praat could not
        measure one (unvoiced or too short a window).
    """
    import numpy as np
    import parselmouth

    mid = (start + end) / 2.0
    total = len(audio) / sample_rate
    lo, hi = max(0.0, mid - window / 2), min(total, mid + window / 2)
    if hi - lo <= 0.005:
        return {"f1": None, "f2": None, "f3": None, "time": mid}

    snd = parselmouth.Sound(np.asarray(audio, dtype="float64"),
                            sampling_frequency=sample_rate)
    seg = snd.extract_part(from_time=lo, to_time=hi)
    formant = seg.to_formant_burg(max_number_of_formants=5,
                                  maximum_formant=max_formant)
    t = seg.xmin + seg.get_total_duration() / 2.0
    vals = {}
    for n in (1, 2, 3):
        v = formant.get_value_at_time(n, t)
        vals[f"f{n}"] = None if (v is None or np.isnan(v)) else float(v)
    vals["time"] = mid
    return vals


# ----------------------------------------------------------------------
# The whole chain
# ----------------------------------------------------------------------

def measure_word_in_turn(turn: Any, word: str, pad: float = 0.3,
                         max_formant: float = 5500.0,
                         variant: int = 0) -> Optional[FormantMeasurement]:
    """
    Locate ``word`` in a turn's audio and measure its stressed vowel.

    Word-aligns the turn, takes the first occurrence of ``word``, phone-aligns
    just that word, and measures formants at its stressed vowel.

    Returns None when the word is not found in the alignment.
    """
    f = _turn_fields(turn)
    audio, sr = fetch_turn_audio(turn, pad=pad)

    words = align_turn(turn, audio=audio, sample_rate=sr, level="word")
    hit = next((w for w in words if w.word.lower() == word.lower()), None)
    if hit is None:
        return None

    # Phone-align only the word's own audio: a shorter, cleaner CTC problem than
    # aligning the whole turn's phones, and it keeps the target unambiguous when
    # the word occurs more than once.
    lo = max(0, int((hit.start - 0.05) * sr))
    hi = min(len(audio), int((hit.end + 0.05) * sr))
    clip = audio[lo:hi]
    phones = align_turn(turn, audio=clip, sample_rate=sr, level="phone",
                        word=word, variant=variant)
    arp = [p.arpabet for p in phones]
    vi = stressed_vowel_index(arp)
    if vi is None or vi >= len(phones):
        return None
    v = phones[vi]

    vals = measure_formants(clip, sr, v.start, v.end, max_formant=max_formant)
    if vals["f1"] is None or vals["f2"] is None:
        return None
    base = v.arpabet.rstrip("012")
    return FormantMeasurement(
        word=word, vowel=base, lexical_set=lexical_set(word),
        f1=vals["f1"], f2=vals["f2"], f3=vals["f3"],
        time=f["start_time"] + hit.start, vowel_duration=v.duration,
        speaker=f["speaker"], episode_id=f["episode_id"],
        podcast_id=f["podcast_id"], turn_text=f["text"][:200],
    )


def find_word_tokens(sporc, word: str, limit: int = 50,
                     podcast_id: Optional[str] = None,
                     max_formant: float = 5500.0,
                     pad: float = 0.3,
                     skip_errors: bool = True) -> List[FormantMeasurement]:
    """
    Search the corpus for a word and measure its vowel in every hit.

    Runs the whole chain: find turns containing ``word``, fetch each turn's
    audio, align, and measure. Network-bound -- roughly 1-2 s per token.

    Args:
        sporc: A :class:`~sporc.dataset.SPORCDataset`.
        word: The word to find. Matched as a whole word, case-insensitively.
        limit: Maximum number of measurements to return.
        podcast_id: Restrict to one podcast.
        max_formant: Praat formant ceiling (see :func:`measure_formants`).
        pad: Context padding per turn.
        skip_errors: Skip turns whose audio is unreachable (link rot is common)
            rather than raising.

    Returns:
        A list of :class:`FormantMeasurement`, at most ``limit`` long.
    """
    backend = sporc._parquet_backend
    hits = sporc.search_turns(word, mode="exact", podcast_id=podcast_id,
                              limit=limit * 4)
    out: List[FormantMeasurement] = []
    for h in hits:
        if len(out) >= limit:
            break
        if not re.search(rf"\b{re.escape(word)}\b", h.get("turn_text", ""), re.I):
            continue                       # 'exact' is a substring match: 'cot' in 'cottage'
        # search_turns() does not return mp3_url; it lives on the episode.
        ep = backend.get_episode_by_id(h.get("episode_id"))
        if not ep:
            continue
        rec = dict(h)
        rec["mp3_url"] = ep.get("mp3_url")
        try:
            m = measure_word_in_turn(rec, word, pad=pad, max_formant=max_formant)
        except PhoneticsError as e:
            if not skip_errors:
                raise
            logger.debug("skipping turn: %s", e)
            continue
        if m is not None:
            out.append(m)
    return out


def lobanov_normalize(measurements: Union[Sequence[FormantMeasurement], Any],
                      min_tokens: int = 5):
    """
    Lobanov-normalize F1/F2 per speaker (z-score within speaker).

    Raw formants are dominated by vocal-tract length, so pooling speakers
    compares anatomy rather than phonology. Lobanov centres and scales each
    speaker's own vowel space, which is what makes tokens comparable across
    people.

    Args:
        measurements: A sequence of :class:`FormantMeasurement`, or a DataFrame
            with ``speaker``/``f1``/``f2`` columns.
        min_tokens: Speakers with fewer tokens than this are dropped: a z-score
            over two tokens is noise.

    Returns:
        A DataFrame with ``f1_z``/``f2_z`` added, restricted to speakers meeting
        ``min_tokens``.
    """
    import pandas as pd

    if isinstance(measurements, pd.DataFrame):
        df = measurements.copy()
    else:
        df = pd.DataFrame([m.to_dict() for m in measurements])
    if df.empty:
        return df

    df = df[df["speaker"].notna()]
    counts = df.groupby("speaker")["f1"].transform("size")
    df = df[counts >= min_tokens]
    if df.empty:
        logger.warning(
            "No speaker has >= %d measured tokens, so nothing can be "
            "normalized. Collect more tokens per speaker.", min_tokens,
        )
        return df
    for f in ("f1", "f2"):
        g = df.groupby("speaker")[f]
        df[f"{f}_z"] = (df[f] - g.transform("mean")) / g.transform("std")
    return df
