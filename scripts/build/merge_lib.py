"""
Merge prosody word timings with RTTM speaker segments to produce speaker turns.

This replays the join that produced ``diarizationMerged`` on the episodes it
skipped: ``prosodyMerged`` supplies word-level (start, end, content) plus
acoustic features, ``diarization/mayJune`` supplies (start, duration, speaker)
segments. Words are labelled by overlapping segment, then runs of words sharing
a speaker set become turns.
"""

import csv
import json
import math

import pyarrow.csv as pv

# Acoustic columns carried per word by prosodyMerged. The Slope variants are
# absent from a small minority of files, so they are read defensively.
BASE_FEATURES = [
    "mfcc1_sma3", "mfcc2_sma3", "mfcc3_sma3", "mfcc4_sma3",
    "F0semitoneFrom27.5Hz_sma3nz", "F1frequency_sma3nz",
]


def parse_rttm(path):
    """Read an RTTM file into sorted (start, end, speaker) segments."""
    segs = []
    with open(path) as fh:
        for line in fh:
            parts = line.split()
            if len(parts) < 8 or parts[0] != "SPEAKER":
                continue
            try:
                start = float(parts[3])
                dur = float(parts[4])
            except ValueError:
                continue
            segs.append((start, start + dur, parts[7]))
    segs.sort()
    return segs


def _parse_prosody_slow(path):
    """Row-at-a-time fallback for files Arrow's CSV reader rejects."""
    words = []
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            try:
                start = float(row["start"])
                end = float(row["end"])
            except (TypeError, ValueError, KeyError):
                continue
            if end < start:
                start, end = end, start
            feats = {}
            for k in BASE_FEATURES:
                v = row.get(k)
                try:
                    feats[k] = float(v) if v not in (None, "") else None
                except ValueError:
                    feats[k] = None
            words.append({
                "start": start, "end": end,
                "content": row.get("content") or "", "feats": feats,
            })
    return words


def parse_prosody(path):
    """
    Read a prosodyMerged CSV into word dicts.

    Arrow's CSV reader is ~6x faster than the csv module here and the files run
    to a couple of megabytes each, which is the difference between a build of
    hours and one of days. A minority of files omit the Slope columns or carry
    malformed rows, so anything Arrow rejects falls back to the slow path.
    """
    try:
        tbl = pv.read_csv(path)
    except Exception:
        return _parse_prosody_slow(path)

    names = set(tbl.column_names)
    if not {"start", "end"} <= names:
        return _parse_prosody_slow(path)

    starts = tbl.column("start").to_pylist()
    ends = tbl.column("end").to_pylist()
    contents = (tbl.column("content").to_pylist() if "content" in names
                else [""] * len(starts))
    cols = {k: (tbl.column(k).to_pylist() if k in names else [None] * len(starts))
            for k in BASE_FEATURES}

    words = []
    for i, (s, e) in enumerate(zip(starts, ends)):
        if s is None or e is None:
            continue
        s = float(s)
        e = float(e)
        # About one word row in 5,000 has its bounds inverted. Left alone these
        # produce turns with negative duration, so the interval is put back the
        # right way round rather than dropping the word and losing its text.
        if e < s:
            s, e = e, s
        words.append({
            "start": s,
            "end": e,
            "content": contents[i] or "",
            "feats": {k: cols[k][i] for k in BASE_FEATURES},
        })
    return words


def assign_speakers(words, segs):
    """
    Label each word with the speakers whose segment overlaps it.

    An active set is required rather than a single forward pointer. Segments are
    sorted by start, so a long segment stays open across many later words; a
    pointer that advanced past it whenever it fell behind the current word would
    silently drop that speaker for the rest of its span. That is precisely the
    defect in the pipeline that produced ``diarizationMerged``, which under-
    assigns speakers during long segments and in overlapping speech.

    One entry per overlapping segment, repeats included: a word spanning two
    segments of the same speaker is labelled with that speaker twice, which
    carries how the word was split across segments.

    Word rows are not reliably time-ordered in the source, so they are walked in
    sorted order and the labels written back to their original positions.
    Feeding the raw order to a streaming merge is what silently drops speakers.
    """
    order = sorted(range(len(words)), key=lambda i: words[i]["start"])
    out = [None] * len(words)
    active = []  # indices into segs, held in start order
    p = 0
    for i in order:
        w = words[i]
        while p < len(segs) and segs[p][0] < w["end"]:
            active.append(p)
            p += 1
        if active:
            active = [j for j in active if segs[j][1] > w["start"]]
        # The pointer only moves forward, so a short word following a long one
        # can leave segments in the active set that start after it ends.
        out[i] = [segs[j][2] for j in active if segs[j][0] < w["end"]]
    return out


def aggregate_turns(words, speakers):
    """
    Collapse runs of words sharing a speaker set into turns.

    Words with no speaker attach to the run in progress so their text is not
    dropped; a leading unlabelled run is emitted with an empty speaker list,
    matching how v1.0 represents ``speaker`` as a list.
    """
    turns = []
    cur = None
    # Words before the first speaker segment have nobody to attach backwards to.
    # They are held and prepended to the first labelled turn rather than emitted
    # as a speaker-less turn, which is how v1.0 treats a leading run.
    pending = []
    for w, spk in zip(words, speakers):
        if cur is None and not spk:
            pending.append(w)
            continue
        # Group on the distinct speakers, not the raw list. A word spanning two
        # segments of one speaker is labelled with that speaker twice, and
        # treating ['S01'] and ['S01','S01'] as different would chop a single
        # continuous turn into fragments at every segment boundary.
        uniq = list(dict.fromkeys(spk))
        key = tuple(sorted(uniq))
        if cur is not None and (not spk or key == cur["key"]):
            cur["words"].append(w)
            # Span from the extremes, not from the first row: word rows are not
            # reliably time-ordered, so taking the first row's start can put the
            # turn's start after its end and yield a negative duration.
            cur["start"] = min(cur["start"], w["start"])
            cur["end"] = max(cur["end"], w["end"])
            continue
        if cur is not None:
            turns.append(cur)
        group = pending + [w]
        cur = {"key": key, "speaker": uniq, "words": group,
               "start": min(x["start"] for x in group),
               "end": max(x["end"] for x in group)}
        pending = []
    if cur is not None:
        turns.append(cur)
    elif pending:
        # Nothing was ever labelled: keep the text as one speaker-less turn.
        turns.append({"key": (), "speaker": [], "words": pending,
                      "start": min(x["start"] for x in pending),
                      "end": max(x["end"] for x in pending)})
    return turns


def turn_features(words):
    """Mean and standard deviation of each acoustic feature over a turn."""
    out = {}
    for k in BASE_FEATURES:
        vals = [w["feats"].get(k) for w in words]
        vals = [v for v in vals if v is not None and not math.isnan(v)]
        if not vals:
            out[k + "Mean"] = None
            out[k + "StDev"] = None
            continue
        mean = sum(vals) / len(vals)
        var = sum((v - mean) ** 2 for v in vals) / len(vals) if len(vals) > 1 else 0.0
        out[k + "Mean"] = mean
        out[k + "StDev"] = math.sqrt(var)
    return out


def build_turns(prosody_path, rttm_path):
    """Full pipeline for one episode: word timings + segments -> turn records."""
    words = parse_prosody(prosody_path)
    segs = parse_rttm(rttm_path)
    speakers = assign_speakers(words, segs)
    turns = []
    for i, t in enumerate(aggregate_turns(words, speakers)):
        text = "".join(w["content"] for w in t["words"])
        rec = {
            "speaker": t["speaker"],
            "turn_text": text,
            "start_time": t["start"],
            "end_time": t["end"],
            "duration": t["end"] - t["start"],
            "turn_count": i,
            "word_count": len(t["words"]),
        }
        rec.update(turn_features(t["words"]))
        turns.append(rec)
    return turns


def read_diarization_merged(path):
    """Reference word-level output, for validating assign_speakers."""
    rows = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows
