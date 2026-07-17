"""
Offline unit tests for sporc.phonetics.

Everything here runs without network, audio, or model downloads. The parts that
need those (fetch_turn_audio, align_turn) are exercised in the tutorial
notebooks and marked slow elsewhere.
"""

import pytest

from sporc.phonetics import (
    ARPABET_TO_IPA,
    ARPABET_VOWELS,
    LOT_WORDS,
    THOUGHT_WORDS,
    FormantMeasurement,
    PhoneTiming,
    PhoneticsError,
    WordTiming,
    _spans_to_intervals,
    _turn_fields,
    fetch_turn_audio,
    lexical_set,
    lobanov_normalize,
    pronounce,
    stressed_vowel_index,
)


class _Span:
    """Stand-in for torchaudio's TokenSpan."""

    def __init__(self, start, end, score=1.0):
        self.start, self.end, self.score = start, end, score


@pytest.mark.unit
class TestLexicalSets:
    def test_caught_is_thought_despite_merged_cmudict(self):
        # The trap this guards: cmudict's first pronunciation of "caught" is
        # K AA1 T -- the LOT vowel -- because the merger is in the dictionary.
        # Classifying by the dictionary vowel would call caught LOT and answer
        # the research question before measuring anything.
        assert pronounce("caught")[1].rstrip("012") == "AA"
        assert lexical_set("caught") == "THOUGHT"
        assert lexical_set("cot") == "LOT"

    def test_sets_are_disjoint(self):
        assert not (THOUGHT_WORDS & LOT_WORDS)

    def test_lookup_is_case_and_space_insensitive(self):
        assert lexical_set("  CAUGHT ") == "THOUGHT"

    def test_unknown_word_is_none(self):
        assert lexical_set("banana") is None

    @pytest.mark.parametrize("word,expected", [
        ("talk", "THOUGHT"), ("thought", "THOUGHT"), ("across", "THOUGHT"),
        ("lot", "LOT"), ("not", "LOT"), ("job", "LOT"),
    ])
    def test_known_members(self, word, expected):
        assert lexical_set(word) == expected


@pytest.mark.unit
class TestG2P:
    def test_pronounce_known_word(self):
        assert pronounce("cot") == ["K", "AA1", "T"]

    def test_pronounce_unknown_word(self):
        assert pronounce("zzzqqq") is None

    def test_variant_selection(self):
        # "caught" has both AO1 and AA1 variants listed.
        v0, v1 = pronounce("caught", 0), pronounce("caught", 1)
        assert {v0[1], v1[1]} == {"AA1", "AO1"}

    def test_variant_index_is_clamped(self):
        assert pronounce("cot", variant=99) == pronounce("cot", variant=0)

    def test_stressed_vowel_picks_second_vowel_in_across(self):
        # AH0 K R AO1 S -- the vowel of interest is the stressed one, not the
        # first. Word-midpoint heuristics get this wrong.
        phones = pronounce("across")
        assert stressed_vowel_index(phones) == 3
        assert phones[3] == "AO1"

    def test_stressed_vowel_none_without_vowels(self):
        assert stressed_vowel_index(["K", "T"]) is None

    def test_falls_back_to_first_vowel_when_unstressed(self):
        assert stressed_vowel_index(["K", "AH0", "T"]) == 1

    def test_every_arpabet_vowel_maps_to_ipa(self):
        assert ARPABET_VOWELS <= set(ARPABET_TO_IPA)

    def test_target_words_are_all_pronounceable(self):
        # A word in the lexical sets that cmudict does not know is dead weight:
        # it can never yield a measurement.
        for w in sorted(THOUGHT_WORDS | LOT_WORDS):
            assert pronounce(w) is not None, f"{w} missing from cmudict"

    def test_target_words_have_a_stressed_vowel(self):
        for w in sorted(THOUGHT_WORDS | LOT_WORDS):
            assert stressed_vowel_index(pronounce(w)) is not None, w


@pytest.mark.unit
class TestSpansToIntervals:
    def test_peaky_spans_become_contiguous(self):
        # CTC emits one frame per token, so raw spans are all ~1 frame wide
        # whatever the real duration. Intervals must run spike-to-spike.
        spans = [_Span(0, 1), _Span(5, 6), _Span(10, 11)]
        ratio = 0.02
        out = _spans_to_intervals(spans, ratio, n_samples=16000, sample_rate=16000)
        assert out[0] == (0.0, 0.1)
        assert out[1] == (0.1, 0.2)
        assert out[2][0] == 0.2
        assert out[2][1] == pytest.approx(1.0)   # last runs to end of audio

    def test_durations_are_not_one_frame(self):
        spans = [_Span(0, 1), _Span(5, 6)]
        out = _spans_to_intervals(spans, 0.02, 16000, 16000)
        assert (out[0][1] - out[0][0]) > 0.02

    def test_single_span_spans_audio(self):
        out = _spans_to_intervals([_Span(0, 1)], 0.02, 16000, 16000)
        assert out == [(0.0, 1.0)]

    def test_intervals_never_exceed_audio(self):
        out = _spans_to_intervals([_Span(0, 1), _Span(400, 401)], 0.02, 16000, 16000)
        assert all(e <= 1.0 for _, e in out)


@pytest.mark.unit
class TestTurnFields:
    def test_reads_search_result_dict(self):
        f = _turn_fields({"turn_text": "hi", "start_time": 1.0, "end_time": 2.0,
                          "mp3_url": "u", "speaker_name": "Ann",
                          "episode_id": "e", "podcast_id": "p"})
        assert f["text"] == "hi" and f["speaker"] == "Ann"
        assert f["start_time"] == 1.0 and f["end_time"] == 2.0

    def test_reads_turn_object(self):
        from sporc.turn import Turn
        t = Turn(speaker=["SPEAKER_00"], text="hi", start_time=1.0,
                 end_time=2.0, duration=1.0, turn_count=1,
                 inferred_speaker_name="Bo", mp3_url="u")
        f = _turn_fields(t)
        assert f["text"] == "hi" and f["speaker"] == "Bo" and f["mp3_url"] == "u"

    def test_turn_object_falls_back_to_speaker_label(self):
        from sporc.turn import Turn
        t = Turn(speaker=["SPEAKER_00"], text="hi", start_time=0.0,
                 end_time=1.0, duration=1.0, turn_count=1)
        assert _turn_fields(t)["speaker"] == "SPEAKER_00"

    def test_missing_url_raises_before_any_network(self):
        with pytest.raises(PhoneticsError, match="no mp3_url"):
            fetch_turn_audio({"turn_text": "x", "start_time": 0, "end_time": 1})


@pytest.mark.unit
class TestDataclasses:
    def test_word_timing_duration(self):
        assert WordTiming("hi", 1.0, 1.5, 0.9).duration == pytest.approx(0.5)

    def test_phone_timing_vowel_and_stress(self):
        p = PhoneTiming("ɔ", "AO1", 0.0, 0.1, 1.0)
        assert p.is_vowel and p.stress == 1

    def test_phone_timing_consonant(self):
        p = PhoneTiming("k", "K", 0.0, 0.1, 1.0)
        assert not p.is_vowel and p.stress is None

    def test_measurement_to_dict(self):
        m = FormantMeasurement("caught", "AO", "THOUGHT", 700.0, 1100.0, 2500.0,
                               1.0, 0.1, speaker="Ann")
        d = m.to_dict()
        assert d["lexical_set"] == "THOUGHT" and d["f1"] == 700.0


@pytest.mark.unit
class TestLobanov:
    def _rows(self, speaker, n, f1, f2):
        return [FormantMeasurement("cot", "AA", "LOT", f1 + i, f2 + i, None,
                                   0.0, 0.1, speaker=speaker)
                for i in range(n)]

    def test_zscores_within_speaker(self):
        rows = self._rows("A", 6, 700, 1200) + self._rows("B", 6, 500, 1000)
        df = lobanov_normalize(rows, min_tokens=5)
        assert {"f1_z", "f2_z"} <= set(df.columns)
        # Normalization is per speaker, so each speaker's own mean is ~0 even
        # though their raw formants differ by 200 Hz.
        for sp in ("A", "B"):
            assert df[df.speaker == sp]["f1_z"].mean() == pytest.approx(0, abs=1e-9)

    def test_drops_speakers_below_min_tokens(self):
        rows = self._rows("A", 6, 700, 1200) + self._rows("B", 2, 500, 1000)
        df = lobanov_normalize(rows, min_tokens=5)
        assert set(df.speaker.unique()) == {"A"}

    def test_empty_input(self):
        assert lobanov_normalize([]).empty

    def test_all_below_threshold_returns_empty(self):
        assert lobanov_normalize(self._rows("A", 2, 700, 1200), min_tokens=5).empty

    def test_drops_rows_without_speaker(self):
        rows = self._rows("A", 6, 700, 1200) + self._rows(None, 6, 500, 1000)
        df = lobanov_normalize(rows, min_tokens=5)
        assert set(df.speaker.unique()) == {"A"}


@pytest.mark.unit
class TestMeasureFormants:
    def test_synthetic_vowel_formants_land_near_truth(self):
        # A synthetic vowel with resonances at 700/1200 Hz should measure near
        # those. This checks the parselmouth call and the windowing, not the
        # aligner.
        np = pytest.importorskip("numpy")
        pytest.importorskip("parselmouth")
        from sporc.phonetics import measure_formants
        sr, dur, f0 = 16000, 0.4, 120.0
        t = np.arange(int(sr * dur)) / sr
        # buzz -> two resonators
        sig = np.zeros_like(t)
        for h in range(1, 40):
            sig += np.sin(2 * np.pi * f0 * h * t) / h
        out = np.zeros_like(sig)
        for f_c, bw in ((700.0, 80.0), (1200.0, 90.0)):
            r = np.exp(-np.pi * bw / sr)
            a1, a2 = -2 * r * np.cos(2 * np.pi * f_c / sr), r * r
            y = np.zeros_like(sig)
            for i in range(2, len(sig)):
                y[i] = sig[i] - a1 * y[i - 1] - a2 * y[i - 2]
            out += y / (np.abs(y).max() + 1e-9)
        out = (out / (np.abs(out).max() + 1e-9)).astype("float32")
        vals = measure_formants(out, sr, 0.15, 0.25)
        assert vals["f1"] == pytest.approx(700, abs=150)
        assert vals["f2"] == pytest.approx(1200, abs=250)

    def test_too_short_window_returns_nones(self):
        np = pytest.importorskip("numpy")
        pytest.importorskip("parselmouth")
        from sporc.phonetics import measure_formants
        audio = np.zeros(16000, dtype="float32")
        vals = measure_formants(audio, 16000, 0.5, 0.5005)
        assert vals["f1"] is None and vals["f2"] is None


@pytest.mark.unit
class TestPhoneClipGuard:
    """
    forced_align makes the target sequence explain all of the audio, so
    phone-aligning a word against a whole turn forces its handful of phones
    across minutes of other speech and returns a confident, meaningless
    answer. The guard turns that into an error.
    """

    def _turn(self, secs):
        return {"turn_text": "we talk a lot", "start_time": 0.0,
                "end_time": secs, "mp3_url": "u", "speaker_name": "Ann",
                "episode_id": "e", "podcast_id": "p"}

    def test_rejects_a_whole_turn(self):
        import numpy as np
        from sporc.phonetics import MAX_PHONE_CLIP_SECONDS, align_turn

        secs = MAX_PHONE_CLIP_SECONDS + 30
        audio = np.zeros(int(16000 * secs), dtype="float32")

        with pytest.raises(PhoneticsError, match="whole turn|one word's clip"):
            align_turn(self._turn(secs), audio=audio, sample_rate=16000,
                       level="phone", word="talk")

    def test_error_names_the_right_entry_point(self):
        import numpy as np
        from sporc.phonetics import MAX_PHONE_CLIP_SECONDS, align_turn

        secs = MAX_PHONE_CLIP_SECONDS + 1
        audio = np.zeros(int(16000 * secs), dtype="float32")

        with pytest.raises(PhoneticsError, match="measure_word_in_turn"):
            align_turn(self._turn(secs), audio=audio, sample_rate=16000,
                       level="phone", word="talk")

    def test_guard_fires_before_any_model_work(self, monkeypatch):
        # It must reject on duration alone -- loading the model to find out
        # would cost more than the check saves.
        import numpy as np
        import sporc.phonetics as ph

        def boom():
            raise AssertionError("model must not be loaded to reject a long clip")

        monkeypatch.setattr(ph, "_phone_model", boom)
        audio = np.zeros(int(16000 * 60), dtype="float32")

        with pytest.raises(PhoneticsError):
            ph.align_turn(self._turn(60), audio=audio, sample_rate=16000,
                          level="phone", word="talk")


@pytest.mark.unit
class TestMaxTurnDuration:
    """
    Locating a word means aligning the turn's whole audio at ~0.45x realtime,
    so cost tracks the turn, not the word. Corpus turns are long -- median ~64s
    for a common word, longest seen 3,240s -- so without a cap a single token
    can take tens of minutes. This is what made notebook 07 time out.
    """

    class _FakeSporc:
        def __init__(self, durations):
            self._durations = durations
            self._parquet_backend = self

        def search_turns(self, word, mode=None, podcast_id=None, limit=None):
            return [{"turn_text": f"we {word} a lot", "start_time": 0.0,
                     "end_time": d, "episode_id": f"e{i}",
                     "podcast_id": "p", "speaker_name": "Ann"}
                    for i, d in enumerate(self._durations)]

        def get_episode_by_id(self, eid):
            return {"mp3_url": f"http://example.com/{eid}.mp3"}

    def _run(self, monkeypatch, durations, **kw):
        import sporc.phonetics as ph

        seen = []

        def fake_measure(turn, word, **kwargs):
            seen.append(float(turn["end_time"]) - float(turn["start_time"]))
            return None

        monkeypatch.setattr(ph, "measure_word_in_turn", fake_measure)
        ph.find_word_tokens(self._FakeSporc(durations), "talk", limit=10, **kw)
        return seen

    def test_long_turns_are_never_fetched(self, monkeypatch):
        seen = self._run(monkeypatch, [5.0, 3240.0, 10.0], max_turn_duration=30.0)

        assert 3240.0 not in seen
        assert sorted(seen) == [5.0, 10.0]

    def test_cap_is_inclusive_of_the_boundary(self, monkeypatch):
        seen = self._run(monkeypatch, [30.0], max_turn_duration=30.0)

        assert seen == [30.0]

    def test_none_disables_the_cap(self, monkeypatch):
        seen = self._run(monkeypatch, [5.0, 3240.0], max_turn_duration=None)

        assert 3240.0 in seen

    def test_default_caps_by_default(self, monkeypatch):
        # The default matters: it is what stops a notebook from hanging.
        seen = self._run(monkeypatch, [5.0, 3240.0])

        assert 3240.0 not in seen

    def test_skipped_turns_are_reported(self, monkeypatch, caplog):
        import logging

        with caplog.at_level(logging.INFO, logger="sporc.phonetics"):
            self._run(monkeypatch, [5.0, 3240.0, 900.0], max_turn_duration=30.0)

        # Silently returning only short turns would read as "the corpus has few
        # tokens of this word".
        assert "skipped 2" in caplog.text


@pytest.mark.unit
class TestLobanovSpeakerIdentity:
    """
    Lobanov exists to remove the vocal-tract-length confound, so what counts as
    "a speaker" is the whole point. Two ways to get it wrong: pool the corpus's
    NO_INFERRED_SPEAKER placeholder (its most common value) into one person, or
    merge same-named people across shows.
    """

    @staticmethod
    def _rows(specs):
        import pandas as pd
        rows = []
        for pid, spk, f1, f2 in specs:
            rows.append({"speaker": spk, "podcast_id": pid, "f1": f1, "f2": f2,
                         "lexical_set": "LOT", "word": "not"})
        return pd.DataFrame(rows)

    def test_placeholder_speakers_are_dropped(self):
        from sporc.phonetics import lobanov_normalize

        df = self._rows([("p1", "NO_INFERRED_SPEAKER", 700 + i, 1200 + i)
                         for i in range(10)])

        out = lobanov_normalize(df, min_tokens=3)

        # Pooling these would z-score across ten different vocal tracts.
        assert out.empty

    def test_anonymous_diarization_labels_are_dropped(self):
        from sporc.phonetics import lobanov_normalize

        df = self._rows([("p1", "SPEAKER_00", 700 + i, 1200 + i)
                         for i in range(10)])

        out = lobanov_normalize(df, min_tokens=3)

        assert out.empty

    def test_real_speakers_survive_alongside_placeholders(self):
        from sporc.phonetics import lobanov_normalize

        specs = [("p1", "NO_INFERRED_SPEAKER", 700 + i, 1200 + i) for i in range(6)]
        specs += [("p1", "Ann Real", 800 + i * 10, 1300 + i * 10) for i in range(6)]

        out = lobanov_normalize(df := self._rows(specs), min_tokens=3)

        assert set(out.speaker.unique()) == {"Ann Real"}
        assert len(out) == 6

    def test_a_name_is_one_speaker_across_podcasts(self):
        from sporc.phonetics import lobanov_normalize

        # The same guest on two shows is one person, and pooling their tokens
        # is what makes min_tokens reachable. The accepted cost is that two
        # distinct people sharing a name pool too -- SPoRC has no canonical
        # speaker id yet.
        specs = [("p1", "John Smith", 700 + i, 1200 + i) for i in range(3)]
        specs += [("p2", "John Smith", 700 + i, 1200 + i) for i in range(3)]

        out = lobanov_normalize(self._rows(specs), min_tokens=5)

        assert len(out) == 6
        assert out.f1_z.mean() == pytest.approx(0, abs=1e-9)

    def test_min_tokens_counts_a_name_across_podcasts(self):
        from sporc.phonetics import lobanov_normalize

        # Three tokens on each of two shows is six for this person, not two
        # speakers with three each.
        specs = [("p1", "John Smith", 700 + i, 1200 + i) for i in range(3)]
        specs += [("p2", "John Smith", 800 + i, 1300 + i) for i in range(3)]

        out = lobanov_normalize(self._rows(specs), min_tokens=6)

        assert len(out) == 6

    def test_works_without_a_podcast_id_column(self):
        from sporc.phonetics import lobanov_normalize

        df = self._rows([("p1", "Ann", 700 + i, 1200 + i) for i in range(5)])
        df = df.drop(columns="podcast_id")

        out = lobanov_normalize(df, min_tokens=3)

        assert len(out) == 5

    def test_dropped_placeholders_are_reported(self, caplog):
        import logging

        from sporc.phonetics import lobanov_normalize

        specs = [("p1", "NO_INFERRED_SPEAKER", 700, 1200) for _ in range(4)]
        specs += [("p1", "Ann Real", 800 + i * 10, 1300 + i * 10) for i in range(4)]

        with caplog.at_level(logging.INFO, logger="sporc.phonetics"):
            lobanov_normalize(self._rows(specs), min_tokens=3)

        assert "unidentified speakers" in caplog.text

    def test_null_podcast_id_does_not_discard_tokens(self):
        # pandas 3 makes a null part poison the whole concatenated key (NA),
        # and groupby drops NA keys -- so this silently returned nothing at all
        # for measurements built without a podcast_id.
        from sporc.phonetics import FormantMeasurement, lobanov_normalize

        rows = [FormantMeasurement("cot", "AA", "LOT", 700 + i, 1200 + i, None,
                                   0.0, 0.1, speaker="A")
                for i in range(6)]

        out = lobanov_normalize(rows, min_tokens=5)

        assert len(out) == 6
        assert {"f1_z", "f2_z"} <= set(out.columns)
