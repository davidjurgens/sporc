#!/usr/bin/env python3
"""
Call every public method against a real Parquet layout and report what breaks.

The suite mocks ParquetBackend for most things, which is how a NoneType crash in
get_all_podcasts once survived a green run. This calls the real thing: point it
at a subset or at the full corpus and it exercises the documented surface of
SPORCDataset, Podcast, Episode and Turn, reporting failures rather than stopping
at the first one.

    python scripts/audit_api.py --data-dir subsets/tutorial
    python scripts/audit_api.py --data-dir /path/to/full --search
"""

import argparse
import logging
import os
import sys
import traceback

# Prefer this checkout. Run as a script, sys.path[0] is scripts/, so an old
# sporc installed in site-packages wins -- and 0.2.0 is on PyPI with a
# different API, so the failure looks like a bug in the audit.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.disable(logging.WARNING)

RESULTS = []


def check(name, fn, *, expect=None):
    """Run fn, record pass/fail. expect(value) may assert further."""
    try:
        value = fn()
    except Exception as exc:
        RESULTS.append((name, "FAIL", f"{type(exc).__name__}: {exc}",
                        traceback.format_exc()))
        return None
    if expect is not None:
        try:
            expect(value)
        except AssertionError as exc:
            RESULTS.append((name, "WRONG", str(exc) or "assertion failed", ""))
            return value
    RESULTS.append((name, "ok", _describe(value), ""))
    return value


def _describe(v):
    if isinstance(v, (list, tuple, set, dict)):
        return f"{type(v).__name__}({len(v)})"
    if v is None:
        return "None"
    s = str(v)
    return s[:60] + ("..." if len(s) > 60 else "")


def audit(data_dir, do_search, load_audio):
    from sporc import SPORCDataset
    from sporc.episode import TimeRangeBehavior

    ds = check("SPORCDataset(parquet_dir=...)",
               lambda: SPORCDataset(parquet_dir=data_dir,
                                    load_audio_features=load_audio),
               expect=lambda d: (_ for _ in ()).throw(AssertionError("None"))
               if d is None else None)
    if ds is None:
        return

    b = ds._parquet_backend

    # ---- dataset level -------------------------------------------------
    check("get_dataset_statistics", ds.get_dataset_statistics,
          expect=lambda v: _assert(v and v.get("total_podcasts", 0) > 0,
                                   "no podcasts in statistics"))
    pods = check("get_all_podcasts", ds.get_all_podcasts,
                 expect=lambda v: _assert(len(v) > 0, "empty"))
    eps = check("get_all_episodes", ds.get_all_episodes,
                expect=lambda v: _assert(len(v) > 0, "empty"))
    check("iterate_podcasts", lambda: list(ds.iterate_podcasts(max_podcasts=3)),
          expect=lambda v: _assert(len(v) == 3, f"got {len(v)}"))
    check("iterate_episodes", lambda: list(ds.iterate_episodes(max_episodes=5)),
          expect=lambda v: _assert(len(v) == 5, f"got {len(v)}"))
    check("iterate_episodes(random)",
          lambda: list(ds.iterate_episodes(max_episodes=3,
                                           sampling_mode="random")))
    if not pods:
        return
    pod = pods[0]
    check("search_podcast(by title)", lambda: ds.search_podcast(pod.title),
          expect=lambda v: _assert(v is not None, "not found"))
    check("search_episodes(min_duration)",
          lambda: ds.search_episodes(min_duration=60))
    check("search_episodes(category)",
          lambda: ds.search_episodes(category=pod.primary_category))
    check("search_episodes_by_subcategory",
          lambda: ds.search_episodes_by_subcategory("Technology",
                                                    max_episodes=3))
    check("search_podcasts_by_subcategory",
          lambda: ds.search_podcasts_by_subcategory("Technology"))
    check("prefetch(local)", lambda: ds.prefetch([pod.podcast_id]))

    # ---- podcast level -------------------------------------------------
    for attr in ("podcast_id", "title", "num_episodes", "total_duration_hours",
                 "avg_episode_duration_minutes", "primary_category",
                 "categories", "main_categories", "subcategories",
                 "primary_subcategory", "host_names", "guest_names", "language",
                 "explicit", "earliest_episode_date", "latest_episode_date",
                 "longest_episode", "shortest_episode", "solo_episodes",
                 "interview_episodes", "panel_episodes", "long_form_episodes",
                 "short_form_episodes", "image_url", "itunes_author",
                 "created_on", "last_update"):
        check(f"Podcast.{attr}", lambda a=attr: getattr(pod, a))
    check("Podcast.get_episode_statistics", pod.get_episode_statistics)
    check("Podcast.to_dict", pod.to_dict)
    check("Podcast.get_episodes_by_duration_range",
          lambda: pod.get_episodes_by_duration_range(0, 1e9))
    check("Podcast.get_episodes_by_speaker_count",
          lambda: pod.get_episodes_by_speaker_count(1, 10))
    check("Podcast.get_episodes_by_category",
          lambda: pod.get_episodes_by_category(pod.primary_category))
    check("Podcast.get_episode_by_title",
          lambda: pod.get_episode_by_title(pod.episodes[0].title))

    # ---- episode level -------------------------------------------------
    ep = next((e for p in pods for e in p.episodes if e.has_turn_data), None)
    if ep is None:
        RESULTS.append(("episode with turns", "FAIL", "none found", ""))
        return
    for attr in ("episode_id", "podcast_id", "title", "duration_minutes",
                 "duration_hours", "episode_date", "categories",
                 "primary_category", "language", "explicit", "num_hosts",
                 "num_guests", "num_main_speakers", "host_names", "guest_names",
                 "has_guests", "is_solo", "is_interview", "is_panel",
                 "is_long_form", "is_short_form", "turn_count",
                 "avg_turn_duration", "overlap_prop_duration",
                 "overlap_prop_turn_count", "total_speaker_labels",
                 "has_turn_data", "has_turns", "image_url"):
        check(f"Episode.{attr}", lambda a=attr: getattr(ep, a))
    turns = check("Episode.turns", lambda: ep.turns,
                  expect=lambda v: _assert(len(v) > 0, "no turns"))
    check("Episode.get_all_turns", ep.get_all_turns)
    check("Episode.get_turn_statistics", ep.get_turn_statistics)
    check("Episode.get_host_turns", ep.get_host_turns)
    check("Episode.get_guest_turns", ep.get_guest_turns)
    check("Episode.get_turns_by_role", lambda: ep.get_turns_by_role("host"))
    check("Episode.get_turns_by_min_length",
          lambda: ep.get_turns_by_min_length(5))
    check("Episode.get_turns_by_time_range",
          lambda: ep.get_turns_by_time_range(0, 120))
    check("Episode.get_turns_by_time_range(trim)",
          lambda: ep.get_turns_by_time_range_with_trimming(0, 120))
    check("Episode.get_turns_by_time_range(behavior)",
          lambda: ep.get_turns_by_time_range(
              0, 120, behavior=TimeRangeBehavior.STRICT))
    check("Episode.sliding_window",
          lambda: list(ep.sliding_window(window_size=3)))
    check("Episode.sliding_window_by_time",
          lambda: list(ep.sliding_window_by_time(60)))
    check("Episode.get_window_statistics",
          lambda: ep.get_window_statistics(window_size=3))
    check("Episode.to_dict", ep.to_dict)
    if turns:
        t = turns[0]
        for attr in ("speaker", "text", "start_time", "end_time", "duration",
                     "turn_count", "word_count", "token_count",
                     "words_per_second", "primary_speaker", "is_overlapping",
                     "is_host", "is_guest", "inferred_speaker_name",
                     "inferred_speaker_role", "speakers_recomputed", "mp3_url"):
            check(f"Turn.{attr}", lambda a=attr: getattr(t, a))
        check("Turn.get_audio_features", t.get_audio_features)
        check("Turn.to_dict", t.to_dict)
        check("Turn.contains_time", lambda: t.contains_time(t.start_time))
        check("Turn.overlaps_with", lambda: t.overlaps_with(turns[-1]))
        check("Turn.word_count is an int",
              lambda: t.word_count,
              expect=lambda v: _assert(isinstance(v, int), f"{type(v)}"))
        check("Turn.mp3_url is populated", lambda: t.mp3_url,
              expect=lambda v: _assert(v, "empty; 1.1 moved it to the episode"))

    # ---- metrics and lazy loading --------------------------------------
    check("get_episode_metrics", lambda: ds.get_episode_metrics(ep.episode_id))
    check("filter_episodes_by_metrics",
          lambda: ds.filter_episodes_by_metrics(min_word_count=10))
    check("get_turn_metrics",
          lambda: ds.get_turn_metrics(ep.podcast_id, ep.episode_id))
    check("load_turns_for_episode", lambda: ds.load_turns_for_episode(ep))
    check("load_turns_for_podcast", lambda: ds.load_turns_for_podcast(pod))
    check("estimate_word_audio",
          lambda: ds.estimate_word_audio(ep.podcast_id, ep.episode_id,
                                         turns[0].text.split()[0]))

    # ---- backend internals used by scripts -----------------------------
    check("backend.manifest", lambda: b.manifest)
    check("backend.shard_map", lambda: b.shard_map)
    check("backend.get_all_podcast_ids", b.get_all_podcast_ids)
    check("backend.has_podcast", lambda: b.has_podcast(pod.podcast_id))
    check("backend.has_turn_data", lambda: b.has_turn_data(ep.podcast_id))
    check("backend.episode_has_turn_data",
          lambda: b.episode_has_turn_data(ep.podcast_id, ep.episode_id))
    check("backend.get_statistics", b.get_statistics)
    check("backend.get_podcasts_by_category",
          lambda: b.get_podcasts_by_category(pod.primary_category))
    check("backend.local_turn_podcast_ids", b.local_turn_podcast_ids)
    check("backend.ensure_podcast_data",
          lambda: b.ensure_podcast_data(pod.podcast_id))

    # ---- search (needs the duckdb files) -------------------------------
    if do_search:
        check("search_turns(fts)",
              lambda: ds.search_turns("climate change", mode="fts", limit=3))
        check("search_turns(exact)",
              lambda: ds.search_turns("the", mode="exact", limit=3))
        check("search_turns(regex)",
              lambda: ds.search_turns("clim(ate|b)", mode="regex", limit=3))
        check("search_episodes_by_text",
              lambda: ds.search_episodes_by_text("climate", limit=3))
        check("concordance", lambda: ds.concordance("the", limit=3))
        check("search_by_speaker_name",
              lambda: ds.search_by_speaker_name("John", limit=3))


def _assert(cond, msg):
    if not cond:
        raise AssertionError(msg)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data-dir", required=True)
    p.add_argument("--search", action="store_true",
                   help="also exercise the DuckDB-backed search methods")
    p.add_argument("--audio", action="store_true",
                   help="load acoustic features (off by default, as in the API)")
    args = p.parse_args()

    import sporc
    if sporc.__version__ < "1.0":
        raise SystemExit(
            f"imported sporc {sporc.__version__} from "
            f"{os.path.dirname(sporc.__file__)}; this audit needs the 1.x API")
    print(f"auditing sporc {sporc.__version__} from "
          f"{os.path.dirname(sporc.__file__)} against {args.data_dir}\n")

    audit(args.data_dir, args.search, args.audio)

    bad = [r for r in RESULTS if r[1] != "ok"]
    width = max(len(r[0]) for r in RESULTS)
    for name, status, detail, _ in RESULTS:
        if status != "ok":
            print(f"{status:5s} {name:{width}s}  {detail}")
    print(f"\n{len(RESULTS) - len(bad)}/{len(RESULTS)} ok, {len(bad)} problems")
    for name, status, detail, tb in bad:
        if tb:
            print(f"\n--- {name} ---\n{tb}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
