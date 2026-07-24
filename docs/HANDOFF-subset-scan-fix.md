# Handoff: tutorial-subset guest scan — FIXED via precomputed guest index (1.1.3)

## TL;DR (measured 2026-07-24, cleared-cache full rebuild, repo HEAD `a4d8181` + sporc 1.1.3)
- **The guest scan is gone.** Dataset 1.1.3 ships `metadata/guest_index.parquet`
  (0.62 MB) + `metadata/guest_episode_index.parquet` (0.9 MB), and
  `scripts/build_tutorial_subset.py:diarized_guest_index` now reads that one small
  file (via `backend.diarized_guest_podcasts()`) instead of range-scanning
  `guest_speaker_labels` out of all 140 episode parts.
- **Guest scan phase: 0.1 s, 0 bytes over the wire** (`range_calls=0`), and it finds
  the **identical 24,019 diarized guests** the 30 GB scan did. The index rides in with
  the metadata catalogs, so it costs nothing extra to fetch.
- Full cold-cache rebuild now downloads **~2 GB in 42 min**, all of it the metadata
  catalogs (~195 MB) + the bounded whole-part prefetch for the selected podcasts
  (~1.85 GB). Neither has anything to do with the guest scan.
- Determinism holds: rebuilt subset keeps **40 repeat guests spanning 75 podcasts**,
  the same selection as the committed `subsets/tutorial`.

## Measured comparison (same instrument, same seed/parts/guests)
| Phase | 1.1.1 range-read (prior) | 1.1.3 guest index (now) |
|---|---|---|
| Guest scan | ~30.1 GB / 105 min / 5864 range calls | **0 B / 0.1 s / 0 range calls** |
| Metadata catalogs | ~320 MB / ~16 s | ~195 MB / ~24 min* |
| Part prefetch (~404 podcasts) | ~1.67 GB / ~1 min | ~1.85 GB / ~18 min* |
| **Full rebuild total** | **~32 GB / 106 min** | **~2 GB / 42 min** |
| Guests found (determinism) | 24,019 → 40 kept / 75 podcasts | 24,019 → 40 kept / 75 podcasts |

\* The 42 min total is now entirely network-latency-bound on ~2 GB of *whole-file*
downloads through HF Xet (each file = redirect + xet-read-token round trip). It is not
the scan and does not scale with corpus size — it scales with `--parts`. The catalog
download being slow on this run (~24 min for ~195 MB) is Xet/network variance, not the
build logic. The scan — the whole reason the build was slow — contributes ~0.

## What made this the right fix (recap of the earlier regression)
The `6f08a2b` range-read approach **doubled** the scan download (~30 GB) rather than
reducing it: fsspec fetches through a 5 MB read-ahead block cache, and the two projected
columns are scattered across each ~110 MB part, so projecting 2 columns pulled ~220 MB/part
— more than the whole file. Precomputing the index into `metadata/` removes the episode-part
reads entirely, which is why it wins on both bytes and wall-clock. The range-read path is
kept only as a fallback for datasets that predate the index (`diarized_guest_index` checks
`backend._source.path("metadata/guest_index.parquet")` first).

## How it is wired now
- `scripts/build_tutorial_subset.py:diarized_guest_index` (:165): if
  `metadata/guest_index.parquet` exists, `return {name: set(podcasts)}` from
  `backend.diarized_guest_podcasts()`; else fall back to the 140-part range scan
  (logs a warning to update the dataset).
- `sporc/parquet_backend.py`: `_ensure_guest_index()` (:1249) loads
  `metadata/guest_index.parquet`; `diarized_guest_podcasts()` (:1318) returns the bulk
  `name_normalized -> {podcast_id}` mapping; `get_podcasts_by_guest()` /
  `search_by_guest()` are the per-lookup public API over the same files.
- `sporc/source.py`: `read_columns` / `_read_projected` (the range-read path) still exist
  and still work; they are now only the fallback.

## How to re-measure (do NOT use cache size for the scan)
Range reads bypass the on-disk cache, so `du` under-reports them. Count real HTTP bytes by
patching the fetch method — this is what produced every number above:
```python
import huggingface_hub.hf_file_system as hm
NET = {"bytes": 0, "calls": 0}
_orig = hm.HfFileSystemFile._fetch_range
def _p(self, start, end):
    NET["bytes"] += (end - start); NET["calls"] += 1
    return _orig(self, start, end)
hm.HfFileSystemFile._fetch_range = _p
```
Full instrumented driver: `scratchpad/measure_rebuild.py` (wraps `diarized_guest_index`,
attributes seconds/range_MB/cache_growth, rebuilds to a scratch dir). Reproduce:
```bash
rm -rf ~/.cache/huggingface/hub/datasets--blitt--SPoRC
python scripts/build_tutorial_subset.py    # defaults: --episodes 2000 --guests 40 --parts 3 --seed 20200525
```
Expected now: scan `range_calls=0`, `range_MB=0`, seconds < 1; cache growth ~2 GB from
catalogs + prefetch. If the scan ever shows nonzero range bytes again, the dataset lost its
`guest_index.parquet` and the build fell back to the part scan — reship the index.

## Acceptance (met)
- Scan download ≈ 0, scan wall-clock < 1 s. ✅ (0 B, 0.1 s)
- `subsets/tutorial_ids.txt` selection byte-for-byte equivalent for the same
  seed/parts/guests. ✅ (24,019 guests → 40 kept, 75 podcasts)

## Env notes
- `import sporc` → site-packages **1.1.3**; verify:
  `python3 -c "import sporc,os;print(sporc.__version__,os.path.dirname(sporc.__file__))"`.
  (Run the build from outside the repo root, or the local `sporc/` checkout can shadow
  site-packages.)
- macOS has no `timeout`/`gtimeout`.
- This measurement re-populated ~2 GB of SpoRC cache;
  `rm -rf ~/.cache/huggingface/hub/datasets--blitt--SPoRC` to reclaim it.
