# Dataset build scripts

The pipeline that repacked the SPoRC corpus from the 1.0 hive layout into the
1.1 packed layout. These are the scripts that produced the released artifacts;
the client-side tooling for the older layout is in `scripts/` one level up.

They are not a library and not a single command. Each stage is a separate
script, run once, in the order below, with its output checked before the next
one starts. Several take hours.

## Configuration

Every path comes from a JSON config. Nothing here derives a path from its own
location, and nothing hardcodes `/shared/...`.

```bash
python3 scripts/build/write_manifest.py --config /shared/6/projects/sporc/build/sporc1.1.json
export SPORC_BUILD_CONFIG=/shared/6/projects/sporc/build/sporc1.1.json   # or this
```

Resolution order: `--config`, then `$SPORC_BUILD_CONFIG`, then
`buildconfig.DEFAULT_CONFIG`.

```json
{
  "name": "sporc-1.1",
  "version": "1.1",
  "paths": {
    "source":  "/shared/6/projects/sporc/v1",
    "release": "/shared/6/projects/sporc/v1.1",
    "build":   "/shared/6/projects/sporc/build"
  },
  "tuning": {
    "workers": 48,
    "duckdb_memory": "300GB",
    "duckdb_threads": 16
  }
}
```

| key | meaning |
|---|---|
| `paths.source` | the corpus being read (1.0) |
| `paths.release` | the tree being written, uploaded to Hugging Face **verbatim** |
| `paths.build` | intermediates, logs, DuckDB spill — never published |
| `tuning.workers` | process count for the parallel stages |
| `tuning.duckdb_memory` | machine-specific; DuckDB will use it all |

`buildconfig` refuses a config whose `build` is inside `release`. That was the
original arrangement — the scratch directory sat in the release tree as
`_build/` — and it meant 61 GB of intermediates were one careless `upload_folder`
away from shipping. The check exists so that cannot be reintroduced.

Use `cfg.scratch(...)` for anything written during a build, `cfg.rel()` /
`cfg.metadata()` / `cfg.parts()` for the release, and `cfg.src()` for the
source corpus.

## Order

| # | script | writes |
|---|---|---|
| 1 | `build_worklist.py` | `build/worklist.parquet` — which episodes can be rebuilt and where their inputs are |
| 2 | `build_turns.py` | `build/turns_raw/` — speaker turns, recomputed with the corrected matcher |
| 3 | `stage.py` | `build/bands/` — the repack, split into restartable stages |
| 4 | `repack.py` | `release/{episodes,turns,acoustics}/` — the packed part files |
| 5 | `build_metadata.py` | `release/metadata/*.parquet` — catalogs and indexes |
| 6 | `stage_metrics.py` | `release/turns/metrics/`, `release/metadata/episode_metrics.parquet` |
| 7 | `stage_search.py` | `release/metadata/turns_search.duckdb` (~104 min) |
| 8 | `stage_search_text.py` | `release/metadata/turns_text.duckdb` (~6 min) |
| 9 | `write_manifest.py` | `release/manifest.json`, measured from disk |

`rebuild_search_all.sh` runs 7 then 8, which is the pair you want after any
change to the turn text.

### Corrections applied after the first pass

Run against an already-built release, not part of a clean build:

- `rename_acoustics.py` — acoustic columns back onto 1.0's naming convention
- `rename_word_count.py` — `turns/text.word_count` → `token_count`, because it
  counts aligner tokens (punctuation included) and never counted words
- `dedupe_turns.py` — removed 85,541 turns that 1.0 stored twice, preserving
  row-group count and order, then corrected the shard map
- `compact_text.py` — reclaims space DuckDB does not return to the OS

Anything that rewrites a turn tree must preserve the row-group count and order.
Row-group position is how the shard map addresses a podcast; a rewrite that
merges or reorders groups silently corrupts every lookup.

## Verification

```bash
setsid nohup bash scripts/build/run_verification.sh > "$BUILD/verification.log" 2>&1 &
```

Test suite, then `scripts/audit_api.py` against the tutorial subset and the
full corpus, then it deletes the caches the client leaves inside the release
tree. `setsid` detaches it so it survives the session ending. Last recorded
run: 463 tests, 128/128 on the subset, 133/133 on the full corpus.

The cache deletion is not optional housekeeping. The client writes
`_index_cache.pkl` and two `.arrow` files into whatever metadata directory it
reads, so merely auditing the release puts 330 MB of derived files inside it.
`write_manifest.py` refuses to list `_`-prefixed files for the same reason.

## Ad-hoc checks

`count_empty_text.py`, `count_null_tokens.py`, `count_legacy_eps.py`,
`check_concordance.py`, `test_index_value.py`, `bench_search.py`. Small
one-question scripts kept because they are how the numbers in the dataset
README were established, and rerunning them is how to confirm those numbers
still hold.

## Retired

`rename_search_column.py.obsolete` and `repair_search_fts.py.obsolete` — see
`OBSOLETE.md`. They encode a mistake and must not run against the current
database.
