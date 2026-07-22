"""
Build metadata/turns_text.duckdb: the turn text that turns_search.duckdb drops.

The search database holds the inverted index and enough columns to identify a
matching turn, but not the text. Dropping the text is what takes it to 13.7 GB,
and most of what people download it for is the index.

The text costs far more than the index does: 30.8 GB here against 11.9 GB for
the same strings in turns/text Parquet. DuckDB's string compression is simply
not Parquet's zstd. Anyone who cares more about bytes than about SQL should
skip this file and read the Parquet.

Search still has to be able to show what it matched, and two of the three search
modes (exact and regex substring matching) have no index to run against at all.
Both need the text present in SQL. Putting it in its own file means the choice
is the user's: take the index alone and join back to the Parquet, or take this
as well and have search work entirely inside DuckDB.

Keyed on (episode_id, turn_count) rather than the search database's row_id.
row_id was assigned by row_number() over a parallel Parquet scan, and nothing
guarantees a second scan numbers the rows the same way; the pair is stable
because turn_count is the turn's index within its episode.
"""

import glob
import os
import sys
import time

import duckdb

from buildconfig import load as _load_build_config

_CFG = _load_build_config()

BUILD = _CFG.build
OUT = _CFG.release
DB = os.path.join(OUT, "metadata", "turns_text.duckdb")
SEARCH_DB = os.path.join(OUT, "metadata", "turns_search.duckdb")

MEMORY_LIMIT = os.environ.get("SPORC_DUCKDB_MEMORY", "300GB")
THREADS = int(os.environ.get("SPORC_WORKERS", "16"))


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def has_table(con, name):
    return bool(con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
        [name]).fetchone())


def verify():
    """Prove the two databases join, rather than assuming they do."""
    con = duckdb.connect(SEARCH_DB, read_only=True)
    con.execute("LOAD fts")
    con.execute(f"ATTACH '{DB}' AS txt (READ_ONLY)")
    rows = con.execute("""
        WITH scored AS (
            SELECT row_id, episode_id, turn_count,
                   fts_main_turns.match_bm25(row_id, 'climate change') AS score
            FROM turns WHERE score IS NOT NULL
        )
        SELECT s.episode_id, s.turn_count, t.turn_text, s.score
        FROM scored s
        JOIN txt.turn_text t USING (episode_id, turn_count)
        ORDER BY s.score DESC LIMIT 3
    """).fetchall()
    con.close()
    if not rows or not all(r[2] for r in rows):
        raise SystemExit("join returned no text; the two databases disagree")
    for eid, tc, text, score in rows:
        log(f"verify: {eid} turn {tc} score {score:.2f} -> {text[:70]!r}")


def main():
    parts = sorted(glob.glob(f"{OUT}/turns/text/part-*.parquet"))
    if not parts:
        raise SystemExit("no turns/text parts; run stage.py first")
    if not os.path.exists(SEARCH_DB):
        raise SystemExit(f"{SEARCH_DB} not found; run stage_search.py first")

    con = duckdb.connect(DB)
    con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
    con.execute(f"SET threads={THREADS}")
    con.execute(f"SET temp_directory='{os.path.join(BUILD, 'duckdb_tmp')}'")
    log(f"memory_limit={MEMORY_LIMIT} threads={THREADS}")

    t0 = time.time()
    if has_table(con, "turn_text"):
        n = con.execute("SELECT COUNT(*) FROM turn_text").fetchone()[0]
        log(f"turn_text already present with {n:,} rows")
    else:
        log(f"loading text from {len(parts)} parts...")
        # Same WHERE clause as the search build, so the two contain the same
        # set of turns and every hit can be resolved.
        con.execute(f"""
            CREATE TABLE turn_text AS
            SELECT CAST(episode_id AS VARCHAR) AS episode_id,
                   CAST(turn_count AS INTEGER) AS turn_count,
                   CAST(turn_text AS VARCHAR) AS turn_text
            FROM read_parquet({parts!r})
            WHERE turn_text IS NOT NULL AND CAST(turn_text AS VARCHAR) != ''
        """)
        n = con.execute("SELECT COUNT(*) FROM turn_text").fetchone()[0]
        log(f"loaded {n:,} rows in {(time.time()-t0)/60:.1f}m")

    # No index on (episode_id, turn_count). It looks like the obvious thing to
    # build, but DuckDB hash-joins this regardless of whether it exists: the
    # same query measured 16.1s with the index and 14.9s without. Dropping it
    # also took the file from 32.6 GB to 30.8 GB. A slight slowdown and 1.8 GB
    # is not much of a case for keeping it.
    con.close()

    verify()
    size = os.path.getsize(DB) / 2**30
    log(f"done: {size:.1f} GB, {(time.time()-t0)/60:.1f}m total")
    return 0


if __name__ == "__main__":
    sys.exit(main())
