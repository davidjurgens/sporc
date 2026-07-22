"""
Rebuild metadata/turns_search.duckdb against the repacked turns.

``scripts/build_indexes.py`` loads this table by walking ``turns/podcast_id=*/``
and inserting one small Parquet file at a time through PyArrow. That layout is
gone, and the loop is unnecessary anyway: DuckDB reads the 127 turn parts
directly and in parallel, which is both simpler and much faster than 152,266
round trips through Python.

The three phases -- load, indexes, full-text index -- are checkpointed against
the database itself rather than a marker file, so a rerun after a failure picks
up at the first phase that did not finish. The full-text index is the expensive
one and cannot be split, so it is worth not repeating the load to get to it.
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
DB = os.path.join(OUT, "metadata", "turns_search.duckdb")

MEMORY_LIMIT = os.environ.get("SPORC_DUCKDB_MEMORY", "300GB")
THREADS = int(os.environ.get("SPORC_WORKERS", "16"))


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def has_table(con, name):
    return bool(con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
        [name]).fetchone())


def phase_load(con, parts):
    if has_table(con, "turns"):
        n = con.execute("SELECT COUNT(*) FROM turns").fetchone()[0]
        log(f"turns table already present with {n:,} rows, skipping load")
        return n

    log(f"loading {len(parts)} parts into turns...")
    t0 = time.time()
    # One statement over every part. row_number() supplies the FTS key column;
    # it is assigned over the whole set, so it stays unique across parts.
    con.execute(f"""
        CREATE TABLE turns AS
        SELECT
            CAST(row_number() OVER () AS INTEGER) AS row_id,
            CAST(episode_id AS VARCHAR) AS episode_id,
            CAST(podcast_id AS VARCHAR) AS podcast_id,
            CAST(turn_count AS INTEGER) AS turn_count,
            CAST(turn_text AS VARCHAR) AS turn_text,
            start_time,
            end_time,
            duration,
            CAST(inferred_speaker_role AS VARCHAR) AS speaker_role,
            CAST(inferred_speaker_name AS VARCHAR) AS speaker_name,
            CAST(length(CAST(turn_text AS VARCHAR))
                 - length(replace(CAST(turn_text AS VARCHAR), ' ', ''))
                 + 1 AS INTEGER) AS word_count
        FROM read_parquet({parts!r})
        WHERE turn_text IS NOT NULL AND CAST(turn_text AS VARCHAR) != ''
    """)
    n = con.execute("SELECT COUNT(*) FROM turns").fetchone()[0]
    log(f"loaded {n:,} rows in {(time.time()-t0)/60:.1f}m")
    return n


def phase_indexes(con):
    existing = {r[0] for r in con.execute(
        "SELECT index_name FROM duckdb_indexes()").fetchall()}
    for name, col in (("idx_episode", "episode_id"),
                      ("idx_podcast", "podcast_id"),
                      ("idx_role", "speaker_role")):
        if name in existing:
            log(f"{name} already present")
            continue
        t0 = time.time()
        con.execute(f"CREATE INDEX {name} ON turns({col})")
        log(f"{name} built in {(time.time()-t0)/60:.1f}m")


def phase_fts(con):
    # create_fts_index materialises its own schema; its presence is the
    # checkpoint. Rebuilding it from scratch is hours, so never do it twice.
    if has_table(con, "docs") and has_table(con, "terms"):
        log("full-text index already present, skipping")
        return
    con.execute("INSTALL fts")
    con.execute("LOAD fts")
    log("building full-text index (the long one)...")
    t0 = time.time()
    con.execute("PRAGMA create_fts_index('turns', 'row_id', 'turn_text', "
                "stemmer='english', stopwords='english')")
    log(f"full-text index built in {(time.time()-t0)/60:.1f}m")


def phase_slim(con):
    """
    Drop the turn text once the index is built.

    The database only needs the text to construct the inverted index. BM25
    scoring reads the index, not the source column, so dropping it afterwards
    leaves search returning identical row_ids and identical scores -- verified
    on a sample before this was applied. What remains is enough to identify a
    matching turn; the text itself is already in turns/text, and a client joins
    back on (episode_id, turn_count).

    This is most of the file. In v1.0 the turn copy was 9.67 GB against 2.54 GB
    of actual index.
    """
    cols = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'turns'").fetchall()}
    if "turn_text" not in cols:
        log("turn_text already dropped")
        return

    # DuckDB refuses to drop a column while an index depends on any column
    # positioned after it, and speaker_role sits after turn_text. Drop the
    # indexes, drop the column, put the indexes back.
    log("dropping indexes so turn_text can be removed...")
    for name in ("idx_episode", "idx_podcast", "idx_role"):
        con.execute(f"DROP INDEX IF EXISTS {name}")

    log("dropping turn_text...")
    con.execute("ALTER TABLE turns DROP COLUMN turn_text")

    phase_indexes(con)


def phase_compact(db_path):
    """
    Rewrite the database into a fresh file to reclaim the dropped space.

    DuckDB never returns blocks to the OS, so dropping a column frees nothing on
    disk by itself -- v1.0 shipped 24.2 GB of file for 12.22 GB of live data for
    exactly this reason. Copying database-to-database rebuilds the file from the
    live blocks only, and carries the FTS schema and its macros across intact.
    """
    tmp = db_path + ".compact"
    for suffix in ("", ".wal"):
        if os.path.exists(tmp + suffix):
            os.remove(tmp + suffix)

    before = os.path.getsize(db_path) / 2**30
    log(f"compacting {before:.1f} GB database...")
    t0 = time.time()
    con = duckdb.connect(db_path)
    con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
    con.execute(f"SET threads={THREADS}")
    con.execute("INSTALL fts")
    con.execute("LOAD fts")
    alias = os.path.splitext(os.path.basename(db_path))[0]
    con.execute(f"ATTACH '{tmp}' AS clean")
    con.execute(f"COPY FROM DATABASE {alias} TO clean")
    con.execute("DETACH clean")
    con.close()

    if os.path.exists(db_path + ".wal"):
        os.remove(db_path + ".wal")
    os.replace(tmp, db_path)
    after = os.path.getsize(db_path) / 2**30
    log(f"compacted {before:.1f} GB -> {after:.1f} GB "
        f"in {(time.time()-t0)/60:.1f}m")


def verify(db_path):
    """Search has to still work after all that, so prove it against the file."""
    con = duckdb.connect(db_path, read_only=True)
    con.execute("LOAD fts")
    n = con.execute("SELECT COUNT(*) FROM turns").fetchone()[0]
    cols = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'turns'").fetchall()]
    hits = con.execute(
        "SELECT row_id, episode_id, turn_count, "
        "fts_main_turns.match_bm25(row_id, 'climate change') AS s "
        "FROM turns WHERE s IS NOT NULL ORDER BY s DESC LIMIT 3").fetchall()
    con.close()
    log(f"verify: {n:,} rows, columns {cols}")
    log(f"verify: top hits for 'climate change' -> {hits}")
    if not hits:
        raise SystemExit("full-text search returned nothing after compaction")


def main():
    parts = sorted(glob.glob(f"{OUT}/turns/text/part-*.parquet"))
    if not parts:
        raise SystemExit("no turns/text parts; run stage.py first")

    os.makedirs(os.path.dirname(DB), exist_ok=True)
    con = duckdb.connect(DB)
    con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
    con.execute(f"SET threads={THREADS}")
    # Spill next to the database rather than into /tmp, which is far too small
    # for a sort over 185M rows.
    con.execute(f"SET temp_directory='{os.path.join(BUILD, 'duckdb_tmp')}'")
    log(f"memory_limit={MEMORY_LIMIT} threads={THREADS}")

    t0 = time.time()
    n = phase_load(con, parts)
    phase_indexes(con)
    phase_fts(con)
    phase_slim(con)
    final = con.execute("SELECT COUNT(*) FROM turns").fetchone()[0]
    con.close()

    phase_compact(DB)
    verify(DB)

    size = os.path.getsize(DB) / 2**30
    log(f"done: {final:,} rows, {size:.1f} GB, {(time.time()-t0)/60:.1f}m total")
    if final != n:
        raise SystemExit(f"row count changed during build: {n:,} -> {final:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
