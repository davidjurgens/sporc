import duckdb, glob, os, time

from buildconfig import load as _load_build_config

_CFG = _load_build_config()
BUILD = _CFG.build
parts = sorted(glob.glob(_CFG.parts("turns/text")))
con = duckdb.connect()
con.execute("SET memory_limit='100GB'"); con.execute("SET threads=16")
con.execute(f"SET temp_directory='{os.path.join(BUILD,'duckdb_tmp')}'")
t = time.time()
row = con.execute(
    "SELECT count(*) AS total, "
    "count(*) FILTER (WHERE turn_text IS NULL OR CAST(turn_text AS VARCHAR) = '') AS empty "
    "FROM read_parquet(?)", [parts]).fetchone()
print(f"parts={len(parts)} total={row[0]:,} empty={row[1]:,} nonempty={row[0]-row[1]:,} in {time.time()-t:.0f}s")
