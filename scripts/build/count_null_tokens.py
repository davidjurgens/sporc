import duckdb, glob, os, time

from buildconfig import load as _load_build_config

_CFG = _load_build_config()
BUILD = _CFG.build
parts = sorted(glob.glob(_CFG.parts("turns/text")))
con = duckdb.connect()
con.execute("SET memory_limit='100GB'"); con.execute("SET threads=16")
con.execute(f"SET temp_directory='{os.path.join(BUILD,'duckdb_tmp')}'")
t = time.time()
r = con.execute(
    "SELECT count(*) total, "
    "count(*) FILTER (WHERE token_count IS NULL) null_tok, "
    "count(*) FILTER (WHERE speakers_recomputed = false) not_recomp, "
    "count(*) FILTER (WHERE token_count IS NULL AND speakers_recomputed = false) both_n "
    "FROM read_parquet(?)", [parts]).fetchone()
tot, nul, nr, both = r
print(f"total={tot:,}")
print(f"token_count NULL = {nul:,} ({100*nul/tot:.1f}%)")
print(f"speakers_recomputed=false = {nr:,}")
print(f"both = {both:,}  (identical sets: {nul==nr==both})")
print(f"{time.time()-t:.0f}s")
