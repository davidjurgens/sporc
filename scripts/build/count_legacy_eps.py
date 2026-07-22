import duckdb, glob, os

from buildconfig import load as _load_build_config

_CFG = _load_build_config()
parts = sorted(glob.glob(_CFG.parts("turns/text")))
con = duckdb.connect()
con.execute("SET memory_limit='100GB'"); con.execute("SET threads=16")
con.execute("SET temp_directory=_CFG.duckdb_tmp")
r = con.execute("SELECT count(DISTINCT episode_id) FROM read_parquet(?) "
                "WHERE token_count IS NULL", [parts]).fetchone()
print(f"episodes with any null token_count = {r[0]:,}")
