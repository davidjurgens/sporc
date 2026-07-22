"""Reclaim the space the dropped join index left behind."""
import duckdb, os, time

from buildconfig import load as _load_build_config

_CFG = _load_build_config()
DB = _CFG.metadata("turns_text.duckdb")
tmp = DB + ".compact"
for s in ("", ".wal"):
    if os.path.exists(tmp + s):
        os.remove(tmp + s)
before = os.path.getsize(DB) / 2**30
print(f"compacting {before:.1f} GB...", flush=True)
t0 = time.time()
con = duckdb.connect(DB)
con.execute(f"SET memory_limit='{_CFG.duckdb_memory}'")
con.execute(f"SET threads={_CFG.duckdb_threads}")
con.execute(f"SET temp_directory='{_CFG.duckdb_tmp}'")
con.execute(f"ATTACH '{tmp}' AS clean")
con.execute("COPY FROM DATABASE turns_text TO clean")
con.execute("DETACH clean"); con.close()
if os.path.exists(DB + ".wal"):
    os.remove(DB + ".wal")
os.replace(tmp, DB)
after = os.path.getsize(DB) / 2**30
print(f"compacted {before:.1f} GB -> {after:.1f} GB in {(time.time()-t0)/60:.1f}m", flush=True)
