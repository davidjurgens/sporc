"""Is the 13.5 GB join index earning its size?"""
import duckdb, os, time

from buildconfig import load as _load_build_config

_CFG = _load_build_config()
BASE = _CFG.metadata()
Q = """
WITH top AS (
  SELECT episode_id, turn_count, score FROM (
    SELECT episode_id, turn_count, fts_main_turns.match_bm25(row_id, 'climate change') AS score
    FROM turns) WHERE score IS NOT NULL ORDER BY score DESC LIMIT 20)
SELECT p.episode_id, p.turn_count, t.turn_text FROM top p
JOIN txt.turn_text t ON t.episode_id = p.episode_id AND t.turn_count = p.turn_count
"""
def run(label):
    con = duckdb.connect(f"{BASE}/turns_search.duckdb", read_only=True)
    con.execute("LOAD fts"); con.execute("SET threads=8")
    con.execute(f"ATTACH '{BASE}/turns_text.duckdb' AS txt (READ_ONLY)")
    ts = []
    for _ in range(2):
        t0 = time.time(); r = con.execute(Q).fetchall(); ts.append(time.time()-t0)
    con.close()
    size = os.path.getsize(f"{BASE}/turns_text.duckdb")/2**30
    print(f"{label:16s} {ts[0]:6.1f}s then {ts[1]:6.1f}s   db {size:.1f} GB  ({len(r)} hits)", flush=True)

run("with index")
c = duckdb.connect(f"{BASE}/turns_text.duckdb")
c.execute("DROP INDEX IF EXISTS idx_text_key"); c.close()
print("index dropped (file does not shrink until compaction)", flush=True)
run("without index")
