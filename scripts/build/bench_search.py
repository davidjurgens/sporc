"""
Compare search query shapes against the built databases.

The client's inherited query scores every turn, joins the text on, and only then
takes the top 20. Ranking before joining should turn the join into twenty point
lookups instead of one over 185M rows -- and if that is what the join costs, the
13.5 GB index on (episode_id, turn_count) is not buying anything.
"""

import sys
import time

import duckdb

from buildconfig import load as _load_build_config

_CFG = _load_build_config()

BASE = _CFG.metadata()
QUERY = "climate change"

JOIN_THEN_RANK = """
WITH scored AS (
    SELECT row_id, episode_id, turn_count,
           fts_main_turns.match_bm25(row_id, ?) AS score
    FROM turns WHERE score IS NOT NULL
)
SELECT s.episode_id, s.turn_count, t.turn_text, s.score
FROM scored s JOIN txt.turn_text t USING (episode_id, turn_count)
ORDER BY s.score DESC LIMIT 20
"""

RANK_THEN_JOIN = """
WITH top AS (
    SELECT episode_id, turn_count, score FROM (
        SELECT episode_id, turn_count,
               fts_main_turns.match_bm25(row_id, ?) AS score
        FROM turns
    ) WHERE score IS NOT NULL
    ORDER BY score DESC LIMIT 20
)
SELECT p.episode_id, p.turn_count, t.turn_text, p.score
FROM top p JOIN txt.turn_text t USING (episode_id, turn_count)
ORDER BY p.score DESC
"""

SCORE_ONLY = """
SELECT episode_id, turn_count, score FROM (
    SELECT episode_id, turn_count,
           fts_main_turns.match_bm25(row_id, ?) AS score
    FROM turns
) WHERE score IS NOT NULL
ORDER BY score DESC LIMIT 20
"""


def main():
    con = duckdb.connect(f"{BASE}/turns_search.duckdb", read_only=True)
    con.execute("LOAD fts")
    con.execute("SET threads=8")
    con.execute(f"ATTACH '{BASE}/turns_text.duckdb' AS txt (READ_ONLY)")

    for name, sql in (("score only (no text)", SCORE_ONLY),
                      ("rank then join", RANK_THEN_JOIN),
                      ("join then rank (current)", JOIN_THEN_RANK)):
        times = []
        for _ in range(2):
            t0 = time.time()
            rows = con.execute(sql, [QUERY]).fetchall()
            times.append(time.time() - t0)
        print(f"{name:26s} {times[0]:7.1f}s then {times[1]:6.1f}s  "
              f"({len(rows)} hits)", flush=True)
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
