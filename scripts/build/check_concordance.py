import sys, time, logging
sys.path.insert(0, "/home/jurgens/projects/sporc")
logging.basicConfig(level=logging.WARNING)
from sporc import SPORCDataset

from buildconfig import load as _load_build_config

_CFG = _load_build_config()
ds = SPORCDataset(parquet_dir=_CFG.release, allow_downloads=False)
for kw, kwargs in [("climate change", {}), ("climate change", {"podcast_id": None})]:
    if kwargs.get("podcast_id") is None and kwargs:
        continue
    t = time.time()
    rows = ds.concordance(kw, limit=3, **kwargs)
    print(f"concordance({kw!r}) -> {len(rows)} rows in {time.time()-t:.1f}s", flush=True)
    for r in rows:
        print(f"   ...{r['left_context'][-40:]} [{r['keyword']}] {r['right_context'][:40]}...")
        print(f"      ep={r['episode_id']} role={r['speaker_role']} t={r['start_time']:.1f}")
