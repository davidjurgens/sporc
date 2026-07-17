"""Cells for 01_corpus_cartography.ipynb."""

TITLE = "Mapping the corpus: what is actually in SPoRC"

CELLS = [
    ("md", """\
# 1. Mapping the corpus

This notebook covers four properties of the corpus that the later tutorials
build on: its size and composition, its date range, how many episodes carry
speaker turns, and how many turns carry a speaker name and role.

The last two set up the guards the other notebooks use. `has_turn_data`
distinguishes "no turn data here" from "nobody spoke", and role counts are a
lower bound rather than a partition.

> **Data.** These notebooks read a small pre-built subset (`subsets/tutorial`),
> not the 57 GB corpus. See `examples/notebooks/README.md` to build it.
"""),
    ("code", "PREAMBLE"),
    ("code", "DATA_CELL"),
    ("md", """\
## 1.1 Corpus scale

`get_dataset_statistics()` computes from whatever layout you point at. Pointed at
the subset it describes the subset — that is the point of a subset built with
`make_subset.py`, which filters the *catalogs* alongside the partitions so every
count stays true.

For reference, the full corpus is **228,099 podcasts / 1,124,058 episodes**.
"""),
    ("code", '''\
stats = sporc.get_dataset_statistics()

for k in ("total_podcasts", "total_episodes", "total_duration_hours",
          "avg_episode_duration_minutes"):
    v = stats.get(k)
    print(f"{k:32s} {v:,.1f}" if isinstance(v, float) else f"{k:32s} {v:,}")
'''),
    ("md", """\
## 1.2 Time span

SPoRC is a **two-month snapshot**, not a longitudinal archive. Every episode was
published between **1 May and 30 June 2020**.

So "trends over the years" is out. What the window does give you is a sharp,
dateable event sitting in the middle of it (25 May 2020), which turns a
before/after comparison into something closer to a natural experiment than a
correlation. Notebook 05 uses exactly that.
"""),
    ("code", '''\
import pandas as pd

dates = pd.Series([e.episode_date for e in sporc.iterate_episodes()])
dates = pd.to_datetime(dates.dropna())

print(f"episodes with a parsable date : {len(dates):,}")
print(f"earliest                      : {dates.min():%Y-%m-%d}")
print(f"latest                        : {dates.max():%Y-%m-%d}")
print(f"span                          : {(dates.max() - dates.min()).days} days")
'''),
    ("code", '''\
import matplotlib.pyplot as plt

by_day = dates.dt.floor("D").value_counts().sort_index()

fig, ax = plt.subplots(figsize=(9, 3.4))
ax.fill_between(by_day.index, by_day.values, color=PALETTE[0], alpha=0.18,
                linewidth=0)
ax.plot(by_day.index, by_day.values, color=PALETTE[0], linewidth=2)

# The event the window straddles. Annotated in ink, not by hue.
event = pd.Timestamp("2020-05-25")
if by_day.index.min() <= event <= by_day.index.max():
    ax.axvline(event, color=INK_MUTED, linewidth=1, linestyle=(0, (4, 3)))
    ax.annotate("25 May 2020", xy=(event, ax.get_ylim()[1]),
                xytext=(6, -10), textcoords="offset points",
                fontsize=9, color=INK_SECONDARY, va="top")

finish(ax, title="The corpus is a two-month window",
       subtitle="Episodes per day in the tutorial subset",
       ylabel="episodes")
plt.show()
'''),
    ("md", """\
## 1.3 Categories

Categories come from the Apple Podcasts taxonomy: up to ten per episode,
`category1`–`category10`, with `category1` the primary. `sporc.constants` carries
the hierarchy, so you can roll a subcategory up to its parent.

The palette stops at eight hues on purpose — a ninth would be indistinguishable
from an existing one for a colour-blind reader — so the tail folds into
**Other** rather than getting a generated colour.
"""),
    ("code", '''\
from collections import Counter
from _viz import fold_other

primary = Counter(e.primary_category for e in sporc.iterate_episodes()
                  if e.primary_category)
rows = fold_other(list(primary.items()), keep=7)
labels = [k for k, _ in rows][::-1]
values = [v for _, v in rows][::-1]
colors = [INK_MUTED if l == "Other" else PALETTE[0] for l in labels]

fig, ax = plt.subplots(figsize=(8, 4.2))
ax.barh(labels, values, color=colors, height=0.72)
ax.grid(False, axis="y")
for y, v in enumerate(values):
    ax.annotate(f"{v:,}", (v, y), xytext=(5, 0), textcoords="offset points",
                va="center", fontsize=9, color=INK_SECONDARY)
ax.set_xlim(0, max(values) * 1.12)

finish(ax, title="What the subset is about",
       subtitle="Episodes by primary category; the tail is folded into Other",
       xlabel="episodes")
plt.show()
'''),
    ("md", """\
## 1.4 Turn coverage

SPoRC diarized **372,604 of 1,124,058 episodes (33%)**. The rest have a
transcript but no speaker turns — upstream marked them `SPEAKER_DATA_UNAVAILABLE`.

So `len(episode.turns) == 0` is ambiguous: it means *either* "nobody spoke"
(never true) *or* "the corpus has no turn data here" (usually true).
`Episode.has_turn_data` is what tells them apart, and it is the guard every
turn-based analysis needs:

```python
if episode.has_turn_data:
    analyze(episode.turns)
```

The tutorial subset is **diarized-only** by default, so coverage here is 100% —
that is a property of the subset, not of SPoRC. Point the same code at the full
corpus and two thirds of episodes drop out.
"""),
    ("code", '''\
eps = list(sporc.iterate_episodes())
with_turns = sum(1 for e in eps if e.has_turn_data)

print(f"episodes in subset      : {len(eps):,}")
print(f"with turn data          : {with_turns:,} ({with_turns/len(eps):.0%})")
print(f"without                 : {len(eps)-with_turns:,}")
print()
print("In the FULL corpus: 372,604 / 1,124,058 = 33%.")
print("This subset is diarized-only, so it is not representative on this axis.")
'''),
    ("md", """\
## 1.5 Speaker names and roles

Diarization gives every turn a speaker *label* (`SPEAKER_00`), and a separate
inference step tries to attach a *name* and a *role*. That second step is sparse:
in a 174k-turn sample, **90.6% of turns are `NO_INFERRED_ROLE`** — only 7.4%
host, 1.9% guest.

The consequence is easy to miss: `get_host_turns()` does not return "the host's
turns", it returns "the turns we could confidently attribute to a host". Role
counts are a **lower bound**, never a partition of the conversation. A chart of
host-vs-guest talk time that ignores this is mostly measuring the inference
step's confidence.
"""),
    ("code", '''\
role_counts = Counter()
for e in eps:
    if not e.has_turn_data:
        continue
    for t in e.turns:
        role_counts[t.inferred_speaker_role or "NO_INFERRED_ROLE"] += 1

total = sum(role_counts.values())
rows = sorted(role_counts.items(), key=lambda kv: -kv[1])

fig, ax = plt.subplots(figsize=(8, 2.0))
left = 0
for i, (role, n) in enumerate(rows):
    color = INK_MUTED if role == "NO_INFERRED_ROLE" else PALETTE[i % 8]
    ax.barh([0], [n], left=left, color=color, height=0.5,
            edgecolor="#fcfcfb", linewidth=2)     # 2px surface gap between fills
    if n / total > 0.03:
        ax.annotate(f"{role}\\n{n/total:.1%}", (left + n / 2, 0),
                    ha="center", va="center", fontsize=9, color="white"
                    if role == "NO_INFERRED_ROLE" else INK)
    left += n

ax.set_yticks([]); ax.grid(False); ax.set_xlim(0, total)
for s in ("left", "bottom"):
    ax.spines[s].set_visible(False)
ax.set_xticks([])
finish(ax, title="Most turns have no inferred role",
       subtitle=f"{total:,} turns in the subset. Role-based counts are a lower bound.")
plt.show()

for role, n in rows:
    print(f"  {role:20s} {n:8,}  {n/total:6.1%}")
'''),
    ("md", """\
## 1.6 Episode duration

Duration is heavily right-skewed — a long tail of multi-hour episodes drags the
mean well above the median. Report the median.
"""),
    ("code", '''\
mins = pd.Series([e.duration_minutes for e in eps])
mins = mins[(mins > 0) & (mins < 240)]

fig, ax = plt.subplots(figsize=(8, 3.6))
ax.hist(mins, bins=60, color=PALETTE[0], alpha=0.85)
ax.axvline(mins.median(), color=INK, linewidth=1.5)
ax.annotate(f"median {mins.median():.0f} min", (mins.median(), 0),
            xytext=(8, 8), textcoords="offset points",
            fontsize=9, color=INK_SECONDARY,
            xycoords=("data", "axes fraction"))

finish(ax, title="Episode length is right-skewed",
       subtitle=f"n={len(mins):,}, capped at 240 min for display",
       xlabel="minutes", ylabel="episodes")
plt.show()

print(mins.describe().round(1).to_string())
'''),
    ("md", """\
## 1.7 Summary

1. **Two months, May–June 2020.** No long-run trends. But a dateable event sits
   in the middle of the window.
2. **33% of episodes have turns** in the full corpus. Gate on
   `episode.has_turn_data`, never on `len(turns) > 0`.
3. **~90% of turns have no role.** Role-conditioned numbers are lower bounds.

Next: **02 · Named entities and co-mention networks**, which builds on the
`has_turn_data` guard from here.
"""),
]
