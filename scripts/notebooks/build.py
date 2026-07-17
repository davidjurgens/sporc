"""
Build the tutorial notebooks from plain Python sources.

Notebook JSON is unreviewable in a diff, so each tutorial is written here as a
list of (kind, text) cells and rendered to .ipynb. Edit the source, re-run this,
commit both. This build machinery lives outside examples/ so that directory holds
only what a user runs; the sources it reads are under scripts/notebooks/src, and
the .ipynb it writes land in examples/notebooks alongside _viz.py.

    python scripts/notebooks/build.py             # build all
    python scripts/notebooks/build.py 07          # build one
    python scripts/notebooks/build.py --check     # verify .ipynb match src/
    python scripts/notebooks/build.py --execute   # build, then run so the
                                                  # .ipynb ship with outputs

The committed .ipynb are executed: they carry their outputs and figures so the
notebooks read on GitHub without anyone building the subset first. A plain build
keeps the outputs of cells whose source did not change, so rebuilding after a
prose edit does not throw away figures that took minutes to produce. Only
--execute re-runs, and it needs `subsets/tutorial` on disk.
"""

import importlib
import os
import sys

import nbformat as nbf

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

# Sources live here (scripts/notebooks); the rendered .ipynb and their _viz.py
# runtime helper live in examples/notebooks. Notebooks are written to and
# executed from NB_DIR so their relative paths ("../.." to the repo root,
# _viz.py, ../../subsets/tutorial) resolve exactly as when a user runs them.
NB_DIR = os.path.abspath(os.path.join(HERE, "..", "..", "examples", "notebooks"))

# 07 fetches and aligns real audio for ~12 minutes; 06 waits on MALLET.
EXECUTE_TIMEOUT = 2400


def render(stem: str, title: str, cells) -> nbf.NotebookNode:
    nb = nbf.v4.new_notebook()
    resolved = []
    for kind, text in cells:
        # Sources refer to the shared opening cells by name rather than pasting
        # them, so a fix to the import guard lands in every notebook at once.
        if kind == "code" and text in _SHARED:
            text = _SHARED[text]
        resolved.append((kind, text))
    nb.cells = [
        nbf.v4.new_markdown_cell(c) if kind == "md" else nbf.v4.new_code_cell(c)
        for kind, c in resolved
    ]
    # nbformat mints a random id per cell, so every rebuild rewrites every id and
    # buries the real change in noise -- which is how a stale 03 shipped with its
    # source's guest-artifact section missing. Deterministic ids keep a rebuild
    # that changed nothing a no-op in git.
    for i, cell in enumerate(nb.cells):
        cell["id"] = f"{stem}-{i:02d}"
    nb.metadata = {
        "kernelspec": {"display_name": "Python 3", "language": "python",
                       "name": "python3"},
        "language_info": {"name": "python"},
        "title": title,
    }
    return nb


def _sources(nb) -> list:
    """The cell sources, which is what src/ owns. Outputs come from executing."""
    return [(c.cell_type, c.source) for c in nb.cells]


def carry_outputs(fresh, path: str):
    """
    Copy outputs from the notebook on disk onto a fresh render.

    The .ipynb ship executed, so a rebuild must not silently throw away figures
    that cost minutes to produce. Outputs are only reused where the cell's source
    is unchanged; edit a cell and its stale output is dropped rather than left
    sitting under new code.
    """
    if not os.path.exists(path):
        return fresh
    old = nbf.read(path, as_version=4)
    by_source = {}
    for c in old.cells:
        if c.cell_type == "code" and c.get("outputs"):
            by_source[c.source] = (c["outputs"], c.get("execution_count"))
    for c in fresh.cells:
        if c.cell_type != "code":
            continue
        got = by_source.get(c.source)
        if got:
            c["outputs"], c["execution_count"] = got
    return fresh


def build(stem: str, title: str, cells) -> str:
    nb = carry_outputs(render(stem, title, cells),
                       os.path.join(NB_DIR, f"{stem}.ipynb"))
    path = os.path.join(NB_DIR, f"{stem}.ipynb")
    with open(path, "w") as f:
        nbf.write(nb, f)
    return path


# Every notebook opens with this. The import guard is not boilerplate: an old
# sporc on PyPI (0.2.x) shadows a source checkout unless it is installed, and its
# API is different enough that the failure is confusing rather than obvious.
PREAMBLE = '''\
import sys, os
sys.path.insert(0, os.path.abspath("../.."))     # prefer the source tree
sys.path.insert(0, os.path.dirname(os.path.abspath("_viz.py")))

import sporc
if sporc.__version__ < "1.0":
    raise RuntimeError(
        f"This notebook needs sporc >= 1.0 but imported {sporc.__version__} from "
        f"{os.path.dirname(sporc.__file__)}.\\n"
        "PyPI's latest release is 0.2.0, which has a different API, so "
        "`pip install sporc` gets you the wrong one. Install this checkout:\\n"
        "    pip install -e /path/to/sporc"
    )

from _viz import apply_style, finish, PALETTE, INK, INK_SECONDARY, INK_MUTED
apply_style()
print("sporc", sporc.__version__, "from", os.path.dirname(sporc.__file__))
'''

DATA_CELL = '''\
from sporc import SPORCDataset

# Built by scripts/make_subset.py; see examples/notebooks/README.md.
SUBSET = os.path.abspath("../../subsets/tutorial")
sporc = SPORCDataset(parquet_dir=SUBSET)
print(sporc)
'''

_SHARED = {"PREAMBLE": PREAMBLE, "DATA_CELL": DATA_CELL}

#: stem -> source module under src/
NOTEBOOKS = {
    "01_corpus_cartography": "nb01_cartography",
    "02_ner_comention_networks": "nb02_ner_comention",
    "03_host_guest_networks": "nb03_host_guest",
    "04_repeat_guest_language": "nb04_repeat_guests",
    "05_stance_over_time": "nb05_stance_over_time",
    "06_topic_modeling_mallet": "nb06_topic_modeling",
    "07_sociophonetics_caught_cot": "nb07_sociophonetics",
    "08_conversational_dynamics": "nb08_conversational_dynamics",
}


def execute(stem: str) -> bool:
    """Run a notebook in place so it ships with its outputs and figures."""
    from nbclient import NotebookClient
    from nbclient.exceptions import CellExecutionError

    path = os.path.join(NB_DIR, f"{stem}.ipynb")
    nb = nbf.read(path, as_version=4)
    client = NotebookClient(
        nb, timeout=EXECUTE_TIMEOUT, kernel_name="python3",
        resources={"metadata": {"path": NB_DIR}},
    )
    try:
        client.execute()
        ok = True
    except CellExecutionError as e:
        print(f"  FAILED: {str(e)[-300:]}")
        ok = False
    # Written either way: a failed run's traceback is more use on disk than a
    # notebook that looks like it was never run.
    nbf.write(nb, path)
    return ok


def main(argv):
    flags = {a for a in argv[1:] if a.startswith("--")}
    want = [a for a in argv[1:] if not a.startswith("--")] or None
    check = "--check" in flags
    run = "--execute" in flags
    stale, failed = [], []
    for stem, mod_name in NOTEBOOKS.items():
        if want and not any(w in stem for w in want):
            continue
        mod = importlib.import_module(f"src.{mod_name}")
        importlib.reload(mod)
        path = os.path.join(NB_DIR, f"{stem}.ipynb")
        if check:
            # Compare sources, not whole files: the shipped .ipynb carry outputs
            # that src/ knows nothing about, so a serialized compare would call
            # every executed notebook stale.
            fresh = _sources(render(stem, mod.TITLE, mod.CELLS))
            on_disk = _sources(nbf.read(path, as_version=4)) if os.path.exists(path) else []
            status = "ok" if fresh == on_disk else "STALE"
            if status == "STALE":
                stale.append(stem)
            n_out = 0
            if os.path.exists(path):
                n_out = sum(1 for c in nbf.read(path, as_version=4).cells
                            if c.cell_type == "code" and c.get("outputs"))
            print(f"{status:5} {stem}.ipynb  ({n_out} cells with output)")
            continue
        build(stem, mod.TITLE, mod.CELLS)
        print(f"built {stem}.ipynb  ({len(mod.CELLS)} cells)", flush=True)
        if run:
            print(f"  executing...", flush=True)
            if not execute(stem):
                failed.append(stem)
            else:
                print("  ok", flush=True)
    if stale:
        print(f"\n{len(stale)} notebook(s) out of date with src/. Re-run without "
              f"--check, and commit the .ipynb alongside the source.")
        return 1
    if failed:
        print(f"\n{len(failed)} notebook(s) failed to execute: {', '.join(failed)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
