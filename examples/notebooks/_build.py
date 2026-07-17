"""
Build the tutorial notebooks from plain Python sources.

Notebook JSON is unreviewable in a diff, so each tutorial is written here as a
list of (kind, text) cells and rendered to .ipynb. Edit the source, re-run this,
commit both.

    python examples/notebooks/_build.py            # build all
    python examples/notebooks/_build.py 07         # build one
    python examples/notebooks/_build.py --check    # verify .ipynb match src/
"""

import importlib
import os
import sys

import nbformat as nbf

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)


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


def build(stem: str, title: str, cells) -> str:
    nb = render(stem, title, cells)
    path = os.path.join(HERE, f"{stem}.ipynb")
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


def main(argv):
    args = [a for a in argv[1:] if a != "--check"]
    check = "--check" in argv
    want = args or None
    stale = []
    for stem, mod_name in NOTEBOOKS.items():
        if want and not any(w in stem for w in want):
            continue
        mod = importlib.import_module(f"src.{mod_name}")
        importlib.reload(mod)
        if check:
            path = os.path.join(HERE, f"{stem}.ipynb")
            # nbf.write appends a trailing newline that nbf.writes omits, so
            # compare the serialized forms rather than raw file text.
            fresh = nbf.writes(render(stem, mod.TITLE, mod.CELLS))
            on_disk = (nbf.writes(nbf.read(path, as_version=4))
                       if os.path.exists(path) else "")
            status = "ok" if fresh == on_disk else "STALE"
            if status == "STALE":
                stale.append(stem)
            print(f"{status:5} {stem}.ipynb")
            continue
        path = build(stem, mod.TITLE, mod.CELLS)
        print(f"built {os.path.relpath(path, HERE)}  ({len(mod.CELLS)} cells)")
    if stale:
        print(f"\n{len(stale)} notebook(s) out of date with src/. Re-run without "
              f"--check, and commit the .ipynb alongside the source.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
