"""
Shared plotting style for the SPoRC tutorial notebooks.

Import this first in every notebook so the figures read as one set::

    from _viz import PALETTE, apply_style, finish, fold_other
    apply_style()

Why a module rather than a style cell per notebook: the palette is not a taste
choice. The eight categorical hues below are a validated set -- fixed *order*,
checked for colour-vision separation against the chart surface -- and the order
is the safety mechanism, not decoration. Cycling past slot 8, or re-ordering,
breaks it. Fold a long tail into "Other" with :func:`fold_other` instead.

Rules this module encodes, so the notebooks do not each re-lifigate them:

* Categorical hues are assigned in fixed order and never cycled.
* Colour follows the entity, never its rank, so filtering never repaints.
* Sequential magnitude is one hue, light to dark. Diverging is two opposed hues
  with a neutral grey midpoint -- never a rainbow.
* Slots 3, 4 and 5 sit below 3:1 contrast on the light surface, so anything
  using them carries a visible label; :func:`direct_label` is the shortcut.
* Scatter/bubble compares every pair at once rather than neighbours, and only
  the first four slots clear that bar -- hence :data:`SCATTER_MAX`.
* Text wears ink colours, never the series colour.
"""

from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import matplotlib as mpl
import matplotlib.pyplot as plt

# --- Surfaces and ink -------------------------------------------------------

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#8a8880"
GRID = "#e6e5e1"
NEUTRAL = "#f0efec"          # diverging midpoint / "Other"

# --- Categorical ------------------------------------------------------------

#: The eight categorical hues, in the order that maximises colour-vision
#: separation between neighbours. Do not re-order and do not extend.
PALETTE: List[str] = [
    "#2a78d6",   # 1 blue
    "#008300",   # 2 green
    "#e87ba4",   # 3 magenta   (low contrast -> label it)
    "#eda100",   # 4 yellow    (low contrast -> label it)
    "#1baf7a",   # 5 aqua      (low contrast -> label it)
    "#eb6834",   # 6 orange
    "#4a3aa7",   # 7 violet
    "#e34948",   # 8 red
]

#: Slots needing a visible label rather than colour alone (sub-3:1 on light).
LOW_CONTRAST_SLOTS = frozenset({2, 3, 4})

#: Max series for forms where every pair is compared at once (scatter, bubble).
#: Past four, facet or fold to "Other".
SCATTER_MAX = 4

#: Max series for neighbour-compared forms (bars, lines, stacks).
CATEGORICAL_MAX = 8

# --- Sequential (one hue, light -> dark) ------------------------------------

SEQUENTIAL: List[str] = [
    "#cde2fb", "#b7d3f6", "#9ec5f4", "#86b6ef", "#6da7ec",
    "#5598e7", "#3987e5", "#2a78d6", "#256abf", "#1c5cab",
    "#184f95", "#104281", "#0d366b",
]

#: An ordinal ramp must stay readable against the surface: start no lighter
#: than step 250.
ORDINAL = SEQUENTIAL[3:]

# --- Diverging (opposed poles, neutral middle) ------------------------------

DIVERGING_LOW = "#2a78d6"     # blue
DIVERGING_MID = NEUTRAL       # grey: must read as "nothing"
DIVERGING_HIGH = "#e34948"    # red

# --- Status (reserved; never a series colour) -------------------------------

STATUS = {
    "good": "#0ca30c",
    "warning": "#fab219",
    "serious": "#ec835a",
    "critical": "#d03b3b",
}


def sequential_cmap(name: str = "sporc_seq"):
    """A one-hue light->dark colormap for continuous magnitude."""
    from matplotlib.colors import LinearSegmentedColormap
    return LinearSegmentedColormap.from_list(name, SEQUENTIAL)


def diverging_cmap(name: str = "sporc_div"):
    """A two-pole colormap with a neutral grey midpoint, for signed values."""
    from matplotlib.colors import LinearSegmentedColormap
    return LinearSegmentedColormap.from_list(
        name, [DIVERGING_LOW, DIVERGING_MID, DIVERGING_HIGH])


def apply_style() -> None:
    """Install the house style. Call once, at the top of a notebook."""
    mpl.rcParams.update({
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
        "savefig.facecolor": SURFACE,
        "figure.dpi": 120,
        "savefig.dpi": 150,
        "figure.figsize": (8.0, 4.8),

        "font.family": "sans-serif",
        "font.sans-serif": ["DejaVu Sans"],
        "font.size": 10,
        "text.color": INK,

        # Chrome recedes; the data does not.
        "axes.edgecolor": GRID,
        "axes.linewidth": 0.8,
        "axes.grid": True,
        "axes.grid.axis": "y",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.labelcolor": INK_SECONDARY,
        "axes.labelsize": 10,
        "axes.titlesize": 12.5,
        "axes.titleweight": "medium",
        "axes.titlecolor": INK,
        "axes.titlelocation": "left",
        "axes.titlepad": 12,
        "axes.prop_cycle": mpl.cycler(color=PALETTE),

        "grid.color": GRID,
        "grid.linewidth": 0.8,
        "grid.alpha": 1.0,

        "xtick.color": INK_MUTED,
        "ytick.color": INK_MUTED,
        "xtick.labelcolor": INK_SECONDARY,
        "ytick.labelcolor": INK_SECONDARY,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "xtick.direction": "out",
        "ytick.direction": "out",
        "xtick.major.size": 0,
        "ytick.major.size": 0,

        "lines.linewidth": 2.0,
        "lines.markersize": 8,
        "lines.solid_capstyle": "round",

        "legend.frameon": False,
        "legend.fontsize": 9,
        "legend.labelcolor": INK_SECONDARY,

        "figure.autolayout": False,
    })


def color_for(entity: str, order: Sequence[str]) -> str:
    """
    The colour for *entity*, fixed by its position in *order*.

    Keying on a stable order rather than on the current row number is what stops
    a filtered chart from repainting the survivors: a reader who learned "Acme is
    blue" keeps that.
    """
    try:
        i = list(order).index(entity)
    except ValueError:
        return INK_MUTED
    if i >= CATEGORICAL_MAX:
        return INK_MUTED          # tail -> "Other" grey; never a 9th hue
    return PALETTE[i]


def fold_other(items: Sequence[Tuple[str, float]], keep: int = CATEGORICAL_MAX - 1,
               other_label: str = "Other") -> List[Tuple[str, float]]:
    """
    Keep the *keep* largest items and sum the rest into one "Other" row.

    The palette stops at eight for a reason; a ninth generated hue is
    indistinguishable from an existing one under colour-vision deficiency. Fold
    the tail instead.
    """
    ranked = sorted(items, key=lambda kv: kv[1], reverse=True)
    head, tail = ranked[:keep], ranked[keep:]
    if tail:
        head.append((other_label, sum(v for _, v in tail)))
    return head


def direct_label(ax, x, y, text: str, color: str = INK_SECONDARY, **kw):
    """
    Label a mark in ink, not in the series colour.

    Required for slots 3-5, which sit below 3:1 on the light surface: they must
    never carry meaning by colour alone.
    """
    kw.setdefault("fontsize", 9)
    kw.setdefault("va", "center")
    return ax.annotate(text, (x, y), color=color, **kw)


def finish(ax, title: Optional[str] = None, subtitle: Optional[str] = None,
           xlabel: Optional[str] = None, ylabel: Optional[str] = None,
           source: Optional[str] = None, legend: bool = False):
    """
    Apply the title block and tidy an axes.

    *subtitle* is where the caveat goes -- sample size, coverage, what the chart
    does not show. Most SPoRC figures need one.
    """
    if title:
        ax.set_title(title, pad=18 if subtitle else 12)
    if subtitle:
        ax.text(0.0, 1.02, subtitle, transform=ax.transAxes, fontsize=9.5,
                color=INK_SECONDARY, va="bottom")
    if xlabel:
        ax.set_xlabel(xlabel)
    if ylabel:
        ax.set_ylabel(ylabel)
    if legend:
        ax.legend(loc="best")
    if source:
        ax.figure.text(0.005, -0.02, source, fontsize=8, color=INK_MUTED,
                       ha="left", va="top")
    ax.figure.tight_layout()
    return ax
