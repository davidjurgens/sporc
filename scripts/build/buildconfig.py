"""
Paths and tuning for a dataset build, read from a JSON config.

These scripts used to derive everything from their own location: the build
directory was `dirname(__file__)` and the release directory was its parent,
because the scratch directory lived inside the release. That coupling is what
made it impossible to move the scripts into version control without also
shipping 61 GB of intermediates to Hugging Face, so the paths now come from a
config file instead.

    from buildconfig import load
    cfg = load()
    cfg.source                      # the 1.0 corpus being read
    cfg.release                     # the 1.1 tree being written
    cfg.build                       # scratch: bands/, turns_raw/, logs
    cfg.metadata("turns_search.duckdb")
    cfg.tree("turns/text")          # release-relative
    cfg.scratch("bands")            # build-relative, created on demand

The config is found in this order:

1. ``--config PATH`` on the command line (see :func:`add_argument`)
2. ``$SPORC_BUILD_CONFIG``
3. ``DEFAULT_CONFIG`` below

Nothing here creates or writes the release tree; a build stage that wants a
directory asks for it explicitly with ``scratch()``.
"""

import json
import os
import sys

DEFAULT_CONFIG = "/shared/6/projects/sporc/build/sporc1.1.json"

_REQUIRED = ("source", "release", "build")


class BuildConfig:
    """Resolved paths and tuning knobs for one build."""

    def __init__(self, data: dict, origin: str):
        self.origin = origin
        self.name = data.get("name", "unnamed")
        self.version = data.get("version", "")

        paths = data.get("paths", {})
        missing = [k for k in _REQUIRED if not paths.get(k)]
        if missing:
            raise ValueError(
                f"{origin}: paths.{{{','.join(missing)}}} missing or empty. "
                f"Every build needs a source corpus to read, a release tree to "
                f"write, and a build directory for intermediates.")

        # Absolute so a stage's working directory cannot change what it means.
        self.source = os.path.abspath(paths["source"])
        self.release = os.path.abspath(paths["release"])
        self.build = os.path.abspath(paths["build"])

        if self.build == self.release or self.build.startswith(
                self.release + os.sep):
            raise ValueError(
                f"{origin}: paths.build ({self.build}) is inside paths.release "
                f"({self.release}). The release tree is uploaded verbatim, so "
                f"intermediates kept inside it ship with the dataset -- which "
                f"is the arrangement this config exists to undo.")

        tuning = data.get("tuning", {})
        self.workers = int(tuning.get("workers", 16))
        self.duckdb_memory = str(tuning.get("duckdb_memory", "64GB"))
        self.duckdb_threads = int(tuning.get("duckdb_threads", self.workers))

    # -- release tree ---------------------------------------------------
    def rel(self, *parts) -> str:
        """A path inside the release tree."""
        return os.path.join(self.release, *parts)

    def tree(self, name: str) -> str:
        """A tree directory in the release, e.g. tree('turns/text')."""
        return os.path.join(self.release, *name.split("/"))

    def parts(self, name: str) -> str:
        """The part-file glob for a release tree."""
        return os.path.join(self.tree(name), "part-*.parquet")

    def metadata(self, *parts) -> str:
        """A path inside the release's metadata directory."""
        return os.path.join(self.release, "metadata", *parts)

    # -- source corpus --------------------------------------------------
    def src(self, *parts) -> str:
        """A path inside the source corpus being read."""
        return os.path.join(self.source, *parts)

    # -- scratch --------------------------------------------------------
    def scratch(self, *parts, create: bool = True) -> str:
        """
        A path inside the build directory, created unless create=False.

        Intermediates, logs and DuckDB spill live here. Nothing under this
        path is published.
        """
        path = os.path.join(self.build, *parts)
        if create:
            os.makedirs(path, exist_ok=True)
        return path

    @property
    def duckdb_tmp(self) -> str:
        """Spill directory for DuckDB, kept off /tmp and off the release."""
        return self.scratch("duckdb_tmp")

    def __repr__(self):
        return (f"BuildConfig({self.name!r} from {self.origin!r}: "
                f"source={self.source!r} release={self.release!r} "
                f"build={self.build!r})")


def _from_argv() -> str:
    """
    ``--config PATH`` from the command line, if present.

    The stages take no other arguments and build their config at import time,
    before any parsing could happen, so this is read straight from argv. A
    stage that grows a real argparse parser should use :func:`add_argument`
    instead; this stays compatible with it.
    """
    argv = sys.argv[1:]
    if "--config" in argv:
        i = argv.index("--config")
        if i + 1 < len(argv):
            return argv[i + 1]
        raise ValueError("--config given with no path after it")
    for a in argv:
        if a.startswith("--config="):
            return a.split("=", 1)[1]
    return None


def config_path(explicit: str = None) -> str:
    return (explicit
            or _from_argv()
            or os.environ.get("SPORC_BUILD_CONFIG")
            or DEFAULT_CONFIG)


def load(explicit: str = None) -> BuildConfig:
    """Load the build config, failing loudly if it is not there."""
    path = config_path(explicit)
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"No build config at {path}. Pass --config, set "
            f"$SPORC_BUILD_CONFIG, or create that file. See "
            f"scripts/build/README.md for the schema.")
    with open(path) as fh:
        data = json.load(fh)
    return BuildConfig(data, path)


def add_argument(parser):
    """Add the standard --config option to an argparse parser."""
    parser.add_argument(
        "--config", default=None,
        help=f"Build config JSON (default: $SPORC_BUILD_CONFIG or "
             f"{DEFAULT_CONFIG})")
    return parser
