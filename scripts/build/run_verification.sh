#!/bin/bash
# Verify the sporc client against a built release.
#
# Runs the test suite, then the API audit against the tutorial subset and the
# full corpus, then clears the caches the client leaves inside the release
# tree. Paths come from the build config, never from this file.
#
#   cd /home/jurgens/projects/sporc
#   setsid nohup bash scripts/build/run_verification.sh > "$BUILD/verification.log" 2>&1 &
#
# setsid detaches it, so it survives the terminal or agent session ending.
# Point it at another build with --config PATH or $SPORC_BUILD_CONFIG; the log
# names the config it used.

set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$HERE/../.." && pwd)

# Read the paths from the config rather than repeating them.
eval "$(SPORC_BUILD_SCRIPTS="$HERE" python3 - "$@" <<'PYEOF'
import os
import sys

sys.path.insert(0, os.environ["SPORC_BUILD_SCRIPTS"])
from buildconfig import config_path, load

argv = sys.argv[1:]
explicit = argv[argv.index("--config") + 1] if "--config" in argv else None
cfg = load(explicit)
print(f"BUILD={cfg.build}")
print(f"RELEASE={cfg.release}")
print(f"CONFIG={config_path(explicit)}")
PYEOF
)"

if [ -z "${BUILD:-}" ] || [ -z "${RELEASE:-}" ]; then
    echo "could not read the build config; aborting" >&2
    exit 1
fi

export MALLET_PATH=${MALLET_PATH:-/shared/0/resources/mallet/mallet-2.0.8/bin/mallet}
cd "$REPO" || exit 1
stamp() { date '+%Y-%m-%d %H:%M:%S'; }

run() {   # run <name> <logfile> <cmd...>
    local name=$1 log=$2; shift 2
    echo "STEP  $(stamp)  $name -> $log"
    "$@" > "$log" 2>&1
    local rc=$?
    echo "RESULT $(stamp)  $name exit=$rc"
    return $rc
}

echo "STEP  $(stamp)  verification start (pid $$)  config=$CONFIG"

run "pytest"       "$BUILD/pytest.log"       python3 -m pytest -q
run "audit:subset" "$BUILD/audit_subset.log" python3 -u scripts/audit_api.py \
                                               --data-dir subsets/tutorial
run "audit:full"   "$BUILD/audit_full.log"   python3 -u scripts/audit_api.py \
                                               --data-dir "$RELEASE" --search

# The client writes its own caches into whatever metadata directory it reads.
# The audit above just recreated them inside the tree that gets published,
# where they are several hundred MB of derived junk. Remove them every time.
echo "STEP  $(stamp)  removing client caches from the release tree"
rm -fv "$RELEASE"/metadata/_index_cache.pkl \
       "$RELEASE"/metadata/_episode_df.arrow \
       "$RELEASE"/metadata/_podcast_df.arrow

echo "STEP  $(stamp)  verification done"
