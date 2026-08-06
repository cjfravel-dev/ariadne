#!/usr/bin/env bash
# Run every governance script that Maven binds to the test phase, without
# compiling or testing Scala.
#
# The scripts in dev/scripts validate documentation, release metadata, runtime
# policy and workflow wiring. Maven normally runs them during `mvn test`, which
# means they are only reachable after a full Scala compile. CI uses this script
# to run them directly for documentation-only changes, where the compile is
# skipped but the checks still matter.
#
# The list is derived from pom.xml rather than duplicated here, so a newly
# registered script is picked up automatically and cannot silently go unrun.

set -uo pipefail

cd "$(git rev-parse --show-toplevel)"

# Scripts that need build output and therefore cannot run on their own.
# package-contents-tests.sh inspects the packaged JAR, which only exists after
# the package phase.
REQUIRES_BUILD_OUTPUT=(
    package-contents-tests.sh
)

needs_build_output() {
    local candidate
    for candidate in "${REQUIRES_BUILD_OUTPUT[@]}"; do
        [[ "$1" == "dev/scripts/$candidate" ]] && return 0
    done
    return 1
}

mapfile -t SCRIPTS < <(grep -o 'dev/scripts/[a-z0-9-]*\.sh' pom.xml | sort -u)

if [[ ${#SCRIPTS[@]} -eq 0 ]]; then
    echo "No governance scripts found in pom.xml; refusing to report success." >&2
    exit 1
fi

failed=0
ran=0

for script in "${SCRIPTS[@]}"; do
    if needs_build_output "$script"; then
        echo "==> SKIP  $script (requires build output)"
        continue
    fi

    if [[ ! -f "$script" ]]; then
        echo "==> ERROR $script is referenced by pom.xml but does not exist" >&2
        failed=1
        continue
    fi

    if bash "$script" >/tmp/governance-output.txt 2>&1; then
        echo "==> PASS  $script"
        ran=$((ran + 1))
    else
        echo "==> FAIL  $script" >&2
        cat /tmp/governance-output.txt >&2
        failed=1
    fi
done

echo
if [[ $failed -ne 0 ]]; then
    echo "Governance checks failed."
    exit 1
fi

echo "All $ran governance checks passed."
