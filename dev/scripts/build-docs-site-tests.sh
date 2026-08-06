#!/usr/bin/env bash
# Tests for the output-directory guard in build-docs-site.sh.
#
# The script deletes its output directory with `rm -rf`, so a stray argument
# could destroy the checkout. These tests pin the guard: `docs` is the most
# plausible mistake, since it would delete the hand-written pages the script
# reads from, and `.` or `/` would be far worse.
#
# Only the guard is exercised. The full site build needs Maven and a JDK, which
# is covered by the Pages workflow.

set -uo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
SCRIPT="$REPO_ROOT/dev/scripts/build-docs-site.sh"

failures=0

# The guard runs before any Maven work, so a rejected path exits non-zero with
# a message and an accepted path proceeds past the guard into version
# resolution. Matching on the guard's own output keeps this fast.
expect_rejected() {
    local arg="$1"
    local description="$2"
    local output

    output=$(cd "$REPO_ROOT" && bash "$SCRIPT" "$arg" 2>&1)
    local status=$?

    if [[ $status -ne 0 ]] && grep -qE 'ERROR: (refusing|output directory)' <<<"$output"; then
        echo "PASS  rejects $description"
    else
        echo "FAIL  should reject $description (exit=$status)" >&2
        echo "      output: $(head -3 <<<"$output")" >&2
        failures=$((failures + 1))
    fi
}

expect_accepted() {
    local description="$2"
    local output
    output=$(cd "$REPO_ROOT" && bash "$SCRIPT" "$1" 2>&1)

    if grep -qE 'ERROR: (refusing|output directory)' <<<"$output"; then
        echo "FAIL  should accept $description" >&2
        echo "      output: $(head -3 <<<"$output")" >&2
        failures=$((failures + 1))
    else
        echo "PASS  accepts $description"
    fi
}

echo "== Dangerous output directories must be rejected =="
expect_rejected "." "the repository root as '.'"
expect_rejected "/" "the filesystem root"
expect_rejected "$REPO_ROOT" "the repository root by absolute path"
expect_rejected "docs" "the hand-written docs directory"
expect_rejected "src" "the source directory"
expect_rejected "dev/scripts" "the dev scripts directory"
expect_rejected "/tmp/ariadne-outside-repo" "a path outside the repository"
expect_rejected "   " "a whitespace-only argument"

echo
echo "== Safe inputs must be accepted =="
# An empty argument is not dangerous: ${1:-default} substitutes for both unset
# and empty, so it resolves to the default disposable path.
expect_accepted "" "an empty argument (falls back to the default path)"
# Confirm the guard lets a clean target path through: it must not print any of
# the guard's rejection messages before moving on to version resolution.
output=$(cd "$REPO_ROOT" && timeout 20 bash "$SCRIPT" target/site/guard-check 2>&1)
if grep -qE 'ERROR: (refusing|output directory)' <<<"$output"; then
    echo "FAIL  target/site/guard-check was rejected" >&2
    echo "      output: $(head -3 <<<"$output")" >&2
    failures=$((failures + 1))
else
    echo "PASS  accepts target/site/guard-check"
fi
rm -rf "$REPO_ROOT/target/site/guard-check"

echo
if [[ $failures -ne 0 ]]; then
    echo "$failures build-docs-site guard test(s) failed."
    exit 1
fi

echo "All build-docs-site guard tests passed."
