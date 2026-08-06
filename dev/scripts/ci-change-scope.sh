#!/usr/bin/env bash
# Classify a change set as source-affecting or not, so CI can skip the
# expensive Scala compile/test/coverage work for documentation-only changes.
#
# This script decides ONLY whether the Maven build is needed. It never gates
# the governance scripts in dev/scripts, which validate documentation and
# release metadata and must run for every change -- they are the checks most
# likely to catch a docs-only mistake.
#
# The classification fails safe: any uncertainty (missing base ref, shallow
# clone, unreadable history) reports source=true and runs the full build. A
# wrong "build" costs minutes; a wrong "skip" ships an untested change.
#
# Usage:
#   dev/scripts/ci-change-scope.sh <base-ref> [head-ref]
#
# Writes "source=true|false" to stdout and, when running under GitHub Actions,
# appends it to $GITHUB_OUTPUT.

set -uo pipefail

BASE_REF="${1:-}"
HEAD_REF="${2:-HEAD}"

emit() {
    echo "source=$1"
    if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
        echo "source=$1" >>"$GITHUB_OUTPUT"
    fi
    exit 0
}

reason() {
    echo "==> $1" >&2
}

if [[ -z "$BASE_REF" ]]; then
    reason "No base ref supplied; assuming the change affects source."
    emit true
fi

MERGE_BASE=$(git merge-base "$BASE_REF" "$HEAD_REF" 2>/dev/null)
if [[ -z "$MERGE_BASE" ]]; then
    reason "Could not resolve a merge base for '$BASE_REF'..'$HEAD_REF'; assuming the change affects source."
    emit true
fi

CHANGED=$(git diff --name-only "$MERGE_BASE" "$HEAD_REF" 2>/dev/null)
if [[ $? -ne 0 ]]; then
    reason "Could not diff '$MERGE_BASE'..'$HEAD_REF'; assuming the change affects source."
    emit true
fi

if [[ -z "$CHANGED" ]]; then
    reason "No files changed; skipping the Maven build."
    emit false
fi

# Paths that can change what the build produces or how it is built. Everything
# else -- documentation, release metadata, dev scripts -- is covered by the
# governance scripts, which always run.
while IFS= read -r file; do
    [[ -z "$file" ]] && continue
    case "$file" in
        src/* | pom.xml | mvnw | mvnw.cmd | .mvn/* | .github/workflows/*)
            reason "Source-affecting change detected: $file"
            emit true
            ;;
    esac
done <<<"$CHANGED"

reason "Only documentation, release metadata, or dev scripts changed:"
while IFS= read -r file; do
    [[ -n "$file" ]] && reason "    $file"
done <<<"$CHANGED"
emit false
