#!/usr/bin/env bash
# Tests for ci-change-scope.sh.
#
# The dangerous failure mode is a false "skip": reporting source=false for a
# change that actually needs the Maven build would merge untested code. These
# tests therefore cover the source-detection cases exhaustively, and assert
# that every ambiguous or degraded input falls back to source=true.

set -uo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
SCRIPT="$REPO_ROOT/dev/scripts/ci-change-scope.sh"

failures=0

# Build a throwaway git repo with a base commit and a set of changed files,
# then assert the classification.
expect_scope() {
    local expected="$1"
    local description="$2"
    shift 2
    local files=("$@")

    local tmp
    tmp=$(mktemp -d)

    (
        cd "$tmp"
        git init -q .
        git config user.email test@example.com
        git config user.name test
        mkdir -p src docs dev/scripts .github/workflows .mvn
        echo base >src/Base.scala
        echo base >docs/index.html
        echo base >pom.xml
        echo base >mvnw
        echo base >dev/scripts/thing.sh
        echo base >.github/workflows/ci.yml
        echo base >.mvn/config
        git add -A
        git commit -qm base
        git branch -q base-branch

        for f in "${files[@]}"; do
            mkdir -p "$(dirname "$f")"
            echo changed >>"$f"
        done
        git add -A
        git commit -qm change
    ) >/dev/null 2>&1

    local actual
    actual=$(cd "$tmp" && GITHUB_OUTPUT= bash "$SCRIPT" base-branch HEAD 2>/dev/null)
    rm -rf "$tmp"

    if [[ "$actual" == "source=$expected" ]]; then
        echo "PASS  $description"
    else
        echo "FAIL  $description (expected source=$expected, got '$actual')" >&2
        failures=$((failures + 1))
    fi
}

echo "== Changes that must trigger the Maven build =="
expect_scope true "Scala source change" src/main/scala/dev/cjfravel/ariadne/Index.scala
expect_scope true "pom.xml change" pom.xml
expect_scope true "Maven wrapper change" mvnw
expect_scope true "Maven wrapper config change" .mvn/wrapper/maven-wrapper.properties
expect_scope true "CI workflow change" .github/workflows/ci.yml
expect_scope true "docs change alongside a source change" docs/index.html src/main/scala/X.scala

echo
echo "== Changes that may skip the Maven build =="
expect_scope false "documentation page change" docs/users/getting-started.html
expect_scope false "README change" README.md
expect_scope false "changelog change" CHANGELOG.md
expect_scope false "dev script change" dev/scripts/readme-has-version.sh
expect_scope false "multiple documentation changes" docs/a.html docs/b.html CITATION.cff

echo
echo "== Degraded inputs must fail safe to a full build =="

actual=$(cd "$REPO_ROOT" && GITHUB_OUTPUT= bash "$SCRIPT" 2>/dev/null)
if [[ "$actual" == "source=true" ]]; then
    echo "PASS  missing base ref falls back to source=true"
else
    echo "FAIL  missing base ref (expected source=true, got '$actual')" >&2
    failures=$((failures + 1))
fi

actual=$(cd "$REPO_ROOT" && GITHUB_OUTPUT= bash "$SCRIPT" definitely-not-a-ref HEAD 2>/dev/null)
if [[ "$actual" == "source=true" ]]; then
    echo "PASS  unresolvable base ref falls back to source=true"
else
    echo "FAIL  unresolvable base ref (expected source=true, got '$actual')" >&2
    failures=$((failures + 1))
fi

echo
if [[ $failures -ne 0 ]]; then
    echo "$failures ci-change-scope test(s) failed."
    exit 1
fi

echo "All ci-change-scope tests passed."
