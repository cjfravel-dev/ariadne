#!/usr/bin/env bash
# Tests for dev/hooks/pre-commit and dev/scripts/install-git-hooks.sh.
#
# The dangerous failure modes are silent ones. A hook that is not executable is
# ignored by git and looks identical to a hook that passed, and a hook that
# re-stages a partially staged file would quietly commit work the author left
# out. Both are covered here.
#
# Maven is stubbed so these tests exercise the hook's decision logic rather than
# the build, which keeps them fast enough to run on every `mvn test`.

set -uo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
HOOK="$REPO_ROOT/dev/hooks/pre-commit"
INSTALLER="$REPO_ROOT/dev/scripts/install-git-hooks.sh"

failures=0

fail() {
    echo "FAIL: $1" >&2
    failures=$((failures + 1))
}

# A throwaway repo with the hook wired in and `mvn` stubbed on PATH.
# The stub records its invocations and can be told to fail a specific goal.
make_sandbox() {
    local tmp
    tmp=$(mktemp -d)

    mkdir -p "$tmp/repo" "$tmp/bin"
    cat >"$tmp/bin/mvn" <<'STUB'
#!/usr/bin/env bash
echo "$*" >>"$MVN_CALLS"
for arg in "$@"; do
    if [[ -n "${MVN_FAIL_GOAL:-}" && "$arg" == "$MVN_FAIL_GOAL" ]]; then
        echo "stub failure for $arg" >&2
        exit 1
    fi
    if [[ "$arg" == "scalafmt:format" && -n "${MVN_FORMAT_FILE:-}" ]]; then
        echo "// reformatted" >>"$MVN_FORMAT_FILE"
    fi
done
exit 0
STUB
    chmod +x "$tmp/bin/mvn"

    (
        cd "$tmp/repo"
        git init -q .
        git config user.email test@example.com
        git config user.name test
        mkdir -p src dev/hooks
        cp "$HOOK" dev/hooks/pre-commit
        chmod +x dev/hooks/pre-commit
        echo "object Base" >src/Base.scala
        echo "docs" >README.md
        git add -A
        git commit -qm base
        # Wired in only after the base commit, so the hook does not run before
        # the stub is on PATH.
        git config core.hooksPath dev/hooks
    ) >/dev/null 2>&1

    echo "$tmp"
}

# ---------------------------------------------------------------------------
# The hook must not pay for a compile when no Scala is staged.
# ---------------------------------------------------------------------------
sandbox=$(make_sandbox)
(
    cd "$sandbox/repo"
    export PATH="$sandbox/bin:$PATH"
    export MVN_CALLS="$sandbox/calls.txt"
    : >"$MVN_CALLS"
    echo "changed" >>README.md
    git add README.md
    git commit -qm "docs only" >/dev/null 2>&1
) >/dev/null 2>&1
if [[ -s "$sandbox/calls.txt" ]]; then
    fail "docs-only commit invoked maven: $(cat "$sandbox/calls.txt")"
fi
rm -rf "$sandbox"

# ---------------------------------------------------------------------------
# ARIADNE_SKIP_HOOKS must short-circuit before any work.
# ---------------------------------------------------------------------------
sandbox=$(make_sandbox)
(
    cd "$sandbox/repo"
    export PATH="$sandbox/bin:$PATH"
    export MVN_CALLS="$sandbox/calls.txt"
    export ARIADNE_SKIP_HOOKS=1
    : >"$MVN_CALLS"
    echo "object Changed" >src/Base.scala
    git add src/Base.scala
    git commit -qm "scala change" >/dev/null 2>&1
) >/dev/null 2>&1
if [[ -s "$sandbox/calls.txt" ]]; then
    fail "ARIADNE_SKIP_HOOKS=1 still invoked maven"
fi
rm -rf "$sandbox"

# ---------------------------------------------------------------------------
# A fully staged file that scalafmt rewrites must be re-staged, so the commit
# contains the formatted text rather than leaving it as an unstaged change.
# ---------------------------------------------------------------------------
sandbox=$(make_sandbox)
(
    cd "$sandbox/repo"
    export PATH="$sandbox/bin:$PATH"
    export MVN_CALLS="$sandbox/calls.txt"
    export MVN_FORMAT_FILE="$sandbox/repo/src/Base.scala"
    : >"$MVN_CALLS"
    echo "object Changed" >src/Base.scala
    git add src/Base.scala
    git commit -qm "scala change"
) >/dev/null 2>&1
committed=$(cd "$sandbox/repo" && git show HEAD:src/Base.scala 2>/dev/null)
if ! grep -Fq "// reformatted" <<<"$committed"; then
    fail "reformatted file was not re-staged into the commit"
fi
leftover=$(cd "$sandbox/repo" && git status --porcelain)
if [[ -n "$leftover" ]]; then
    fail "working tree left dirty after re-staging: $leftover"
fi
rm -rf "$sandbox"

# ---------------------------------------------------------------------------
# A partially staged file must NOT be re-staged: `git add` would sweep in the
# unstaged work the author deliberately excluded. The commit must abort instead.
# ---------------------------------------------------------------------------
sandbox=$(make_sandbox)
(
    cd "$sandbox/repo"
    export PATH="$sandbox/bin:$PATH"
    export MVN_CALLS="$sandbox/calls.txt"
    export MVN_FORMAT_FILE="$sandbox/repo/src/Base.scala"
    : >"$MVN_CALLS"
    echo "object Staged" >src/Base.scala
    git add src/Base.scala
    echo "// unstaged work in progress" >>src/Base.scala
    git commit -qm "partial"
) >/dev/null 2>&1
if (cd "$sandbox/repo" && git log --oneline | grep -q partial); then
    fail "commit succeeded despite a partially staged reformatted file"
fi
staged_now=$(cd "$sandbox/repo" && git diff --cached -- src/Base.scala)
if grep -Fq "unstaged work in progress" <<<"$staged_now"; then
    fail "hook staged unstaged work from a partially staged file"
fi
rm -rf "$sandbox"

# ---------------------------------------------------------------------------
# A scalafix or scalastyle violation must abort the commit.
# ---------------------------------------------------------------------------
for goal in "scalafix:scalafix@scalafix-check" "scalastyle:check@scalastyle-check"; do
    sandbox=$(make_sandbox)
    (
        cd "$sandbox/repo"
        export PATH="$sandbox/bin:$PATH"
        export MVN_CALLS="$sandbox/calls.txt"
        export MVN_FAIL_GOAL="$goal"
        : >"$MVN_CALLS"
        echo "object Changed" >src/Base.scala
        git add src/Base.scala
        git commit -qm "violating commit"
    ) >/dev/null 2>&1
    if (cd "$sandbox/repo" && git log --oneline | grep -q "violating commit"); then
        fail "commit succeeded despite a $goal failure"
    fi
    rm -rf "$sandbox"
done

# ---------------------------------------------------------------------------
# scalafix must run only after a compile, or it lints stale semanticdb.
# ---------------------------------------------------------------------------
sandbox=$(make_sandbox)
(
    cd "$sandbox/repo"
    export PATH="$sandbox/bin:$PATH"
    export MVN_CALLS="$sandbox/calls.txt"
    : >"$MVN_CALLS"
    echo "object Changed" >src/Base.scala
    git add src/Base.scala
    git commit -qm "scala change"
) >/dev/null 2>&1
compile_line=$(grep -n "test-compile" "$sandbox/calls.txt" | head -1 | cut -d: -f1)
scalafix_line=$(grep -n "scalafix-check" "$sandbox/calls.txt" | head -1 | cut -d: -f1)
if [[ -z "$compile_line" || -z "$scalafix_line" ]]; then
    fail "hook did not run both test-compile and scalafix (calls: $(tr '\n' ';' <"$sandbox/calls.txt"))"
elif [[ "$compile_line" -ge "$scalafix_line" ]]; then
    fail "scalafix ran before test-compile, so it would lint stale semanticdb"
fi
rm -rf "$sandbox"

# ---------------------------------------------------------------------------
# Installer round trip.
# ---------------------------------------------------------------------------
sandbox=$(mktemp -d)
(
    cd "$sandbox"
    git init -q .
    git config user.email test@example.com
    git config user.name test
    mkdir -p dev/hooks dev/scripts
    cp "$HOOK" dev/hooks/pre-commit
    chmod +x dev/hooks/pre-commit
    cp "$INSTALLER" dev/scripts/install-git-hooks.sh
    chmod +x dev/scripts/install-git-hooks.sh
    git add -A
    git commit -qm base
) >/dev/null 2>&1

if (cd "$sandbox" && ./dev/scripts/install-git-hooks.sh --check) >/dev/null 2>&1; then
    fail "--check reported installed before installation"
fi
if ! (cd "$sandbox" && ./dev/scripts/install-git-hooks.sh) >/dev/null 2>&1; then
    fail "installer failed on a clean repository"
fi
configured=$(cd "$sandbox" && git config --local --get core.hooksPath)
if [[ "$configured" != "dev/hooks" ]]; then
    fail "core.hooksPath is '$configured', expected 'dev/hooks'"
fi
if ! (cd "$sandbox" && ./dev/scripts/install-git-hooks.sh --check) >/dev/null 2>&1; then
    fail "--check reported not installed after installation"
fi
if ! (cd "$sandbox" && ./dev/scripts/install-git-hooks.sh --uninstall) >/dev/null 2>&1; then
    fail "uninstall failed"
fi
if (cd "$sandbox" && git config --local --get core.hooksPath) >/dev/null 2>&1; then
    fail "core.hooksPath survived uninstall"
fi

# A non-executable hook is silently ignored by git, so installation must refuse.
(cd "$sandbox" && chmod -x dev/hooks/pre-commit)
if (cd "$sandbox" && ./dev/scripts/install-git-hooks.sh) >/dev/null 2>&1; then
    fail "installer accepted a non-executable hook"
fi
rm -rf "$sandbox"

# ---------------------------------------------------------------------------
# The committed hook must itself be executable, for the same reason.
# ---------------------------------------------------------------------------
mode=$(git -C "$REPO_ROOT" ls-files -s dev/hooks/pre-commit | awk '{print $1}')
if [[ "$mode" != "100755" ]]; then
    fail "dev/hooks/pre-commit is committed with mode $mode; git ignores non-executable hooks"
fi
mode=$(git -C "$REPO_ROOT" ls-files -s dev/scripts/install-git-hooks.sh | awk '{print $1}')
if [[ "$mode" != "100755" ]]; then
    fail "dev/scripts/install-git-hooks.sh is committed with mode $mode"
fi

if [[ $failures -ne 0 ]]; then
    echo "$failures git hook test(s) failed."
    exit 1
fi

echo "All git hook tests passed."
