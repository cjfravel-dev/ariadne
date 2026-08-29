#!/usr/bin/env bash
# Point git at the version-controlled hooks in dev/hooks.
#
# Uses core.hooksPath rather than copying files into .git/hooks, so a hook that
# changes on main takes effect on the next pull instead of silently running a
# stale copy. Nothing under .git needs to be touched, and the hooks stay
# reviewable in the repository.
#
# Usage:
#   dev/scripts/install-git-hooks.sh              install
#   dev/scripts/install-git-hooks.sh --uninstall  revert to .git/hooks
#   dev/scripts/install-git-hooks.sh --check      report status only (exit 1 if not installed)

set -uo pipefail

HOOKS_DIR="dev/hooks"

REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null) || {
    echo "Not inside a git repository." >&2
    exit 1
}
cd "$REPO_ROOT" || exit 1

MODE="install"
case "${1:-}" in
    --uninstall) MODE="uninstall" ;;
    --check) MODE="check" ;;
    "") ;;
    *)
        echo "Unknown argument: $1" >&2
        echo "Usage: $0 [--uninstall|--check]" >&2
        exit 1
        ;;
esac

current=$(git config --local --get core.hooksPath || true)

if [[ "$MODE" == "uninstall" ]]; then
    if [[ -z "$current" ]]; then
        echo "Hooks are not installed; nothing to do."
        exit 0
    fi
    git config --local --unset core.hooksPath
    echo "Uninstalled: core.hooksPath cleared, git will use .git/hooks again."
    exit 0
fi

if [[ "$MODE" == "check" ]]; then
    if [[ "$current" == "$HOOKS_DIR" ]]; then
        echo "Installed: core.hooksPath = $current"
        exit 0
    fi
    echo "Not installed (core.hooksPath = ${current:-<unset>}). Run dev/scripts/install-git-hooks.sh" >&2
    exit 1
fi

if [[ ! -d "$HOOKS_DIR" ]]; then
    echo "$HOOKS_DIR does not exist." >&2
    exit 1
fi

# A hook that is not executable is silently ignored by git, which looks exactly
# like a hook that passed. Fail loudly instead.
missing_exec=0
for hook in "$HOOKS_DIR"/*; do
    [[ -f "$hook" ]] || continue
    if [[ ! -x "$hook" ]]; then
        echo "Not executable: $hook (run: chmod +x $hook)" >&2
        missing_exec=1
    fi
done
[[ $missing_exec -eq 0 ]] || exit 1

# core.hooksPath replaces .git/hooks wholesale, so any local hook there stops
# running. Say so rather than letting it disappear quietly.
if [[ -n "${GIT_DIR:-}" ]]; then
    git_dir="$GIT_DIR"
else
    git_dir=$(git rev-parse --git-dir)
fi

if [[ -d "$git_dir/hooks" ]]; then
    for existing in "$git_dir/hooks"/*; do
        [[ -f "$existing" && -x "$existing" && "$existing" != *.sample ]] || continue
        echo "Note: $existing will no longer run while core.hooksPath is set." >&2
    done
fi

git config --local core.hooksPath "$HOOKS_DIR"

echo "Installed: core.hooksPath = $HOOKS_DIR"
echo
echo "The pre-commit hook formats staged Scala with scalafmt and fails the commit"
echo "on scalafix or scalastyle violations, the two lint gates that fail CI."
echo
echo "  git commit --no-verify        skip for one commit"
echo "  ARIADNE_SKIP_HOOKS=1          disable all hooks"
echo "  ARIADNE_HOOK_SKIP_SCALAFIX=1  skip only the compile-dependent scalafix step"
