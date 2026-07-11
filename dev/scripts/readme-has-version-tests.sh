#!/bin/bash

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
TEST_ROOT=$(mktemp -d)
trap 'rm -rf "$TEST_ROOT"' EXIT

mkdir -p "$TEST_ROOT/dev/scripts" "$TEST_ROOT/docs/users"
cp "$REPO_ROOT/dev/scripts/readme-has-version.sh" "$TEST_ROOT/dev/scripts/"

cat >"$TEST_ROOT/pom.xml" <<'EOF'
<project>
  <version>1.2.3</version>
</project>
EOF
echo "Install version 1.2.3" >"$TEST_ROOT/README.md"
echo "Install version 1.2.3" >"$TEST_ROOT/docs/users/getting-started.html"
echo "## [1.2.2]" >"$TEST_ROOT/CHANGELOG.md"

if (cd "$TEST_ROOT" && bash dev/scripts/readme-has-version.sh >/dev/null); then
  echo "Version check accepted a changelog that omitted the current version"
  exit 1
fi

echo "## [1.2.3]" >"$TEST_ROOT/CHANGELOG.md"
(cd "$TEST_ROOT" && bash dev/scripts/readme-has-version.sh >/dev/null)
