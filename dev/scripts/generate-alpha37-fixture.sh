#!/usr/bin/env bash

set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
TAG="0.0.1-alpha-37"
WORKTREE=$(mktemp -d)
OUTPUT=$(mktemp -d)
DESTINATION="$ROOT/src/test/resources/fixtures/alpha37"

cleanup() {
    git -C "$ROOT" worktree remove --force "$WORKTREE" >/dev/null 2>&1 || true
    rm -rf "$WORKTREE" "$OUTPUT"
}
trap cleanup EXIT

git -C "$ROOT" worktree add --detach "$WORKTREE" "$TAG"
cp "$ROOT/dev/fixtures/alpha37/GoldenFixtureGeneratorTests.scala" \
    "$WORKTREE/src/test/scala/dev/cjfravel/ariadne/"

(
    cd "$WORKTREE"
    GOLDEN_OUT="$OUTPUT" mvn -q test \
        -Dgpg.skip=true \
        -Dsuites=dev.cjfravel.ariadne.GoldenFixtureGeneratorTests
)

rm -rf "$DESTINATION"
mkdir -p "$DESTINATION"
cp -a "$OUTPUT/." "$DESTINATION/"
find "$DESTINATION" -type f -name '.*.crc' -delete
cat >"$DESTINATION/PROVENANCE.txt" <<EOF
Generated from Ariadne tag $TAG at commit $(git -C "$WORKTREE" rev-parse --short HEAD).

Generator: dev/scripts/generate-alpha37-fixture.sh
Runtime: Java 11, Scala 2.12.17, Spark 3.4.1, Delta Lake 2.4.0.
Source URI: file:///tmp/ariadne-alpha37-source.json

The fixture is immutable test input. Regenerate it only through the pinned script.
EOF

echo "Generated alpha37 fixture from $(git -C "$WORKTREE" rev-parse HEAD)"
