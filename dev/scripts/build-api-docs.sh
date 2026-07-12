#!/usr/bin/env bash
# Regenerate the API reference (Scaladoc) under docs/api/.
#
# Run this whenever public Scala API surface changes — new public methods,
# trait additions, scaladoc updates. The output is checked in so that
# GitHub Pages can serve it directly from the docs/ folder.
#
# Usage:
#   dev/scripts/build-api-docs.sh

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

echo "==> Generating Scaladoc from the complete Spark 3.5 source set..."
bash dev/scripts/clean-api-docs-output.sh
mvn -q package -Pspark35 -DskipTests -Dgpg.skip=true

if [[ ! -d target/site/scaladocs ]]; then
  echo "ERROR: target/site/scaladocs not produced" >&2
  exit 1
fi

echo "==> Replacing docs/api/ with fresh output..."
rm -rf docs/api
cp -r target/site/scaladocs docs/api

echo "==> Done. Review with: git status docs/api/"
