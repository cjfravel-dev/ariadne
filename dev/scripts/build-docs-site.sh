#!/usr/bin/env bash
# Assemble the complete documentation site that GitHub Pages publishes.
#
# The site is built, never committed. Hand-written pages live in docs/ and
# reference the release version through the __ARIADNE_VERSION__ token; the
# generated Scaladoc is produced from source at build time. Both are combined
# here so CI and local previews share one code path.
#
# Usage:
#   dev/scripts/build-docs-site.sh [output-dir]
#
# Default output: target/site/pages
#
# Preview locally with:
#   dev/scripts/build-docs-site.sh && python3 -m http.server -d target/site/pages

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

OUTPUT_DIR=${1:-target/site/pages}
VERSION_TOKEN='__ARIADNE_VERSION__'

echo "==> Resolving project version..."
VERSION=$(./mvnw -q help:evaluate -Dexpression=project.version -DforceStdout)
if [[ -z "$VERSION" ]]; then
    echo "ERROR: unable to resolve project version from pom.xml" >&2
    exit 1
fi
echo "    version: $VERSION"

echo "==> Generating Scaladoc from the canonical Spark 3.5 source set..."
bash dev/scripts/clean-api-docs-output.sh
./mvnw -q package -Pspark35 -DskipTests -Dgpg.skip=true

if [[ ! -d target/site/scaladocs ]]; then
    echo "ERROR: target/site/scaladocs not produced" >&2
    exit 1
fi

echo "==> Assembling site into $OUTPUT_DIR..."
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"
cp -r docs/. "$OUTPUT_DIR"/
cp -r target/site/scaladocs "$OUTPUT_DIR/api"

echo "==> Substituting $VERSION_TOKEN -> $VERSION..."
substituted=0
while IFS= read -r -d '' file; do
    if grep -Fq "$VERSION_TOKEN" "$file"; then
        # The version is a plain Maven coordinate, so no escaping is required
        # beyond the delimiter, which cannot appear in a version string.
        sed -i "s|$VERSION_TOKEN|$VERSION|g" "$file"
        substituted=$((substituted + 1))
    fi
done < <(find "$OUTPUT_DIR" -type f \( -name '*.html' -o -name '*.js' -o -name '*.css' \) -print0)
echo "    substituted in $substituted file(s)"

if grep -rqF "$VERSION_TOKEN" "$OUTPUT_DIR"; then
    echo "ERROR: unsubstituted $VERSION_TOKEN remains in the assembled site" >&2
    grep -rlF "$VERSION_TOKEN" "$OUTPUT_DIR" >&2
    exit 1
fi

# Pages must not run the output through Jekyll, which would strip underscore-
# prefixed paths that Scaladoc emits.
touch "$OUTPUT_DIR/.nojekyll"

echo "==> Done. Site assembled at $OUTPUT_DIR"
