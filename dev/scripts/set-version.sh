#!/usr/bin/env bash
# Set the coordinated release version across every file that must carry it
# literally.
#
# Only four files hold a literal version: pom.xml is the source of truth,
# README.md and CITATION.cff are consumed outside GitHub Pages (Maven Central
# and citation tooling), and CHANGELOG.md needs a release heading. Every
# documentation page under docs/ uses the __ARIADNE_VERSION__ token instead and
# is substituted when the site is built, so it never needs editing.
#
# Usage:
#   dev/scripts/set-version.sh <version>
#
# Example:
#   dev/scripts/set-version.sh 0.1.8-beta

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

if [[ $# -ne 1 ]]; then
    echo "Usage: dev/scripts/set-version.sh <version>" >&2
    exit 1
fi

NEW_VERSION="$1"

if [[ ! "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
    echo "Version must look like 1.2.3 or 1.2.3-beta: $NEW_VERSION" >&2
    exit 1
fi

if [[ "$NEW_VERSION" == *-SNAPSHOT ]]; then
    echo "Refusing to set a SNAPSHOT version; releases must be fixed." >&2
    exit 1
fi

OLD_VERSION=$(grep -oPm1 "(?<=<version>)[^<]+" pom.xml)
if [[ -z "$OLD_VERSION" ]]; then
    echo "Unable to read the current version from pom.xml" >&2
    exit 1
fi

if [[ "$OLD_VERSION" == "$NEW_VERSION" ]]; then
    echo "Version is already $NEW_VERSION; nothing to do."
    exit 0
fi

echo "==> $OLD_VERSION -> $NEW_VERSION"

# Only the first <version> element is the project version; plugin and
# dependency versions that follow must not be touched.
perl -0pi -e "s|<version>\Q$OLD_VERSION\E</version>|<version>$NEW_VERSION</version>|" pom.xml
echo "    pom.xml"

sed -i "s|<version>$OLD_VERSION</version>|<version>$NEW_VERSION</version>|g" README.md
echo "    README.md"

sed -i "s|^version: $OLD_VERSION$|version: $NEW_VERSION|" CITATION.cff
sed -i "s|^date-released: .*$|date-released: $(date -u +%Y-%m-%d)|" CITATION.cff
echo "    CITATION.cff"

if grep -Fqx "## [$NEW_VERSION]" CHANGELOG.md; then
    echo "    CHANGELOG.md already has a [$NEW_VERSION] section"
else
    # Promote the accumulated Unreleased entries into the new release section
    # and leave a fresh Unreleased heading behind.
    perl -0pi -e "s|## \[Unreleased\]\n|## [Unreleased]\n\n## [$NEW_VERSION]\n|" CHANGELOG.md
    echo "    CHANGELOG.md (promoted [Unreleased] to [$NEW_VERSION])"
fi

echo
echo "==> Verifying release metadata..."
bash dev/scripts/readme-has-version.sh

echo
echo "Done. Review with: git diff"
