#!/usr/bin/env bash

set -euo pipefail

VERSION=$(grep -oPm1 "(?<=<version>)[^<]+" pom.xml || true)

if [[ -z "$VERSION" ]]; then
    echo "Version not found in pom.xml"
    exit 1
fi

VERSION_TOKEN='__ARIADNE_VERSION__'

STATUS=0

require_fixed_line() {
    local file="$1"
    local expected="$2"
    if ! grep -Fqx "$expected" "$file"; then
        echo "$file is missing exact release metadata: $expected"
        STATUS=1
    fi
}

require_compact_sequence() {
    local file="$1"
    local expected="$2"
    local compact
    compact=$(tr -d '[:space:]' <"$file")
    if ! grep -Fq "$expected" <<<"$compact"; then
        echo "$file is missing coordinated release metadata: $expected"
        STATUS=1
    fi
}

for file in README.md CHANGELOG.md CITATION.cff docs/users/getting-started.html; do
    if [[ ! -f "$file" ]]; then
        echo "Version-check file missing: $file"
        exit 1
    fi
done

require_fixed_line CHANGELOG.md "## [$VERSION]"
require_fixed_line CITATION.cff "version: $VERSION"

for profile in spark35 spark41; do
    profile_xml=$(awk "/<id>$profile<\\/id>/,/<\\/profile>/" pom.xml)
    spark_suffix=$(grep -oPm1 "(?<=<spark.suffix>)[^<]+" <<<"$profile_xml" || true)
    scala_binary_version=$(grep -oPm1 "(?<=<scala.binary.version>)[^<]+" <<<"$profile_xml" || true)
    if [[ -z "$spark_suffix" || -z "$scala_binary_version" ]]; then
        echo "Unable to determine artifact coordinates for $profile"
        STATUS=1
        continue
    fi
    artifact_id="ariadne-spark${spark_suffix}_${scala_binary_version}"

    # README.md is rendered on GitHub and Maven Central, so it carries the
    # literal version.
    require_compact_sequence README.md \
        "<artifactId>$artifact_id</artifactId><version>$VERSION</version>"

    # Documentation pages are published through GitHub Pages, which substitutes
    # the version token when the site is built. They must reference the token so
    # that a release never has to edit them.
    require_compact_sequence docs/users/getting-started.html \
        "&lt;artifactId&gt;$artifact_id&lt;/artifactId&gt;&lt;version&gt;$VERSION_TOKEN&lt;/version&gt;"
done

# A literal version committed under docs/ silently goes stale, because the site
# build only rewrites the token.
while IFS= read -r -d '' file; do
    if grep -Fq "$VERSION" "$file"; then
        echo "$file hardcodes the release version; use $VERSION_TOKEN instead"
        STATUS=1
    fi
done < <(find docs -type f -name '*.html' -print0)

exit $STATUS