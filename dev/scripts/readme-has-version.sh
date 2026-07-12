#!/usr/bin/env bash

set -euo pipefail

VERSION=$(grep -oPm1 "(?<=<version>)[^<]+" pom.xml)

if [[ -z "$VERSION" ]]; then
    echo "Version not found in pom.xml"
    exit 1
fi

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
    spark_suffix=$(grep -oPm1 "(?<=<spark.suffix>)[^<]+" <<<"$profile_xml")
    scala_binary_version=$(grep -oPm1 "(?<=<scala.binary.version>)[^<]+" <<<"$profile_xml")
    artifact_id="ariadne-spark${spark_suffix}_${scala_binary_version}"

    require_compact_sequence README.md \
        "<artifactId>$artifact_id</artifactId><version>$VERSION</version>"
    require_compact_sequence docs/users/getting-started.html \
        "&lt;artifactId&gt;$artifact_id&lt;/artifactId&gt;&lt;version&gt;$VERSION&lt;/version&gt;"
done

exit $STATUS