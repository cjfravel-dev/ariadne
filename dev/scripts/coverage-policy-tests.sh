#!/usr/bin/env bash

set -euo pipefail

SCOVERAGE_PLUGIN=$(sed -n '/<artifactId>scoverage-maven-plugin<\/artifactId>/,/<\/plugin>/p' pom.xml)
SPARK35_PROFILE=$(sed -n '/<id>spark35<\/id>/,/<\/profile>/p' pom.xml)

require_setting() {
    local expected="$1"
    if ! grep -Fq -- "$expected" <<<"$SCOVERAGE_PLUGIN"; then
        echo "Scoverage configuration is missing coverage policy: $expected"
        exit 1
    fi
}

require_setting '<minimumCoverage>80</minimumCoverage>'
require_setting '<minimumCoverageBranchTotal>73</minimumCoverageBranchTotal>'
require_setting '<failOnMinimumCoverage>true</failOnMinimumCoverage>'

for expected in \
    '<artifactId>scoverage-maven-plugin</artifactId>' \
    '<id>check-coverage</id>' \
    '<phase>verify</phase>' \
    '<goal>check</goal>'; do
    if ! grep -Fq -- "$expected" <<<"$SPARK35_PROFILE"; then
        echo "Spark 3.5 profile is missing coverage enforcement: $expected"
        exit 1
    fi
done

if ! grep -Fq 'dev/scripts/coverage-policy-tests.sh' pom.xml; then
    echo "Coverage policy test is not wired into the Maven lifecycle"
    exit 1
fi
