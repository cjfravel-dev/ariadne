#!/usr/bin/env bash

set -euo pipefail

SCOVERAGE_PLUGIN=$(sed -n '/<artifactId>scoverage-maven-plugin<\/artifactId>/,/<\/plugin>/p' pom.xml)

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
require_setting '<id>check-coverage</id>'
require_setting '<phase>verify</phase>'
require_setting '<goal>check</goal>'

if ! grep -Fq 'dev/scripts/coverage-policy-tests.sh' pom.xml; then
    echo "Coverage policy test is not wired into the Maven lifecycle"
    exit 1
fi
