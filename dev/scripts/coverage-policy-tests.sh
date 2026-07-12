#!/usr/bin/env bash

set -euo pipefail

COVERAGE_PROFILE=$(sed -n '/<id>coverage-enforcement<\/id>/,/<\/profile>/p' pom.xml)
COVERAGE_EXECUTION=$(sed -n '/<id>check-coverage<\/id>/,/<\/execution>/p' <<<"$COVERAGE_PROFILE")
SPARK35_PROFILE=$(sed -n '/<id>spark35<\/id>/,/<\/profile>/p' pom.xml)
SPARK41_PROFILE=$(sed -n '/<id>spark41<\/id>/,/<\/profile>/p' pom.xml)

require_setting() {
    local expected="$1"
    if ! grep -Fq -- "$expected" pom.xml; then
        echo "Scoverage configuration is missing coverage policy: $expected"
        exit 1
    fi
}

require_setting '<minimumCoverage>80</minimumCoverage>'
require_setting '<minimumCoverageBranchTotal>73</minimumCoverageBranchTotal>'
require_setting '<failOnMinimumCoverage>true</failOnMinimumCoverage>'

for expected in \
    '<id>coverage-enforcement</id>' \
    '<name>env.CI</name>' \
    '<value>true</value>' \
    '<artifactId>scoverage-maven-plugin</artifactId>'; do
    if ! grep -Fq -- "$expected" <<<"$COVERAGE_PROFILE"; then
        echo "CI coverage profile is missing enforcement: $expected"
        exit 1
    fi
done

for expected in '<id>check-coverage</id>' '<phase>verify</phase>' '<goal>check</goal>'; do
    if ! grep -Fq -- "$expected" <<<"$COVERAGE_EXECUTION"; then
        echo "CI check-coverage execution is missing enforcement: $expected"
        exit 1
    fi
done

if ! grep -Fq '<scoverage.skip>true</scoverage.skip>' <<<"$SPARK41_PROFILE"; then
    echo "Spark 4.1 profile must skip unavailable Scoverage instrumentation"
    exit 1
fi

if ! grep -Fq '<jdk>[11,12)</jdk>' <<<"$SPARK35_PROFILE"; then
    echo "Spark 3.5 must activate on the required Java 11 runtime alongside CI coverage"
    exit 1
fi

if ! grep -Fq 'dev/scripts/coverage-policy-tests.sh' pom.xml; then
    echo "Coverage policy test is not wired into the Maven lifecycle"
    exit 1
fi
