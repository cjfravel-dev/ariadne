#!/bin/bash

set -euo pipefail

WORKFLOW=".github/workflows/ci.yml"
POM="pom.xml"

if [[ ! -f "$WORKFLOW" ]]; then
    echo "CI workflow not found: $WORKFLOW"
    exit 1
fi

assert_contains() {
    local expected="$1"
    local description="$2"
    if ! grep -Fq "$expected" "$WORKFLOW"; then
        echo "CI workflow is missing $description: $expected"
        exit 1
    fi
}

if grep -Eq 'mvn .*verify.*scoverage:report|mvn .*scoverage:report.*verify' "$WORKFLOW"; then
    echo "CI must not combine verify and scoverage:report in one invocation because it runs tests twice"
    exit 1
fi

assert_contains 'mvn -B -q scoverage:report -Dgpg.skip=true' "single coverage lifecycle"
assert_contains 'mvn -B -q verify -DskipTests -Dgpg.skip=true' "post-coverage verify command"
assert_contains 'MAVEN_OPTS: -Xmx8g -XX:+UseG1GC' "bounded Maven heap"

if ! grep -Fq '<skip>${skipTests}</skip>' "$POM"; then
    echo "Maven exec scripts must honor -DskipTests during the post-coverage verify lifecycle"
    exit 1
fi
