#!/bin/bash

set -euo pipefail

WORKFLOW=".github/workflows/ci.yml"

if grep -Eq 'mvn .*verify.*scoverage:report|mvn .*scoverage:report.*verify' "$WORKFLOW"; then
    echo "CI must not combine verify and scoverage:report in one invocation because it runs tests twice"
    exit 1
fi

grep -Fq 'mvn -B -q scoverage:report -Dgpg.skip=true' "$WORKFLOW"
grep -Fq 'mvn -B -q verify -DskipTests -Dgpg.skip=true' "$WORKFLOW"
grep -Fq 'MAVEN_OPTS: -Xmx8g -XX:+UseG1GC' "$WORKFLOW"
