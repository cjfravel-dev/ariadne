#!/usr/bin/env bash

set -euo pipefail

assert_contains() {
    local file="$1"
    local expected="$2"
    if [[ ! -f "$file" ]] || ! grep -Fq -- "$expected" "$file"; then
        echo "$file is missing dependency policy: $expected"
        exit 1
    fi
}

assert_text() {
    local text="$1"
    local expected="$2"
    if ! grep -Fq -- "$expected" <<<"$text"; then
        echo "Maven Enforcer configuration is missing dependency policy: $expected"
        exit 1
    fi
}

ENFORCER_PLUGIN=$(sed -n '/<artifactId>maven-enforcer-plugin<\/artifactId>/,/<\/plugin>/p' pom.xml)
assert_text "$ENFORCER_PLUGIN" '<artifactId>maven-enforcer-plugin</artifactId>'
assert_text "$ENFORCER_PLUGIN" '<dependencyConvergence'
assert_text "$ENFORCER_PLUGIN" '<phase>validate</phase>'
assert_contains pom.xml 'dev/scripts/dependency-policy-tests.sh'
assert_contains .github/dependabot.yml 'package-ecosystem: "maven"'
assert_contains .github/dependabot.yml 'spark-delta'
assert_contains .github/dependabot.yml 'shaded-runtime'
assert_contains .github/dependabot.yml 'org.apache.spark:*'
assert_contains .github/dependabot.yml 'io.delta:*'
assert_contains .github/dependabot.yml 'com.fasterxml.jackson.*:*'
assert_contains .github/dependabot.yml 'org.scala-lang:*'
