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

assert_contains pom.xml '<artifactId>maven-enforcer-plugin</artifactId>'
assert_contains pom.xml '<dependencyConvergence'
assert_contains pom.xml '<phase>validate</phase>'
assert_contains pom.xml 'dev/scripts/dependency-policy-tests.sh'
assert_contains .github/dependabot.yml 'package-ecosystem: "maven"'
assert_contains .github/dependabot.yml 'spark-delta'
assert_contains .github/dependabot.yml 'shaded-runtime'
assert_contains .github/dependabot.yml 'org.apache.spark:*'
assert_contains .github/dependabot.yml 'io.delta:*'
assert_contains .github/dependabot.yml 'com.fasterxml.jackson.*:*'
assert_contains .github/dependabot.yml 'org.scala-lang:*'
