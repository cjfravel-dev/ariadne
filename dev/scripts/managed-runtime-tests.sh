#!/usr/bin/env bash

set -euo pipefail

profile_xml() {
    local profile="$1"
    sed -n "/<id>$profile<\\/id>/,/<\\/profile>/p" pom.xml
}

require_text() {
    local file="$1"
    local expected="$2"
    if [[ ! -f "$file" ]]; then
        echo "Managed-runtime contract file is missing: $file"
        exit 1
    fi
    if ! grep -Fq -- "$expected" "$file"; then
        echo "$file is not aligned with managed runtimes: $expected"
        exit 1
    fi
}

SPARK35=$(profile_xml spark35)
SPARK41=$(profile_xml spark41)

for expected in \
    '<jdk>[11,12)</jdk>' \
    '<scala.version>2.12.17</scala.version>' \
    '<spark.version>3.5.5</spark.version>' \
    '<delta.version>3.2.1</delta.version>' \
    '<log4j.version>2.19.0</log4j.version>'; do
    if ! grep -Fq -- "$expected" <<<"$SPARK35"; then
        echo "spark35 profile is not aligned with Synapse: $expected"
        exit 1
    fi
done

for expected in \
    '<scala.version>2.13.17</scala.version>' \
    '<spark.version>4.1.1</spark.version>' \
    '<delta.version>4.1.0</delta.version>' \
    '<log4j.version>2.24.3</log4j.version>' \
    '<arg>-release:21</arg>'; do
    if ! grep -Fq -- "$expected" <<<"$SPARK41"; then
        echo "spark41 profile is not aligned with Fabric: $expected"
        exit 1
    fi
done

require_text .github/workflows/ci.yml 'Set up Java 11'
require_text .github/workflows/ci.yml 'java-version: 11'
require_text .github/workflows/ci.yml 'Set up Java 21'
require_text .github/workflows/ci.yml 'java-version: 21'
require_text .github/workflows/publish.yml 'Set up Java 11 and release credentials'
require_text .github/workflows/publish.yml 'java-version: "11"'
require_text .github/workflows/publish.yml 'Set up Java 21 and release credentials'
require_text .github/workflows/publish.yml 'java-version: "21"'
require_text examples/Dockerfile 'eclipse-temurin:11-jdk-jammy'
require_text README.md 'Scala 2.12, Java 11'
require_text README.md 'Scala 2.13, Java 21'
require_text README.md 'ariadne-spark41_2.13'
require_text docs/users/getting-started.html 'Scala 2.12, Java 11'
require_text docs/users/getting-started.html 'Scala 2.13, Java 21'
require_text docs/users/getting-started.html 'ariadne-spark41_2.13'
require_text docs/contributors/index.html 'Java 11'
require_text docs/contributors/index.html 'Java 21'
require_text .github/copilot-instructions.md 'Java 11 is required'
require_text .github/copilot-instructions.md 'ariadne-spark41_2.13'
require_text .github/ISSUE_TEMPLATE/bug.yml 'Scala 2.12.17, Java 11'

for dependency in \
    'org.apache.spark:*' \
    'io.delta:*' \
    'org.scala-lang:*' \
    'org.apache.logging.log4j:*' \
    'com.fasterxml.jackson.*:*'; do
    require_text .github/dependabot.yml "dependency-name: \"$dependency\""
done
