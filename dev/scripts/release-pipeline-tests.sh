#!/usr/bin/env bash

set -euo pipefail

assert_file() {
    if [[ ! -f "$1" ]]; then
        echo "Required release file is missing: $1"
        exit 1
    fi
}

assert_contains() {
    local file="$1"
    local expected="$2"
    if ! grep -Fq -- "$expected" "$file"; then
        echo "$file is missing release contract: $expected"
        exit 1
    fi
}

assert_not_contains() {
    local file="$1"
    local rejected="$2"
    if grep -Fq -- "$rejected" "$file"; then
        echo "$file contains forbidden release configuration: $rejected"
        exit 1
    fi
}

assert_file .github/workflows/publish.yml
assert_file docs/contributors/releasing.html
assert_file .mvn/wrapper/maven-wrapper.properties
assert_file mvnw

assert_contains .github/workflows/publish.yml "types: [published]"
assert_contains .github/workflows/publish.yml "environment: maven-central"
assert_contains .github/workflows/publish.yml "MAVEN_GPG_PASSPHRASE"
assert_contains .github/workflows/publish.yml "Ariadne Release <ariadne-releases@cjfravel.dev>"
assert_contains .github/workflows/publish.yml "-Pspark35"
assert_contains .github/workflows/publish.yml "-Pspark41"
assert_contains .github/workflows/publish.yml "-Dcentral.autoPublish=false"
assert_contains .github/workflows/publish.yml "-Dcentral.waitUntil=validated"
assert_contains .github/workflows/publish.yml "set +e"
assert_contains .github/workflows/publish.yml "state35=UNKNOWN"
assert_contains .github/workflows/publish.yml "state41=UNKNOWN"
assert_contains .github/workflows/publish.yml "cleanup_failed=0"
assert_contains .github/workflows/publish.yml "publish_failed=0"
assert_contains .github/workflows/publish.yml 'if [[ "$state35" == "VALIDATED" ]]'
assert_contains .github/workflows/publish.yml 'if [[ "$state41" == "VALIDATED" ]]'
assert_contains .github/workflows/publish.yml "publisher/deployment/"
assert_contains .github/workflows/publish.yml '--header "Authorization: ******"'
assert_contains .github/workflows/publish.yml "ariadne-spark35_2.12"
assert_contains .github/workflows/publish.yml "ariadne-spark41_2.13"
assert_not_contains .github/workflows/publish.yml "gpg-passphrase:"
assert_not_contains .github/workflows/publish.yml "gpg.pinentryMode"

assert_contains pom.xml "<central.autoPublish>false</central.autoPublish>"
assert_contains pom.xml "<central.waitUntil>validated</central.waitUntil>"
assert_contains pom.xml "<project.build.outputTimestamp>"
assert_contains pom.xml "<version>0.11.0</version>"
assert_not_contains pom.xml "<autoPublish>true</autoPublish>"

assert_contains docs/contributors/releasing.html "both deployments reach the validated state"
assert_contains docs/contributors/releasing.html "publishes neither artifact"

echo "Release pipeline contracts passed."
