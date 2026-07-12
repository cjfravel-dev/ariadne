#!/usr/bin/env bash

set -euo pipefail

assert_file() {
    if [[ ! -f "$1" ]]; then
        echo "Required repository file is missing: $1"
        exit 1
    fi
}

assert_contains() {
    local file="$1"
    local expected="$2"
    if ! grep -Eiq "$expected" "$file"; then
        echo "$file is missing required content: $expected"
        exit 1
    fi
}

for file in \
    CODE_OF_CONDUCT.md \
    CITATION.cff \
    .github/ISSUE_TEMPLATE/bug.yml \
    .github/ISSUE_TEMPLATE/feature.yml \
    .github/ISSUE_TEMPLATE/config.yml \
    .github/PULL_REQUEST_TEMPLATE.md; do
    assert_file "$file"
done

assert_contains .github/PULL_REQUEST_TEMPLATE.md 'RED'
assert_contains .github/PULL_REQUEST_TEMPLATE.md 'GREEN'
assert_contains .github/PULL_REQUEST_TEMPLATE.md 'storage|persisted'
assert_contains .github/PULL_REQUEST_TEMPLATE.md 'CHANGELOG'
assert_contains .github/ISSUE_TEMPLATE/bug.yml 'minimal reproduction'
assert_contains .github/ISSUE_TEMPLATE/bug.yml 'Spark version'
assert_contains .github/ISSUE_TEMPLATE/bug.yml 'storage backend'
assert_contains .github/ISSUE_TEMPLATE/config.yml 'security/advisories/new'
assert_contains README.md 'CODE_OF_CONDUCT.md'
assert_contains README.md 'CITATION.cff'
assert_contains CONTRIBUTING.md 'CODE_OF_CONDUCT.md'
assert_contains CITATION.cff 'version: 0.1.4-beta'
