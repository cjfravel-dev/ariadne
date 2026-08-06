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
    if ! grep -Fiq -- "$expected" "$file"; then
        echo "$file is missing required content: $expected"
        exit 1
    fi
}

for file in \
    README.md \
    CONTRIBUTING.md \
    pom.xml \
    CODE_OF_CONDUCT.md \
    CITATION.cff \
    dev/scripts/build-docs-site.sh \
    dev/scripts/clean-api-docs-output.sh \
    dev/scripts/set-version.sh \
    dev/scripts/coverage-policy-tests.sh \
    dev/scripts/dependency-policy-tests.sh \
    dev/scripts/migration-support-policy-tests.sh \
    dev/scripts/managed-runtime-tests.sh \
    dev/scripts/package-contents-tests.sh \
    dev/scripts/release-pipeline-tests.sh \
    docs/contributors/architecture.html \
    docs/users/troubleshooting.html \
    .github/ISSUE_TEMPLATE/bug.yml \
    .github/ISSUE_TEMPLATE/feature.yml \
    .github/ISSUE_TEMPLATE/config.yml \
    .github/PULL_REQUEST_TEMPLATE.md; do
    assert_file "$file"
done

assert_contains .github/PULL_REQUEST_TEMPLATE.md 'RED'
assert_contains .github/PULL_REQUEST_TEMPLATE.md 'GREEN'
assert_contains .github/PULL_REQUEST_TEMPLATE.md 'Persisted storage'
assert_contains .github/PULL_REQUEST_TEMPLATE.md 'CHANGELOG'
assert_contains .github/ISSUE_TEMPLATE/bug.yml 'minimal reproduction'
assert_contains .github/ISSUE_TEMPLATE/bug.yml 'Spark version'
assert_contains .github/ISSUE_TEMPLATE/bug.yml 'storage backend'
assert_contains .github/ISSUE_TEMPLATE/config.yml 'security/advisories/new'
assert_contains README.md 'CODE_OF_CONDUCT.md'
assert_contains README.md 'CITATION.cff'
assert_contains CONTRIBUTING.md 'CODE_OF_CONDUCT.md'
assert_contains dev/scripts/build-docs-site.sh 'target/site/scaladocs'
assert_contains dev/scripts/build-docs-site.sh 'clean-api-docs-output.sh'
assert_contains dev/scripts/build-docs-site.sh '-Pspark35'
assert_contains pom.xml 'clean-api-docs-output'

# The published site is generated, never committed. Committing docs/api would
# reintroduce a large generated diff on every release and let the reference go
# stale between regenerations.
assert_file .github/workflows/pages.yml
assert_contains .github/workflows/pages.yml 'dev/scripts/build-docs-site.sh'
assert_contains .github/workflows/pages.yml 'actions/upload-pages-artifact'
assert_contains .github/workflows/pages.yml 'actions/deploy-pages'
if [[ -e docs/api ]]; then
    echo "docs/api must not be committed; the Pages workflow generates it"
    exit 1
fi
if ! grep -Fq 'target/site/' .gitignore; then
    echo ".gitignore must exclude generated site output under target/site/"
    exit 1
fi

# Documentation pages carry a substitution token so a release never edits them.
assert_contains dev/scripts/build-docs-site.sh '__ARIADNE_VERSION__'
assert_contains dev/scripts/readme-has-version.sh '__ARIADNE_VERSION__'
assert_contains docs/users/getting-started.html '__ARIADNE_VERSION__'
assert_contains pom.xml 'migration-support-policy-tests.sh'
assert_contains docs/contributors/architecture.html '0.0.1-alpha-37'
assert_contains docs/users/troubleshooting.html 'UnsupportedStorageFormatVersionException'
assert_contains docs/users/troubleshooting.html 'storage_format_version'
assert_contains dev/scripts/package-contents-tests.sh 'dev/cjfravel/ariadne/shaded'
assert_contains dev/scripts/package-contents-tests.sh 'com/fasterxml/jackson'
assert_contains README.md 'ariadne-spark35_2.12'
assert_contains README.md 'ariadne-spark41_2.13'
VERSION=$(grep -oPm1 "(?<=<version>)[^<]+" pom.xml)
assert_contains CITATION.cff "version: $VERSION"
