#!/usr/bin/env bash

set -euo pipefail

STORAGE_FORMAT=src/main/scala/dev/cjfravel/ariadne/StorageFormat.scala
ARCHITECTURE=docs/contributors/architecture.html
TROUBLESHOOTING=docs/users/troubleshooting.html
PROVENANCE=src/test/resources/fixtures/alpha37/PROVENANCE.txt

version_constant() {
    local name="$1"
    grep -oPm1 "(?<=val $name: Int = )[0-9]+" "$STORAGE_FORMAT"
}

require_text() {
    local file="$1"
    local expected="$2"
    if ! grep -Fq -- "$expected" "$file"; then
        echo "$file is missing migration support policy: $expected"
        exit 1
    fi
}

ALPHA37_VERSION=$(version_constant Alpha37StorageVersion)
FILE_SIZE_VERSION=$(version_constant FileSizeStorageVersion)
CURRENT_VERSION=$(version_constant CurrentStorageVersion)
PROJECT_VERSION=$(grep -oPm1 "(?<=<version>)[^<]+" pom.xml)

require_text "$ARCHITECTURE" "data-storage-version=\"$ALPHA37_VERSION\""
require_text "$ARCHITECTURE" "data-storage-version=\"$FILE_SIZE_VERSION\""
require_text "$ARCHITECTURE" "data-storage-version=\"$CURRENT_VERSION\""
require_text "$ARCHITECTURE" "<code>0.0.1-alpha-37</code> through <code>0.0.1-alpha44</code>"
require_text "$ARCHITECTURE" "<code>0.1.0-beta</code> through <code>$PROJECT_VERSION</code>"
require_text "$ARCHITECTURE" "0.0.1-alpha-37 through $PROJECT_VERSION"
require_text "$ARCHITECTURE" "Earlier than <code>0.0.1-alpha-37</code>"
require_text "$ARCHITECTURE" "backfills file sizes, normalizes exploded aliases, verifies the result, then records v3"
require_text "$ARCHITECTURE" "migration checkpoints"
require_text "$ARCHITECTURE" "Fails before physical or metadata writes"
require_text "$TROUBLESHOOTING" "UnsupportedStorageFormatVersionException"
require_text "$TROUBLESHOOTING" "UnsupportedMetadataVersionException"
require_text "$TROUBLESHOOTING" "storage_format_version"
require_text "$TROUBLESHOOTING" "0.0.1-alpha-37</code> through <code>$PROJECT_VERSION"
require_text "$TROUBLESHOOTING" "Remove and rebuild the index from source data"
require_text "$TROUBLESHOOTING" "The version is not advanced on failure"
require_text "$TROUBLESHOOTING" "does not silently rebuild"
require_text "$PROVENANCE" "tag 0.0.1-alpha-37"
