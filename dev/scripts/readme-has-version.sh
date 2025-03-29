#!/bin/bash

# Extract version from pom.xml
VERSION=$(grep -oPm1 "(?<=<version>)[^<]+" pom.xml)

if [[ -z "$VERSION" ]]; then
    echo "Version not found in pom.xml"
    exit 1
fi

# Check if version is in README.md
if grep -q "$VERSION" README.md; then
    echo "Version $VERSION is present in README.md."
else
    echo "Version $VERSION is NOT present in README.md."
    exit 1
fi