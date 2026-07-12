#!/usr/bin/env bash

set -euo pipefail

SPARK_COMPAT=${1:-spark3}

if [[ "$SPARK_COMPAT" != "spark3" ]]; then
    echo "API documentation drift is checked against the canonical Spark 3.5 build."
    exit 0
fi

if [[ ! -d target/site/scaladocs ]]; then
    echo "Generated API documentation is missing: target/site/scaladocs"
    exit 1
fi

if ! diff -qr docs/api target/site/scaladocs; then
    echo "docs/api does not match the generated Spark 3.5 Scaladoc."
    echo "Run dev/scripts/build-api-docs.sh and commit the result."
    exit 1
fi
