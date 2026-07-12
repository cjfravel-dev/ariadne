#!/usr/bin/env bash

set -euo pipefail

FILE_LIST=src/main/scala/dev/cjfravel/ariadne/FileList.scala
INDEX_BUILD=src/main/scala/dev/cjfravel/ariadne/IndexBuildOperations.scala

if grep -Fq '.collect().toSet' "$FILE_LIST"; then
    echo "FileList duplicate detection must not collect all tracked filenames"
    exit 1
fi

if ! grep -A45 'private def migrateFileSizeColumns' "$INDEX_BUILD" | grep -Fq '.mapPartitions'; then
    echo "file_size migration must resolve source sizes on executors"
    exit 1
fi

if grep -A60 'private def migrateFileSizeColumns' "$INDEX_BUILD" | grep -Fq '.toLocalIterator()'; then
    echo "file_size migration must not stream Spark partitions through driver memory"
    exit 1
fi

if ! grep -A60 'private def migrateFileSizeColumns' "$INDEX_BUILD" | grep -Fq 'checkHeartbeat()'; then
    echo "file_size migration must check lock ownership between batch writes"
    exit 1
fi
