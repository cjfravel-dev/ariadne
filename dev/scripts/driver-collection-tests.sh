#!/usr/bin/env bash

set -euo pipefail

FILE_LIST=src/main/scala/dev/cjfravel/ariadne/FileList.scala
INDEX_BUILD=src/main/scala/dev/cjfravel/ariadne/IndexBuildOperations.scala
MIGRATION_METHOD=$(sed -n '/private def migrateFileSizeColumns/,/private def verifyFileSizeColumns/p' "$INDEX_BUILD")

if grep -Fq '.collect' "$FILE_LIST"; then
    echo "FileList duplicate detection must not collect all tracked filenames"
    exit 1
fi

if ! grep -Fq '.mapPartitions' <<<"$MIGRATION_METHOD"; then
    echo "file_size migration must resolve source sizes on executors"
    exit 1
fi

if grep -Fq '.toLocalIterator' <<<"$MIGRATION_METHOD"; then
    echo "file_size migration must not stream Spark partitions through driver memory"
    exit 1
fi

if ! grep -Fq 'checkHeartbeat()' <<<"$MIGRATION_METHOD"; then
    echo "file_size migration must check lock ownership between batch writes"
    exit 1
fi
