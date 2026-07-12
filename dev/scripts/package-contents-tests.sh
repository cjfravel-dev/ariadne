#!/usr/bin/env bash

set -euo pipefail

JAR_PATH=${1:-}

if [[ -z "$JAR_PATH" || ! -f "$JAR_PATH" ]]; then
    echo "Packaged Ariadne JAR is missing: $JAR_PATH"
    exit 1
fi

CONTENTS=$(mktemp)
trap 'rm -f "$CONTENTS"' EXIT
jar tf "$JAR_PATH" >"$CONTENTS"

require_entry() {
    if ! grep -q "^$1" "$CONTENTS"; then
        echo "$JAR_PATH is missing required packaged namespace: $1"
        exit 1
    fi
}

reject_entry() {
    if grep -q "^$1" "$CONTENTS"; then
        echo "$JAR_PATH contains forbidden packaged namespace: $1"
        exit 1
    fi
}

require_entry 'dev/cjfravel/ariadne/shaded/gson/'
require_entry 'dev/cjfravel/ariadne/shaded/guava/'

reject_entry 'com/google/common/'
reject_entry 'com/google/gson/'
reject_entry 'com/fasterxml/jackson/'
reject_entry 'io/delta/'

if grep '^org/apache/spark/' "$CONTENTS" |
    grep -Ev '^org/apache/spark/$|^org/apache/spark/sql/$|^org/apache/spark/sql/AriadneInternalHelper(\$)?\.class$'; then
    echo "$JAR_PATH contains Spark classes outside Ariadne's internal compatibility helper."
    exit 1
fi
