#!/bin/bash

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
TEST_ROOT=$(mktemp -d)
trap 'rm -rf "$TEST_ROOT"' EXIT

mkdir -p "$TEST_ROOT/dev/scripts" "$TEST_ROOT/docs/users"
cp "$REPO_ROOT/dev/scripts/readme-has-version.sh" "$TEST_ROOT/dev/scripts/"

cat >"$TEST_ROOT/pom.xml" <<'EOF'
<project>
  <artifactId>ariadne-spark${spark.suffix}_${scala.binary.version}</artifactId>
  <version>1.2.3</version>
  <profiles>
    <profile>
      <id>spark35</id>
      <properties>
        <scala.binary.version>2.12</scala.binary.version>
        <spark.suffix>35</spark.suffix>
      </properties>
    </profile>
    <profile>
      <id>spark41</id>
      <properties>
        <scala.binary.version>2.13</scala.binary.version>
        <spark.suffix>41</spark.suffix>
      </properties>
    </profile>
  </profiles>
</project>
EOF
cat >"$TEST_ROOT/README.md" <<'EOF'
<artifactId>ariadne-spark35_2.12</artifactId>
<version>1.2.3</version>
<artifactId>ariadne-spark41_2.13</artifactId>
<version>1.2.3</version>
EOF
cat >"$TEST_ROOT/docs/users/getting-started.html" <<'EOF'
&lt;artifactId&gt;ariadne-spark35_2.12&lt;/artifactId&gt;
&lt;version&gt;1.2.3&lt;/version&gt;
&lt;artifactId&gt;ariadne-spark41_2.13&lt;/artifactId&gt;
&lt;version&gt;1.2.3&lt;/version&gt;
EOF
echo "## [1.2.2]" >"$TEST_ROOT/CHANGELOG.md"
echo "version: 1.2.3" >"$TEST_ROOT/CITATION.cff"

if (cd "$TEST_ROOT" && bash dev/scripts/readme-has-version.sh >/dev/null); then
  echo "Version check accepted a changelog that omitted the current version"
  exit 1
fi

echo "## [1.2.3]" >"$TEST_ROOT/CHANGELOG.md"
(cd "$TEST_ROOT" && bash dev/scripts/readme-has-version.sh >/dev/null)

echo "## [11.2.30]" >"$TEST_ROOT/CHANGELOG.md"
if (cd "$TEST_ROOT" && bash dev/scripts/readme-has-version.sh >/dev/null); then
  echo "Version check accepted the current version as a substring"
  exit 1
fi

echo "## [1.2.3]" >"$TEST_ROOT/CHANGELOG.md"
sed -i 's/ariadne-spark41_2.13/ariadne-spark40_2.13/g' "$TEST_ROOT/README.md"
if (cd "$TEST_ROOT" && bash dev/scripts/readme-has-version.sh >/dev/null); then
  echo "Version check accepted documentation with the wrong Spark 4 artifact"
  exit 1
fi

sed -i 's/ariadne-spark40_2.13/ariadne-spark41_2.13/g' "$TEST_ROOT/README.md"
sed -i '0,/1.2.3/s//1.2.2/' "$TEST_ROOT/README.md"
echo "Current release: 1.2.3" >>"$TEST_ROOT/README.md"
if (cd "$TEST_ROOT" && bash dev/scripts/readme-has-version.sh >/dev/null); then
  echo "Version check accepted an artifact paired with a stale version"
  exit 1
fi

cp "$TEST_ROOT/pom.xml" "$TEST_ROOT/pom.valid.xml"
sed -i '/<version>1.2.3<\/version>/d' "$TEST_ROOT/pom.xml"
OUTPUT=$(cd "$TEST_ROOT" && bash dev/scripts/readme-has-version.sh 2>&1 || true)
if ! grep -Fq "Version not found in pom.xml" <<<"$OUTPUT"; then
  echo "Version check did not report a controlled missing-version error"
  exit 1
fi

cp "$TEST_ROOT/pom.valid.xml" "$TEST_ROOT/pom.xml"
sed -i '/<spark.suffix>35<\/spark.suffix>/d' "$TEST_ROOT/pom.xml"
OUTPUT=$(cd "$TEST_ROOT" && bash dev/scripts/readme-has-version.sh 2>&1 || true)
if ! grep -Fq "Unable to determine artifact coordinates for spark35" <<<"$OUTPUT"; then
  echo "Version check did not report a controlled malformed-profile error"
  exit 1
fi
