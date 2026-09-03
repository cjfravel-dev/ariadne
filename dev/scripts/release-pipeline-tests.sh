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

assert_plugin_version() {
    local artifact_id="$1"
    local expected_version="$2"
    local plugin_xml
    plugin_xml=$(awk \
        "/<artifactId>$artifact_id<\\/artifactId>/,/<\\/plugin>/" \
        pom.xml)
    if ! grep -Fq "<version>$expected_version</version>" <<<"$plugin_xml"; then
        echo "pom.xml does not configure $artifact_id at version $expected_version"
        exit 1
    fi
}

assert_file .github/workflows/publish.yml
assert_file docs/contributors/releasing.html
assert_file .mvn/wrapper/maven-wrapper.properties
assert_file mvnw
assert_contains mvnw "HOME is unset and MAVEN_USER_HOME is not set"

assert_contains .github/workflows/publish.yml "types: [published]"
assert_contains .github/workflows/publish.yml "environment: maven-central"
assert_contains .github/workflows/publish.yml "MAVEN_GPG_PASSPHRASE"
assert_contains .github/workflows/publish.yml "Ariadne Release <ariadne-releases@cjfravel.dev>"
assert_contains .github/workflows/publish.yml "-Pspark35"
assert_contains .github/workflows/publish.yml "-Pspark41"
assert_contains .github/workflows/publish.yml "-Dcentral.autoPublish=false"
assert_contains .github/workflows/publish.yml "-Dcentral.waitUntil=validated"

# Staging skips test execution because the tagged commit was already tested by CI on main. That tradeoff is only sound
# while the provenance gate runs first, and -DskipTests also suppresses the governance scripts bound to the test phase
# (exec-maven-plugin inherits <skip>${skipTests}</skip>), so the workflow must run them explicitly. All three pieces
# must be kept together.
assert_contains .github/workflows/publish.yml "-DskipTests"
assert_contains .github/workflows/publish.yml "Verify the release commit passed CI"
assert_contains .github/workflows/publish.yml "commits/\$sha/check-runs"
# A required check landing beyond the first page would read as missing and stop a release that should have gone ahead.
assert_contains .github/workflows/publish.yml "--paginate --slurp"
assert_contains .github/workflows/publish.yml "Build & Test (Spark 3.5)"
assert_contains .github/workflows/publish.yml "Build & Test (Spark 4.1)"
assert_contains .github/workflows/publish.yml "checks: read"
# The gate reads check runs and then the job behind each one. Declaring permissions explicitly sets every unlisted
# scope to none, so both scopes have to be present or the gate fails on a release that should have been allowed.
assert_contains .github/workflows/publish.yml "actions: read"
assert_contains .github/workflows/publish.yml "dev/scripts/run-governance-checks.sh"
# The gate must refuse the release while any attempt is unfinished, and otherwise read the newest attempt by start
# time. A re-run adds a second check run with the same name, so reading only one attempt could accept a stale success.
# Failing closed on an unfinished attempt keeps that safe without depending on how a pending run fills in timestamps.
assert_contains .github/workflows/publish.yml 'any(.[]; .status != "completed")'
# The gate reads the conclusion of the build step inside each CI job, because a job can report success while its build
# steps were skipped. It names those steps literally, so a rename in ci.yml would break the release. Check the names
# still resolve.
assert_ci_step_exists() {
    local step="$1"
    if ! grep -Fq -- "- name: $step" .github/workflows/ci.yml; then
        echo ".github/workflows/publish.yml gates on CI step \"$step\", which no longer exists in .github/workflows/ci.yml"
        exit 1
    fi
}

while IFS= read -r step; do
    assert_ci_step_exists "$step"
done < <(grep -oE '^ *"Build & Test \(Spark [0-9.]+\)\|[^"]+"' .github/workflows/publish.yml | sed 's/.*|//; s/"$//')

assert_contains .github/workflows/publish.yml "actions/jobs/"
# The job behind a check run is looked up by the check run's own id. Confirm the job's name before trusting its
# steps, so a mismatch fails the release instead of judging some other job.
assert_contains .github/workflows/publish.yml 'if [[ "$job_name" != "$name" ]]'
assert_contains .github/workflows/publish.yml "set +e"
assert_contains .github/workflows/publish.yml "state35=UNKNOWN"
assert_contains .github/workflows/publish.yml "state41=UNKNOWN"
assert_contains .github/workflows/publish.yml "|| state35=UNKNOWN"
assert_contains .github/workflows/publish.yml "|| state41=UNKNOWN"
assert_contains .github/workflows/publish.yml "cleanup_failed=0"
assert_contains .github/workflows/publish.yml "publish_failed=0"
assert_contains .github/workflows/publish.yml 'if [[ "$state35" == "VALIDATED" ]]'
assert_contains .github/workflows/publish.yml 'if [[ "$state41" == "VALIDATED" ]]'
assert_contains .github/workflows/publish.yml "publisher/deployment/"
assert_contains .github/workflows/publish.yml "authorization_header="
assert_contains .github/workflows/publish.yml '--header "$authorization_header"'
assert_contains .github/workflows/publish.yml "Final cleanup after a release failure"
assert_contains .github/workflows/publish.yml "--retry-max-time 30"
assert_contains .github/workflows/publish.yml "ariadne-spark35_2.12"
assert_contains .github/workflows/publish.yml "ariadne-spark41_2.13"
assert_contains .github/workflows/publish.yml 'cd "$RUNNER_TEMP"'
assert_contains .github/workflows/publish.yml '"$GITHUB_WORKSPACE/mvnw"'
assert_not_contains .github/workflows/publish.yml "gpg-passphrase:"
assert_not_contains .github/workflows/publish.yml "gpg.pinentryMode"

# The GPG key is imported into the runner keyring once and persists across the job. Importing it in more than one
# setup-java step registers duplicate post-job cleanups that race to delete the key, failing the run with gpg exit 2
# even though the release itself succeeds. Keep exactly one gpg-private-key import.
gpg_import_count=$(grep -c "gpg-private-key:" .github/workflows/publish.yml || true)
if [[ "$gpg_import_count" -ne 1 ]]; then
    echo ".github/workflows/publish.yml must import gpg-private-key exactly once (found $gpg_import_count)"
    exit 1
fi

assert_contains pom.xml "<central.autoPublish>false</central.autoPublish>"
assert_contains pom.xml "<central.waitUntil>validated</central.waitUntil>"
assert_contains pom.xml "<project.build.outputTimestamp>"
assert_plugin_version central-publishing-maven-plugin 0.11.0
assert_not_contains pom.xml "<autoPublish>true</autoPublish>"

assert_contains docs/contributors/releasing.html "both deployments reach the validated state"
assert_contains docs/contributors/releasing.html "publishes neither artifact"

echo "Release pipeline contracts passed."
