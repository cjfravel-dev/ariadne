#!/usr/bin/env bash

set -euo pipefail

DOCKERFILE="examples/Dockerfile"

assert_contains() {
    local expected="$1"
    if ! grep -Fq "$expected" "$DOCKERFILE"; then
        echo "examples/Dockerfile is missing integrity contract: $expected"
        exit 1
    fi
}

assert_contains 'maven:3.9-eclipse-temurin-11@sha256:1fee93ca227db7e8b8c7c72752ada0f03da6ebab40addd6fe48ac6293424186c'
assert_contains 'eclipse-temurin:11-jdk-jammy@sha256:78d5c3a04afb5ae9750e58a755fc6ce6f4bd843587e40edaa3b7467c5184015e'
assert_contains 'ec5ff678136b1ff981e396d1f7b5dfbf399439c5cb853917e8c954723194857607494a89b7e205fce988ec48b1590b5caeae3b18e1b5db1370c0522b256ff376'
assert_contains 'ce09ca3d701f55357199bf5167b3c7daa729a07c'
assert_contains '9d9d56fcae37f1b3d48d80f8b7eefabd3477569d'
assert_contains 'sha512sum -c -'
assert_contains 'sha1sum -c -'
