#!/usr/bin/env bash

set -euo pipefail

DOCKERFILE="examples/Dockerfile"

if [[ ! -f "$DOCKERFILE" ]]; then
    echo "Example Dockerfile not found: $DOCKERFILE"
    exit 1
fi

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
assert_contains '088e187da689a347a6a8556dcb22318e3dfcfb995d807f5e2c19b4d0a7ee9499'
assert_contains '4dcc179fc4076bda5060a4038f979c53e1f5916cf04971e28f9441db390763c7'
assert_contains 'sha512sum -c -'
assert_contains 'sha256sum -c -'
