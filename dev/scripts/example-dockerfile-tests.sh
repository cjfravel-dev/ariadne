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
        echo "$DOCKERFILE is missing integrity contract: $expected"
        exit 1
    fi
}

assert_contains 'maven:3.9-eclipse-temurin-17@sha256:1ed5d1f54416b706707b4f3238f63a20bb06aab27c6d240090a2bb9ad895ed45'
assert_contains 'eclipse-temurin:17-jdk-jammy@sha256:723151f3fc88ca2060153ee08ab8dbbea7983d6ed6f2622fe440acf178737c94'
assert_contains 'ec5ff678136b1ff981e396d1f7b5dfbf399439c5cb853917e8c954723194857607494a89b7e205fce988ec48b1590b5caeae3b18e1b5db1370c0522b256ff376'
assert_contains '088e187da689a347a6a8556dcb22318e3dfcfb995d807f5e2c19b4d0a7ee9499'
assert_contains '4dcc179fc4076bda5060a4038f979c53e1f5916cf04971e28f9441db390763c7'
assert_contains 'sha512sum -c -'
assert_contains 'sha256sum -c -'
