#!/usr/bin/env bash

set -eu
set -o pipefail # since we use pipe, fail if any command of a pipe fails

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${DIR}/../../scripts/vars.sh"

cd "${MONGOT_PATH}"

log "Generating a BUILD file for a pom.xml rule with all our targets"
bazel query 'kind("jvm_import", @maven//:all)'  |
    bazel run //scripts/java/pom:generate_pom_rule > ./bazel/java/generated_pom/BUILD

log "Running the pom.xml rule"
bazel build //bazel/java/generated_pom:pom

log "Deleting BUILD file"
rm -f ./bazel/java/generated_pom/BUILD

BAZEL_OUTPUT_FILE="$(bazel info bazel-bin)/bazel/java/generated_pom/pom.xml"
OUTPUT_FILE="./bazel/java/generated_pom/pom.xml"

log "Copying to ${OUTPUT_FILE}"
# open write permissions on pom.xml as it is going into git
chmod 644 "${BAZEL_OUTPUT_FILE}"
cp "${BAZEL_OUTPUT_FILE}" "${OUTPUT_FILE}"

