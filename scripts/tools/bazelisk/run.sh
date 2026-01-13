#!/usr/bin/env bash

set -e

BAZEL_STARTUP_ARGS=${BAZEL_STARTUP_ARGS:=""}

set -u

BAZELISK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${BAZELISK_DIR}/version.sh"
${BAZELISK_DIR}/install.sh

${BAZELISK_PATH} ${BAZEL_STARTUP_ARGS} ${@}
