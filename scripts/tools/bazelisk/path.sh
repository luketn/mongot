#!/usr/bin/env bash

set -eu

BAZELISK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source ${BAZELISK_DIR}/version.sh
echo "${BAZELISK_PATH}"
