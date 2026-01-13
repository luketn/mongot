#!/usr/bin/env bash

set -eu

TOOLS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

function bazel {
    "${TOOLS_DIR}/bazelisk/run.sh" ${@}
}

function log {
  echo "$@" 1>&2
}
