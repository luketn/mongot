#!/usr/bin/env bash

set -eu

BAZELISK_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export BAZELISK_VERSION="v1.26.0"

if [[ "$OSTYPE" == "linux"* ]]; then
    export BAZELISK_PLATFORM="linux"
elif [[ "$OSTYPE" == "darwin"* ]]; then
    export BAZELISK_PLATFORM="darwin"
else
    echo "Unsupported OS"
    exit 1
fi

case $(uname -m) in
  x86_64)
    BAZELISK_PLATFORM+="-amd64"
    ;;
  aarch64 | arm64)
    BAZELISK_PLATFORM+="-arm64"
    ;;
   *)
    echo "Unsupported CPU architecture"
    exit 1
    ;;
esac

export BAZELISK_PATH="${BAZELISK_DIR}/bazelisk-${BAZELISK_PLATFORM}-${BAZELISK_VERSION}"
