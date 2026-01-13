#!/usr/bin/env bash

set -eu

BAZELISK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source ${BAZELISK_DIR}/version.sh

if [[ -f ${BAZELISK_PATH} ]]; then
  # bazelisk already installed, nothing to do
  exit 0
fi

>&2 echo "Installing bazelisk"

BAZELISK_RELEASE_URL="https://github.com/bazelbuild/bazelisk/releases/download/${BAZELISK_VERSION}/bazelisk-${BAZELISK_PLATFORM}"

curl --fail \
  --silent \
  --show-error \
  --location \
  --retry 3 \
  -o ${BAZELISK_PATH} \
  ${BAZELISK_RELEASE_URL}

chmod +x ${BAZELISK_PATH}
