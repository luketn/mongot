#!/usr/bin/env bash

set -e

BAZEL_STARTUP_ARGS=${BAZEL_STARTUP_ARGS:=""}

set -u

BAZELISK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${BAZELISK_DIR}/version.sh"
${BAZELISK_DIR}/install.sh

# Retry wrapper for Bazelisk to handle GitHub 502 errors when downloading Bazel binary
# This complements the tools/bazel hook which handles dependency download retries
MAX_RETRIES=5
attempt=1

while [ $attempt -le $MAX_RETRIES ]; do
  # Disable exit-on-error temporarily to capture exit code
  set +e
  ${BAZELISK_PATH} ${BAZEL_STARTUP_ARGS} ${@}
  exit_code=$?
  set -e

  if [ $exit_code -eq 0 ]; then
    # Success - exit with success code
    exit 0
  fi

  if [ $attempt -eq $MAX_RETRIES ]; then
    # Final attempt failed - exit with the error code
    echo >&2 "Bazelisk: All $MAX_RETRIES attempts failed"
    exit $exit_code
  fi

  # Calculate exponential backoff (2s, 4s, 6s, 8s)
  sleep_time=$((attempt * 2))
  echo >&2 "Bazelisk: Attempt $attempt failed (exit code: $exit_code), retrying in ${sleep_time}s..."
  sleep "$sleep_time"

  attempt=$((attempt + 1))
done
