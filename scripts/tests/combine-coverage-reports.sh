#!/usr/bin/env bash

set -eu

# define bazel function
JAVA_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${JAVA_DIR}/../vars.sh"

genhtml --ignore-errors inconsistent,corrupt,unsupported "$(bazel info output_path)/_coverage/_coverage_report.dat" \
  --output-directory "$(bazel info output_path)/_coverage/coverage-report"

