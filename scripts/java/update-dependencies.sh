#!/usr/bin/env bash
# This script generates a pom.xml for all maven dependencies included by any maven_install
# This is used by SNYK and Dependabot to detect vulnerabilities
set -eu

JAVA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${JAVA_DIR}/../vars.sh"

cd "${MONGOT_PATH}"
bazel run @unpinned_maven//:pin

"${JAVA_DIR}/update-pom-file.sh"
