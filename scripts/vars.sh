#!/usr/bin/env bash

SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export MONGOT_PATH="${SCRIPTS_DIR}/.."

source "${SCRIPTS_DIR}/tools/tools.sh"

# If there are any more local variables (e.g. BAZELISK_HOME on evergreen) that
# need to be set, source them.
LOCAL_VARS_PATH="${SCRIPTS_DIR}/local-vars.sh"
if [[ -f ${LOCAL_VARS_PATH} ]]; then
  source ${LOCAL_VARS_PATH}
fi
