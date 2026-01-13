#!/usr/bin/env bash
# Accepts one argument refering to a mongot's number
# For instance: ./scripts/metrics/export-ftdc-from-docker.sh 1
set -eu

METRICS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${METRICS_DIR}/../vars.sh"

if [ $# -eq  0 ]
then
  echo "TARGET_MONGOT missing, using '1'."
  TARGET_MONGOT=1
else
  TARGET_MONGOT=${1}
fi

TMP_DIR=$(mktemp -d "/tmp/mongot.XXXXXX")

MONGOT_CONTAINER_NAME="docker-mongot"${TARGET_MONGOT}"-1"
echo "Copying data from $MONGOT_CONTAINER_NAME"
docker cp $MONGOT_CONTAINER_NAME:/var/lib/mongot/diagnostic.data $TMP_DIR

echo "Metrics exported to ${TMP_DIR}/diagnostic.data"
echo "You can now run:"
echo "   t2 ${TMP_DIR}/diagnostic.data"
