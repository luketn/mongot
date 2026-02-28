#!/usr/bin/env bash
set -euo pipefail

WORKDIR="$(pwd)"
PASSWORD="mongotPassword"
PASSWORD_FILE="${WORKDIR}/mongot-password"
MONGOD_CONF_FILE="${WORKDIR}/mongod-dev.conf"
MONGOT_DEV_FILE="${WORKDIR}/mongot-dev.yml"
MONGOT_DATA_DIR="${WORKDIR}/mongot-data"
BUILD_DATA_FILE="${WORKDIR}/conf/mongot/production/build-data.properties"
PRODUCTION_BUILD_FILE="${WORKDIR}/conf/mongot/production/BUILD"

# Start with a clean mongod container if it already exists.
docker rm -f mongod >/dev/null 2>&1 || true

cat > "${MONGOD_CONF_FILE}" <<EOM
replication:
  replSetName: rs0
storage:
  dbPath: /data/db
net:
  port: 27017
  bindIp: 0.0.0.0
setParameter:
  searchIndexManagementHostAndPort: host.docker.internal:27028
  mongotHost: host.docker.internal:27028
  useGrpcForSearch: true
EOM

cat > "${MONGOT_DEV_FILE}" <<EOM
syncSource:
  replicaSet:
    hostAndPort: "localhost:27017"
    username: mongotUser
    passwordFile: "${PASSWORD_FILE}"
    authSource: admin
    tls: false
    readPreference: primaryPreferred
storage:
  dataPath: "${MONGOT_DATA_DIR}"
server:
  grpc:
    address: "0.0.0.0:27028"
    tls:
      mode: "disabled"
logging:
  verbosity: INFO
metrics:
   enabled: true
   address: "0.0.0.0:9946"
EOM

mkdir -p "$(dirname "${BUILD_DATA_FILE}")"
cat > "${BUILD_DATA_FILE}" <<EOM
build.label=local
EOM

if ! grep -q "build-data.properties" "${PRODUCTION_BUILD_FILE}"; then
  perl -0pi -e 's/srcs = \["logback.xml"\],/srcs = \["logback.xml", "build-data.properties"\],/' "${PRODUCTION_BUILD_FILE}"
fi

printf "%s" "${PASSWORD}" > "${PASSWORD_FILE}"
chmod 600 "${PASSWORD_FILE}"

rm -rf "${MONGOT_DATA_DIR}"
mkdir -p "${MONGOT_DATA_DIR}"

# Start mongod as a single-node replica set.
docker run -d --name mongod \
  -p 27017:27017 \
  -v "${PASSWORD_FILE}:/etc/mongot-key-file:ro" \
  -v "${MONGOD_CONF_FILE}:/etc/mongod.conf:ro" \
  --add-host=host.docker.internal:host-gateway \
  mongodb/mongodb-community-server:latest \
  --config "/etc/mongod.conf"

sleep 2

# Initiate the replica set and create a search coordinator user.
docker exec mongod mongosh --eval "rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'host.docker.internal:27017'}]})"

docker exec mongod mongosh --eval "
  db.getSiblingDB('admin').createUser({
    user: 'mongotUser',
    pwd: '${PASSWORD}',
    roles: [{role: 'searchCoordinator', db: 'admin'}]
  })
"

docker logs -f mongod
