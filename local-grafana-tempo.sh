#!/usr/bin/env bash
set -euo pipefail

WORKDIR="$(pwd)"
AGENT_DIR="${WORKDIR}/otel-agent"
AGENT_JAR="${AGENT_DIR}/opentelemetry-javaagent.jar"
AGENT_VERSION="2.5.0"

# Create agent directory if it doesn't exist
mkdir -p "${AGENT_DIR}"

# Download agent if not present
if [ ! -f "${AGENT_JAR}" ]; then
  echo "Downloading OpenTelemetry Java Agent v${AGENT_VERSION}..."
  curl -L -o "${AGENT_JAR}" \
    "https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v${AGENT_VERSION}/opentelemetry-javaagent.jar"
  echo "✓ Agent downloaded to ${AGENT_JAR}"
fi

# Start with clean Tempo container if it already exists
docker rm -f tempo >/dev/null 2>&1 || true

# Create minimal Tempo config for single-instance development
TEMPO_CONFIG="${WORKDIR}/local-tempo-local.yml"
cat > "${TEMPO_CONFIG}" <<'EOF'
server:
  http_listen_port: 3200

multitenancy_enabled: false

distributor:
  receivers:
    otlp:
      protocols:
        grpc:
          endpoint: 0.0.0.0:4317
        http:
          endpoint: 0.0.0.0:4318

ingester:
  max_block_duration: 5m
  lifecycler:
    ring:
      kvstore:
        store: inmemory
      replication_factor: 1
    num_tokens: 128

storage:
  trace:
    backend: local
    local:
      path: /tmp/tempo-blocks
EOF

# Start Grafana Tempo with OTLP receiver
docker run -d --name tempo \
  -p 3200:3200 \
  -p 4317:4317 \
  -p 4318:4318 \
  -v "${TEMPO_CONFIG}:/etc/tempo-config.yml:ro" \
  --add-host=host.docker.internal:host-gateway \
  grafana/tempo:latest \
  -config.file=/etc/tempo-config.yml

echo ""
echo "✓ Grafana Tempo is running on http://localhost:3200"
echo ""
echo "To monitor MongoT with Tempo, run it with the OpenTelemetry Java Agent:"
echo ""
echo "java -javaagent:${AGENT_JAR} \\"
echo "  -Dotel.traces.exporter=otlp \\"
echo "  -Dotel.exporter.otlp.endpoint=http://127.0.0.1:4318 \\"
echo "  -Dotel.service.name=mongot \\"
echo "  -Dotel.logs.exporter=none \\"
echo "  <your-mongot-jar-or-command>"
echo ""
echo "For IntelliJ Bazel run config, use these Bazel parameters:"
echo "--jvmopt=-javaagent:${AGENT_JAR} --jvmopt=-Dotel.traces.exporter=otlp --jvmopt=-Dotel.exporter.otlp.endpoint=http://127.0.0.1:4318 --jvmopt=-Dotel.service.name=mongot --jvmopt=-Dotel.logs.exporter=none"
echo ""
echo "To integrate Tempo with your existing Grafana instance:"
echo "  1. Open Grafana at http://localhost:3000"
echo "  2. Go to Connections > Data Sources"
echo "  3. Click 'Add data source' and select 'Tempo'"
echo "  4. Set URL to http://tempo:3200"
echo "  5. Save"
echo ""
echo "Then traces will appear in Grafana alongside your metrics!"
echo ""
