#!/usr/bin/env bash
set -euo pipefail

WORKDIR="$(pwd)"
AGENT_DIR="${WORKDIR}/otel-agent"
AGENT_JAR="${AGENT_DIR}/opentelemetry-javaagent.jar"
AGENT_VERSION="2.5.0"
JAEGER_MEMORY_MAX_TRACES="${JAEGER_MEMORY_MAX_TRACES:-50000}"

# ── OpenTelemetry Java Agent ─────────────────────────────────────────
mkdir -p "${AGENT_DIR}"
if [ ! -f "${AGENT_JAR}" ]; then
  echo "Downloading OpenTelemetry Java Agent v${AGENT_VERSION}..."
  curl -L -o "${AGENT_JAR}" \
    "https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v${AGENT_VERSION}/opentelemetry-javaagent.jar"
  echo "✓ Agent downloaded to ${AGENT_JAR}"
fi

# ── Jaeger (distributed tracing) ─────────────────────────────────────
docker rm -f jaeger >/dev/null 2>&1 || true
docker run -d --name jaeger \
  -p 6831:6831/udp \
  -p 6832:6832/udp \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 14250:14250 \
  -p 14268:14268 \
  -p 16686:16686 \
  --add-host=host.docker.internal:host-gateway \
  jaegertracing/all-in-one:latest \
  --memory.max-traces="${JAEGER_MEMORY_MAX_TRACES}"

# ── Prometheus (metrics) ─────────────────────────────────────────────
docker rm -f prometheus >/dev/null 2>&1 || true
docker run -d --name prometheus \
  -p 9090:9090 \
  -v "$(pwd)/local-prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
  --add-host=host.docker.internal:host-gateway \
  prom/prometheus:latest

# ── Grafana (dashboards) ─────────────────────────────────────────────
docker rm -f grafana >/dev/null 2>&1 || true
docker run -d --name grafana \
  -p 3000:3000 \
  -v "$(pwd)/local-grafana-datasource.yml:/etc/grafana/provisioning/datasources/datasource.yml:ro" \
  -v "$(pwd)/local-grafana-dashboards.yml:/etc/grafana/provisioning/dashboards/dashboards.yml:ro" \
  -v "$(pwd)/local-grafana-mongot-dashboard.json:/var/lib/grafana/dashboards/mongot-dashboard.json:ro" \
  --add-host=host.docker.internal:host-gateway \
  grafana/grafana:latest

echo ""
echo "✓ Jaeger     http://localhost:16686"
echo "  Jaeger in-memory trace cap: ${JAEGER_MEMORY_MAX_TRACES}"
echo "✓ Prometheus http://localhost:9090"
echo "✓ Grafana    http://localhost:3000  (admin/admin)"
echo ""
echo "To run MongoT with tracing, run from the repo root:"
echo ""
echo "  bazel build //src/main/java/com/xgen/mongot/community:MongotCommunity"
echo "  JAVABIN=\$(/usr/libexec/java_home -v 21)/bin/java DETAILED_TRACE_SPANS=true \\"
echo "    ./bazel-bin/src/main/java/com/xgen/mongot/community/MongotCommunity \\"
echo "    --jvm_flag=-javaagent:${AGENT_JAR} \\"
echo "    --jvm_flag=-Dotel.traces.exporter=otlp \\"
echo "    --jvm_flag=-Dotel.exporter.otlp.endpoint=http://127.0.0.1:4318 \\"
echo "    --jvm_flag=-Dotel.service.name=mongot \\"
echo "    --jvm_flag=-Dotel.metrics.exporter=none \\"
echo "    --jvm_flag=-Dotel.logs.exporter=none \\"
echo "    --config mongot-dev.yml --internalListAllIndexesForTesting"
echo ""
