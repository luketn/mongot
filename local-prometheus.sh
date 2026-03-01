#!/usr/bin/env bash
set -euo pipefail

echo "Starting MongoT observability stack (mongod → tempo → grafana+prometheus)..."
echo ""

# Ensure mongod is running first
if ! docker ps --format '{{.Names}}' | grep -w mongod >/dev/null 2>&1; then
  echo "⚠ mongod not running. Start it first with: ./local-mongod.sh"
  exit 1
fi

# Start with a clean prometheus container if it already exists.
docker rm -f prometheus >/dev/null 2>&1 || true

# Start prometheus with a config to point at MongoT
docker run -d --name prometheus \
  -p 9090:9090 \
  -v "$(pwd)/local-prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
  --add-host=host.docker.internal:host-gateway \
  prom/prometheus:latest

echo "✓ Prometheus started (waiting for Tempo...)"
sleep 2

# Ensure tempo is running
if ! docker ps --format '{{.Names}}' | grep -w tempo >/dev/null 2>&1; then
  echo "⚠ Tempo not running. Start it first with: ./local-grafana-tempo.sh"
  exit 1
fi

echo "✓ Tempo is running"
sleep 2

# Start with a clean grafana container if it already exists.
docker rm -f grafana >/dev/null 2>&1 || true

# Start Grafana with provisioning + dashboards
mkdir -p "$(pwd)/grafana/provisioning"
docker run -d --name grafana \
  -p 3000:3000 \
  -v "$(pwd)/local-grafana-datasource.yml:/etc/grafana/provisioning/datasources/datasource.yml:ro" \
  -v "$(pwd)/local-grafana-dashboards.yml:/etc/grafana/provisioning/dashboards/dashboards.yml:ro" \
  -v "$(pwd)/local-grafana-mongot-dashboard.json:/var/lib/grafana/dashboards/mongot-dashboard.json:ro" \
  -v "$(pwd)/local-grafana-tempo-dashboard.json:/var/lib/grafana/dashboards/tempo-dashboard.json:ro" \
  --add-host=host.docker.internal:host-gateway \
  grafana/grafana:latest

echo "✓ Grafana started"
echo ""
echo "================================"
echo "Observability Stack Running"
echo "================================"
echo "✓ Prometheus: http://localhost:9090"
echo "✓ Tempo:      http://localhost:3200"
echo "✓ Grafana:    http://localhost:3000 (admin/admin)"
echo ""
echo "Next: Configure your MongoT run with these Bazel parameters:"
echo "--jvmopt=-javaagent:$(pwd)/otel-agent/opentelemetry-javaagent.jar \\"
echo "--jvmopt=-Dotel.traces.exporter=otlp \\"
echo "--jvmopt=-Dotel.exporter.otlp.endpoint=http://127.0.0.1:4318 \\"
echo "--jvmopt=-Dotel.service.name=mongot \\"
echo "--jvmopt=-Dotel.logs.exporter=none"
echo ""