#!/usr/bin/env bash
set -euo pipefail

# Start with a clean prometheus container if it already exists.
docker rm -f prometheus >/dev/null 2>&1 || true

# Start prometheus with a config to point at MongoT
docker run -d --name prometheus \
  -p 9090:9090 \
  -v "$(pwd)/local-prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
  --add-host=host.docker.internal:host-gateway \
  prom/prometheus:latest

# Start with a clean grafana container if it already exists.
docker rm -f grafana >/dev/null 2>&1 || true

# Start Grafana with provisioning + dashboard
mkdir -p "$(pwd)/grafana/provisioning"
docker run -d --name grafana \
  -p 3000:3000 \
  -v "$(pwd)/local-grafana-datasource.yml:/etc/grafana/provisioning/datasources/datasource.yml:ro" \
  -v "$(pwd)/local-grafana-dashboards.yml:/etc/grafana/provisioning/dashboards/dashboards.yml:ro" \
  -v "$(pwd)/local-grafana-mongot-dashboard.json:/var/lib/grafana/dashboards/mongot-dashboard.json:ro" \
  --add-host=host.docker.internal:host-gateway \
  grafana/grafana:latest

#docker logs -f prometheus
