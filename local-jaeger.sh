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

# Start with a clean jaeger container if it already exists.
docker rm -f jaeger >/dev/null 2>&1 || true

# Start Jaeger with default configuration
docker run -d --name jaeger \
  -p 6831:6831/udp \
  -p 6832:6832/udp \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 14250:14250 \
  -p 14268:14268 \
  -p 16686:16686 \
  --add-host=host.docker.internal:host-gateway \
  jaegertracing/all-in-one:latest

echo ""
echo "✓ Jaeger is running on http://localhost:16686"
echo ""
echo "To monitor MongoT with Jaeger, run it with the OpenTelemetry Java Agent:"
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
echo "The OpenTelemetry Java Agent will automatically instrument MongoT and export"
echo "distributed traces to Jaeger at http://127.0.0.1:4318 (OTLP HTTP)."
echo ""
