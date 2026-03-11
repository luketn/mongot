#!/bin/bash
# Build script for netty-tcnative with dynamic OpenSSL linking
# This script is run inside the Docker container.
#
# Usage: ./build.sh <version>
# Example: ./build.sh 2.0.71.Final

set -euo pipefail

VERSION="${1:-}"

if [[ -z "${VERSION}" ]]; then
    echo "Usage: $0 <version>"
    echo "Example: $0 2.0.71.Final"
    exit 1
fi

echo "Building netty-tcnative version ${VERSION} with dynamic OpenSSL linking..."

# Clone netty-tcnative
git clone --depth 1 --branch "netty-tcnative-parent-${VERSION}" \
    https://github.com/netty/netty-tcnative.git /build/netty-tcnative

cd /build/netty-tcnative

# Build the openssl-dynamic module which links dynamically to system OpenSSL
./mvnw clean package \
    -pl openssl-dynamic \
    -am \
    -DskipTests \
    -DstaticNativeLib=false \
    -DopensslHome=/usr \
    -Dnative.build.type=release

# The built JAR is at:
# openssl-dynamic/target/netty-tcnative-${VERSION}-linux-x86_64.jar

# Copy the specific JAR we need to output directory with the expected name
mkdir -p /build/output
SOURCE_JAR="/build/netty-tcnative/openssl-dynamic/target/netty-tcnative-${VERSION}-linux-x86_64.jar"
OUTPUT_JAR="/build/output/netty-tcnative-${VERSION}-linux-x86_64.jar"

if [[ ! -f "${SOURCE_JAR}" ]]; then
    echo "Error: Expected JAR not found at ${SOURCE_JAR}"
    echo "Available JARs:"
    find /build/netty-tcnative -name "netty-tcnative*.jar" -type f
    exit 1
fi

cp "${SOURCE_JAR}" "${OUTPUT_JAR}"
echo "Build complete. JAR available at: ${OUTPUT_JAR}"
ls -la /build/output/
