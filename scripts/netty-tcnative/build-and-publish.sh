#!/bin/bash
# Build netty-tcnative jar with dynamic OpenSSL linking and upload to S3.
#
# This script builds a netty-tcnative JAR that links dynamically against OpenSSL. The artifact
# provided by maven only links against OpenSSL1.1, so we have to build it ourselves to work with
# AL2023 which has OpenSSL3.2
#
# The built JAR is uploaded to S3 and bazel/java/deps.bzl is updated with the new SHA256.
#
# Usage: ./build-and-publish.sh <version> [options]
#
# Options:
#   --aws-profile <profile>  AWS profile to use for S3 upload (default: $AWS_PROFILE or "default")
#   --dry-run                Preview what would be uploaded/updated without making changes
#   --skip-build             Skip Docker build and use existing JAR in output directory
#
# Examples:
#   ./build-and-publish.sh 2.0.71.Final
#   ./build-and-publish.sh 2.0.71.Final --aws-profile my-profile
#   ./build-and-publish.sh 2.0.71.Final --dry-run
#   ./build-and-publish.sh 2.0.71.Final --skip-build --dry-run

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NETTY_TCNATIVE_BZL_PATH="${REPO_ROOT}/bazel/java/netty_tcnative.bzl"

# Parse arguments
VERSION=""
AWS_PROFILE="${AWS_PROFILE:-default}"
DRY_RUN=false
SKIP_BUILD=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --aws-profile)
            AWS_PROFILE="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        -*)
            echo "Unknown option: $1"
            echo "Usage: $0 <version> [--aws-profile <profile>] [--dry-run] [--skip-build]"
            exit 1
            ;;
        *)
            VERSION="$1"
            shift
            ;;
    esac
done

if [[ -z "${VERSION}" ]]; then
    echo "Usage: $0 <version> [--aws-profile <profile>] [--dry-run] [--skip-build]"
    echo ""
    echo "Options:"
    echo "  --aws-profile <profile>  AWS profile to use for S3 upload"
    echo "  --dry-run                Preview what would be uploaded/updated without making changes"
    echo "  --skip-build             Skip Docker build and use existing JAR in output directory"
    echo ""
    echo "Examples:"
    echo "  $0 2.0.71.Final"
    echo "  $0 2.0.71.Final --aws-profile my-profile"
    echo "  $0 2.0.71.Final --dry-run"
    echo "  $0 2.0.71.Final --skip-build --dry-run"
    exit 1
fi

# Configuration
IMAGE_NAME="netty-tcnative-builder"
OUTPUT_DIR="${SCRIPT_DIR}/output"
JAR_NAME="netty-tcnative-${VERSION}-linux-x86_64.jar"
JAR_FILE="${OUTPUT_DIR}/${JAR_NAME}"
S3_BUCKET="search-build-and-release-3p-tools"
S3_PATH="netty-tcnative/${JAR_NAME}"

if [[ "${SKIP_BUILD}" == "true" ]]; then
    echo "=== Skipping build, looking for existing JAR ==="
    if [[ ! -f "${JAR_FILE}" ]]; then
        echo "Error: Could not find JAR file at expected location:"
        echo "  ${JAR_FILE}"
        echo ""
        echo "Either run without --skip-build to build the JAR, or ensure the JAR exists."
        exit 1
    fi
    echo "Found existing JAR: ${JAR_FILE}"
else
    echo "=== Building netty-tcnative ${VERSION} with dynamic OpenSSL linking ==="

    # Build the Docker image for linux/amd64 (x86_64)
    echo "Building Docker image for linux/amd64..."
    docker build --platform linux/amd64 -t "${IMAGE_NAME}" "${SCRIPT_DIR}"

    # Create output directory
    rm -rf "${OUTPUT_DIR}"
    mkdir -p "${OUTPUT_DIR}"

    # Run the build inside Docker (force x86_64 platform)
    echo "Running build in Docker container..."
    docker run --rm \
        --platform linux/amd64 \
        -v "${OUTPUT_DIR}:/build/output" \
        "${IMAGE_NAME}" \
        "${VERSION}"

    if [[ ! -f "${JAR_FILE}" ]]; then
        echo "Error: Could not find JAR file (${JAR_NAME})"
        echo "Contents of output directory:"
        ls -la "${OUTPUT_DIR}" 2>/dev/null || echo "  (directory does not exist)"
        exit 1
    fi

    echo "Built JAR: ${JAR_FILE}"
fi

echo ""

# Compute SHA256
SHA256=$(shasum -a 256 "${JAR_FILE}" | awk '{print $1}')

# Check if SHA matches what's already in netty_tcnative.bzl
EXISTING_SHA=$(grep '^NETTY_TCNATIVE_AL2023_X86_64_SHA256 = ' "${NETTY_TCNATIVE_BZL_PATH}" | sed 's/.*= "\(.*\)"/\1/')

echo "=== SHA256 Comparison ==="
echo "Existing SHA256 in netty_tcnative.bzl: ${EXISTING_SHA}"
echo "Computed SHA256 from JAR:    ${SHA256}"
echo ""

if [[ "${SHA256}" == "${EXISTING_SHA}" ]]; then
    echo "SHA256 values match. No need to upload or update netty_tcnative.bzl."
    exit 0
fi

echo "SHA256 values do not match. Proceeding with upload and update."
echo ""

if [[ "${DRY_RUN}" == "true" ]]; then
    echo "=== DRY RUN ==="
    echo "Would upload to: s3://${S3_BUCKET}/${S3_PATH}"
    echo "SHA256: ${SHA256}"
    echo ""
    echo "Would update ${NETTY_TCNATIVE_BZL_PATH} with:"
    echo "    NETTY_TCNATIVE_VERSION = \"${VERSION}\""
    echo "    NETTY_TCNATIVE_AL2023_X86_64_SHA256 = \"${SHA256}\""
    exit 0
fi

# Upload to S3
echo "=== Uploading JAR to S3 ==="
aws --profile "${AWS_PROFILE}" s3 cp "${JAR_FILE}" "s3://${S3_BUCKET}/${S3_PATH}"

# Update bazel/netty_tcnative.bzl (version and SHA256)
echo ""
echo "=== Updating ${NETTY_TCNATIVE_BZL_PATH} ==="

# Update NETTY_TCNATIVE_VERSION (portable sed that works on both macOS and Linux)
sed "s/^NETTY_TCNATIVE_VERSION = \".*\"/NETTY_TCNATIVE_VERSION = \"${VERSION}\"/" "${NETTY_TCNATIVE_BZL_PATH}" > "${NETTY_TCNATIVE_BZL_PATH}.tmp"
mv "${NETTY_TCNATIVE_BZL_PATH}.tmp" "${NETTY_TCNATIVE_BZL_PATH}"

# Update NETTY_TCNATIVE_AL2023_X86_64_SHA256
sed "s/^NETTY_TCNATIVE_AL2023_X86_64_SHA256 = \".*\"/NETTY_TCNATIVE_AL2023_X86_64_SHA256 = \"${SHA256}\"/" "${NETTY_TCNATIVE_BZL_PATH}" > "${NETTY_TCNATIVE_BZL_PATH}.tmp"
mv "${NETTY_TCNATIVE_BZL_PATH}.tmp" "${NETTY_TCNATIVE_BZL_PATH}"

echo ""
echo "=== Done ==="
echo "JAR uploaded to: s3://${S3_BUCKET}/${S3_PATH}"
echo "SHA256: ${SHA256}"
echo "Updated: ${NETTY_TCNATIVE_BZL_PATH}"
echo ""
echo "Next steps:"
echo "  1. Review the changes: git diff ${NETTY_TCNATIVE_BZL_PATH}"
echo "  2. Test the build: make build"
echo "  3. Commit the changes"
