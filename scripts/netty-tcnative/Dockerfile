# Dockerfile for building netty-tcnative with dynamic OpenSSL linking
# This produces a JAR that dynamically links to the system's OpenSSL library
# rather than statically linking BoringSSL.

FROM amazonlinux:2023

# Install build dependencies
RUN dnf install -y \
    gcc \
    gcc-c++ \
    make \
    cmake \
    autoconf \
    automake \
    libtool \
    tar \
    gzip \
    git \
    which \
    # OpenSSL development headers (for dynamic linking)
    openssl-devel \
    # APR development headers
    apr-devel \
    # Java (required for Maven build)
    java-21-amazon-corretto-devel \
    # Additional tools required by netty-tcnative build
    ninja-build \
    perl \
    && dnf clean all

# Install Go (required by netty-tcnative build)
# Detect architecture and download appropriate Go binary
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
        GO_ARCH="amd64"; \
    elif [ "$ARCH" = "aarch64" ]; then \
        GO_ARCH="arm64"; \
    else \
        echo "Unsupported architecture: $ARCH" && exit 1; \
    fi && \
    curl -LO "https://go.dev/dl/go1.22.0.linux-${GO_ARCH}.tar.gz" && \
    tar -C /usr/local -xzf "go1.22.0.linux-${GO_ARCH}.tar.gz" && \
    rm "go1.22.0.linux-${GO_ARCH}.tar.gz"
ENV PATH="${PATH}:/usr/local/go/bin"

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto

# Create working directory
WORKDIR /build

# Copy build script
COPY build.sh /build/build.sh
RUN chmod +x /build/build.sh

# Create a non-root user 'appuser' with home directory
RUN useradd --create-home appuser

# Ensure the user owns the /build directory and its contents
RUN chown -R appuser:appuser /build

# Switch to the non-root user
USER appuser

ENTRYPOINT ["/build/build.sh"]
