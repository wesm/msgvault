# Stage 1: Builder
# Use Debian-based Go image for CGO compatibility (DuckDB, SQLite FTS5)
FROM golang:1.25-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc g++ make git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Copy go.mod and go.sum first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build arguments for version info
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

# Build with CGO enabled (required for DuckDB and SQLite FTS5)
RUN CGO_ENABLED=1 go build -tags fts5 -trimpath \
    -ldflags="-s -w \
      -X github.com/wesm/msgvault/cmd/msgvault/cmd.Version=${VERSION} \
      -X github.com/wesm/msgvault/cmd/msgvault/cmd.Commit=${COMMIT} \
      -X github.com/wesm/msgvault/cmd/msgvault/cmd.BuildDate=${BUILD_DATE}" \
    -o /msgvault ./cmd/msgvault

# Stage 2: Runtime
# Use Debian slim for glibc compatibility with CGO binaries
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user (UID 1000 for compatibility with NAS permissions)
RUN useradd -m -u 1000 -s /bin/bash msgvault

# Copy binary from builder
COPY --from=builder /msgvault /usr/local/bin/msgvault

# Set environment
ENV MSGVAULT_HOME=/data

# Create data directory and set permissions
RUN mkdir -p /data && chown msgvault:msgvault /data

# Declare volume for persistent data
VOLUME /data

# Switch to non-root user
USER msgvault
WORKDIR /data

# Default entrypoint
ENTRYPOINT ["msgvault"]
CMD ["--help"]
