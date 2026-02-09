# Build stage
FROM golang:1.25-bookworm AS builder

# Install build dependencies for CGO (SQLite, DuckDB)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Download dependencies first (layer caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

RUN CGO_ENABLED=1 go build \
    -tags fts5 \
    -trimpath \
    -ldflags="-s -w \
        -X github.com/wesm/msgvault/cmd/msgvault/cmd.Version=${VERSION} \
        -X github.com/wesm/msgvault/cmd/msgvault/cmd.Commit=${COMMIT} \
        -X github.com/wesm/msgvault/cmd/msgvault/cmd.BuildDate=${BUILD_DATE}" \
    -o /msgvault \
    ./cmd/msgvault

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 -s /bin/sh msgvault

# Copy binary from builder
COPY --from=builder /msgvault /usr/local/bin/msgvault

# Set up data directory
ENV MSGVAULT_HOME=/data
VOLUME /data

# Switch to non-root user
USER msgvault
WORKDIR /data

# Health check using wget (curl not included to keep image small)
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -q --spider http://localhost:8080/health || exit 1

# Default port for HTTP API
EXPOSE 8080

# Use entrypoint so users can run any msgvault command
ENTRYPOINT ["msgvault"]

# Default to serve mode
CMD ["serve"]
