# Build stage
FROM golang:1.25.6-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build arguments
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.buildTime=${BUILD_TIME}" \
    -o pgbranch \
    ./cmd/pgbranch

# Final stage
FROM alpine:3.23.3

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata postgresql-client

# Create non-root user
RUN addgroup -g 1000 pgbranch && \
    adduser -u 1000 -G pgbranch -s /bin/sh -D pgbranch

# Create data directory
RUN mkdir -p /data && chown pgbranch:pgbranch /data

# Copy binary from builder
COPY --from=builder /app/pgbranch /usr/local/bin/pgbranch

# Copy migrations if any
# COPY --from=builder /app/migrations /migrations

# Switch to non-root user
USER pgbranch

# Set working directory
WORKDIR /data

# Environment variables
ENV PGBRANCH_LISTEN_ADDR=:6432
ENV PGBRANCH_API_ADDR=:8080
ENV PGBRANCH_DATA_DIR=/data
ENV PGBRANCH_LOG_LEVEL=info
ENV PGBRANCH_LOG_FORMAT=json

# Expose ports
# 6432 - Postgres proxy
# 8080 - HTTP API / Dashboard
EXPOSE 6432 8080

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD wget -q --spider http://localhost:8080/health || exit 1

# Default command
ENTRYPOINT ["pgbranch"]
CMD ["serve"]
