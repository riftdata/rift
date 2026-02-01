FROM --platform=$BUILDPLATFORM golang:1.25.6 AS builder

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.buildTime=${BUILD_TIME}" \
    -o /out/rift ./cmd/rift

FROM alpine:3.23.3

# Install runtime dependencies (curl removed — healthcheck uses busybox wget)
RUN apk add --no-cache ca-certificates tzdata postgresql-client

# Create non-root user
RUN addgroup -g 1000 rift && \
    adduser -u 1000 -G rift -s /bin/sh -D rift

# Create data directory
RUN mkdir -p /data && chown rift:rift /data

# Copy built binary
COPY --from=builder /out/rift /usr/local/bin/rift

# Switch to non-root user
USER rift

# Set working directory
WORKDIR /data

# Environment variables
ENV rift_LISTEN_ADDR=:6432
ENV rift_API_ADDR=:8080
ENV rift_DATA_DIR=/data
ENV rift_LOG_LEVEL=info
ENV rift_LOG_FORMAT=json

# Expose ports
# 6432 - Postgres proxy
# 8080 - HTTP API / Dashboard
EXPOSE 6432 8080

# Health check using busybox wget (no curl dependency needed)
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD wget -qO- http://localhost:8080/health || exit 1

# Default command
ENTRYPOINT ["rift"]
CMD ["serve"]