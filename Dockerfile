# Multi-stage build for Zeabur deployment
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Set build cache
ENV GOCACHE=/root/.cache/go-build
ENV CGO_ENABLED=0
ENV GOOS=linux

# Copy dependency files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary (build entire cmd package so embedded files and multiple sources compile)
RUN go build -o main ./cmd

# Runtime stage
FROM alpine:latest

WORKDIR /root/

# Install CA certificates for HTTPS connections
RUN apk --no-cache add ca-certificates

# Copy binary from builder
COPY --from=builder /app/main /root/main

# Copy static files (index.html must be in project root)
COPY index.html /root/index.html

# Set executable permissions
RUN chmod +x /root/main

# Expose port
EXPOSE 8080

# Health check: ensure app doesn't crash if Redis is temporarily unavailable
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8080/api/health || exit 1

# Run the application
CMD ["/root/main"]
