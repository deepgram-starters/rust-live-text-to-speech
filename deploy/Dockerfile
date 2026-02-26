# Stage 1: Build custom Caddy with rate limiting module
FROM golang:1.23-alpine AS caddy-builder
WORKDIR /build
RUN go install github.com/caddyserver/xcaddy/cmd/xcaddy@latest
RUN xcaddy build --with github.com/mholt/caddy-ratelimit
# Binary output: /build/caddy

# Stage 2: Build frontend
FROM node:24-slim AS frontend-builder
RUN corepack enable
WORKDIR /build
COPY frontend/package.json frontend/pnpm-lock.yaml ./frontend/
RUN cd frontend && pnpm install --frozen-lockfile
COPY frontend/ ./frontend/
RUN cd frontend && pnpm build
# Inject Caddy template directive for subpath base path
RUN sed -i 's|<head>|<head>\n  <base href="{{ .Req.Header.Get "X-Base-Path" }}">|' ./frontend/dist/index.html

# Stage 3: Build Rust backend
FROM rust:1.85-alpine AS backend-builder
RUN apk add --no-cache musl-dev openssl-dev openssl-libs-static pkgconfig
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
# Create a dummy main.rs to cache dependency builds
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
# Now copy real source and rebuild
RUN rm -rf src
COPY src/ ./src/
RUN touch src/main.rs && cargo build --release

# Stage 4: Runtime
FROM alpine:3.21

# Install CA certificates for TLS connections to Deepgram
RUN apk add --no-cache ca-certificates

# Copy custom Caddy binary
COPY --from=caddy-builder /build/caddy /usr/bin/caddy

WORKDIR /app

# Copy compiled Rust binary
COPY --from=backend-builder /build/target/release/rust-live-text-to-speech ./rust-live-text-to-speech

# Copy config files
COPY deepgram.toml sample.env ./

# Copy built frontend
COPY --from=frontend-builder /build/frontend/dist/ ./frontend/dist/

# Copy shared deployment files
COPY deploy/Caddyfile /etc/caddy/Caddyfile
COPY deploy/start.sh ./start.sh
RUN chmod +x ./start.sh

ENV PORT=8081
ENV HOST=0.0.0.0
EXPOSE 8080

ENV BACKEND_CMD="./rust-live-text-to-speech"
CMD ["./start.sh"]
