# syntax=docker/dockerfile:1

# Frontend build: Node 22 is the floor for Vite 8 (^20.19.0 || >=22.12.0).
FROM node:22-alpine AS web
WORKDIR /src/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# Backend build: static Linux binary, CGO off (the app only needs pure Go deps).
# The alpine variant is enough for a static binary and downloads far less than
# the Debian one. GOPROXY defaults to a mirror because BuildKit does not inherit
# the host shell's proxy settings; override it wherever the default is not
# reachable, e.g. --build-arg GOPROXY=https://proxy.golang.org,direct.
FROM golang:1.26-alpine AS backend
ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY}
WORKDIR /src/backend
COPY backend/go.mod backend/go.sum ./
RUN go mod download
COPY backend/ ./
ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags="-s -w" -o /out/im-server ./cmd/server

# Runtime: binary + built web assets. The server needs no writable state, makes
# no outbound HTTPS calls and uses UTC only, so no apk packages are installed --
# the CA bundle is copied from the builder instead. That keeps this stage free of
# network access, which is what made `apk add` the slowest step on a proxied host.
FROM alpine:3.20 AS runtime
COPY --from=backend /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
RUN adduser -D -u 10001 -h /app im
WORKDIR /app
COPY --from=backend /out/im-server /usr/local/bin/im-server
COPY --from=web /src/frontend/dist /app/web
USER im
EXPOSE 8080

# The default binds 127.0.0.1, which is unreachable from outside the container,
# so the published address is always passed explicitly here.
ENTRYPOINT ["/usr/local/bin/im-server"]
CMD ["-addr", "0.0.0.0:8080", "-web-dir", "/app/web"]