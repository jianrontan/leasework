# syntax=docker/dockerfile:1

# Single Dockerfile builds all three binaries (api, worker, scheduler); compose
# invokes this file three times with a different SERVICE build arg so the
# dependency-download layer is shared across all three image builds.
FROM golang:1.25-alpine AS build
ARG SERVICE
WORKDIR /src

# Copy just the module files first so `go mod download` is cached as its own
# layer and only re-runs when dependencies actually change.
COPY go.mod go.sum* ./
RUN go mod download

COPY . .

# CGO disabled so the binary is fully static and runs on distroless.
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/service ./cmd/${SERVICE}

# distroless:nonroot has no shell, no package manager, and runs as a non-root
# user by default -- smallest attack surface for a service that only needs to
# run a static Go binary. Swap to alpine:3.20 if a shell is ever needed to
# debug inside the container.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/service /service
USER nonroot:nonroot
ENTRYPOINT ["/service"]
