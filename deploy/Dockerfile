# syntax=docker/dockerfile:1

FROM golang:1.27-alpine AS build
WORKDIR /src

# Dependencies first: they change far less often than the code does.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO off: a static binary, no libc to match at runtime, no surprise dependency
# on the build image's glibc version.
RUN CGO_ENABLED=0 GOFLAGS=-trimpath go build -ldflags="-s -w" -o /out/ ./cmd/...

# Alpine rather than distroless. Manooch is a thing you debug at three in the
# morning, and a shell, wget and nslookup in the image are worth more than the
# few megabytes they cost.
FROM alpine:3.22
RUN adduser -D -u 10001 manooch
COPY --from=build /out/manooch-feed /out/manooch-status /out/manooch-tap /usr/local/bin/
USER manooch
ENTRYPOINT ["manooch-feed"]
