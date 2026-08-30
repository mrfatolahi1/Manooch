# syntax=docker/dockerfile:1

FROM golang:1.27-alpine AS build
WORKDIR /src

# Dependencies first: they change less often than the code.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO off: a static binary, with no libc to match at runtime.
RUN CGO_ENABLED=0 GOFLAGS=-trimpath go build -ldflags="-s -w" -o /out/ ./cmd/...

# Alpine, not distroless: a shell, wget and nslookup are worth the megabytes,
# and the compose healthcheck uses wget.
FROM alpine:3.22
RUN adduser -D -u 10001 manooch
COPY --from=build /out/manooch-feed /out/manooch-status /out/manooch-tap /usr/local/bin/
USER manooch
ENTRYPOINT ["manooch-feed"]
