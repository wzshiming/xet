# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS build
ARG GOPROXY
WORKDIR /src
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=bind,source=./go.mod,target=/src/go.mod \
    --mount=type=bind,source=./go.sum,target=/src/go.sum \
    go mod download

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=bind,source=./,target=/src/ \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.22
COPY --from=build /out/xetd /out/xetc /usr/local/bin/
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["xetd"]
CMD ["-addr", ":8080", "-storage", "/data"]
