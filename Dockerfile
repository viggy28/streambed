FROM golang:1.26.1-bookworm AS build

WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o /out/streambed ./cmd/streambed

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 streambed \
    && install -d -o streambed -g streambed /var/lib/streambed

ENV HOME=/home/streambed \
    STREAMBED_STATE_PATH=/var/lib/streambed/state.db \
    STREAMBED_DUCKLAKE_CATALOG=/var/lib/streambed/ducklake-catalog.duckdb

COPY --from=build /out/streambed /usr/local/bin/streambed

USER streambed
VOLUME ["/var/lib/streambed"]
EXPOSE 5433 8080

ENTRYPOINT ["/usr/local/bin/streambed"]
CMD ["--help"]
