# AnyStore v1 — CloudBase Run image.
#
# Produces a small, non-root image. Large file traffic never passes through this
# container: the service only signs provider access and commits metadata.

FROM rust:1.94-slim-bookworm AS builder

WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Cache dependency compilation ahead of the source copy.
COPY Cargo.toml Cargo.lock* ./
COPY crates ./crates
COPY migrations ./migrations

RUN cargo build --release --locked -p anystore-server \
    || cargo build --release -p anystore-server

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home anystore

COPY --from=builder /build/target/release/anystore-server /usr/local/bin/anystore-server

USER anystore
WORKDIR /home/anystore

# CloudBase Run injects PORT; ANYSTORE_PORT overrides it.
ENV ANYSTORE_PORT=8088
EXPOSE 8088

ENTRYPOINT ["/usr/local/bin/anystore-server"]
