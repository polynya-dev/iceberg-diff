# Two-stage build: compile iceberg-diff in a rust:1 image, then ship a thin
# debian-slim runtime with just the binary + ca-certificates (needed for HTTPS
# to the Iceberg REST catalog and R2).
#
# Image ends up ~80 MB. If that matters we can switch to distroless later;
# debian-slim keeps us compatible with any openssl-wanting transitive deps.

FROM rust:1 AS builder
WORKDIR /src
COPY . .
# --bin iceberg-diff excludes gen_fixture (maintainer tool, not shipped).
RUN cargo build --release --bin iceberg-diff

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/iceberg-diff /usr/local/bin/iceberg-diff
ENTRYPOINT ["/usr/local/bin/iceberg-diff"]
