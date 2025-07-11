FROM rust:1.85 AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY Cargo.toml Cargo.lock ./

COPY crates ./crates
COPY pkg ./pkg
COPY bin ./bin
COPY testsuite ./testsuite

RUN cd bin/testcontainer && cargo build --release

RUN pwd
RUN cp target/release/reifydb-testcontainer /usr/local/bin/reifydb-testcontainer

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1001 appuser

COPY --from=builder /usr/local/bin/reifydb-testcontainer /usr/local/bin/reifydb-testcontainer
RUN chown appuser:appuser /usr/local/bin/reifydb-testcontainer
USER appuser
EXPOSE 8090
CMD ["reifydb-testcontainer"]