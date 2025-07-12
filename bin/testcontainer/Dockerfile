FROM rust:1.85-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
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

WORKDIR /build/bin/testcontainer
RUN cargo build --release

FROM debian:bookworm-slim

ENV PATH="/usr/local/bin:$PATH"
ENV RUST_BACKTRACE=1

# Install only runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
 && rm -rf /var/lib/apt/lists/*

# Add unprivileged user with no login shell
RUN useradd --system --uid 1001 --no-create-home --shell /usr/sbin/nologin reifydb

COPY --from=builder /build/target/release/reifydb-testcontainer /usr/local/bin/

RUN chown root:root /usr/local/bin/reifydb-testcontainer && \
    chmod 0755 /usr/local/bin/reifydb-testcontainer

USER reifydb

EXPOSE 8090
CMD ["reifydb-testcontainer"]