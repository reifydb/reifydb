FROM rust:1.92.0-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    clang \
    mold \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy workspace root files
COPY Cargo.toml Cargo.lock ./

# Copy vendor directory and cargo config for offline builds
COPY vendor ./vendor
COPY .cargo ./.cargo

# Copy all workspace members
COPY crates ./crates
COPY pkg ./pkg
COPY bin ./bin

WORKDIR /build
RUN RUSTC_WRAPPER="" cargo build --release --offline --package testcontainer

FROM rust:1.92.0-slim

LABEL org.opencontainers.image.title="ReifyDB Test Container"
LABEL org.opencontainers.image.source="https://github.com/reifydb/reifydb"
LABEL org.opencontainers.image.licenses="Apache-2.0"

ENV PATH="/usr/local/bin:$PATH"
ENV RUST_BACKTRACE=1

# Add unprivileged user with no login shell
RUN useradd --system --uid 1001 --no-create-home --shell /usr/sbin/nologin reifydb

# Copy from the workspace target location
COPY --from=builder /build/target/release/testcontainer /usr/local/bin/

RUN chown root:root /usr/local/bin/testcontainer && \
    chmod 0755 /usr/local/bin/testcontainer

USER reifydb

EXPOSE 18090
EXPOSE 18091
CMD ["testcontainer"]