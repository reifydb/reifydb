FROM rust:1.85.1-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy workspace root files
COPY Cargo.toml Cargo.lock rust-toolchain ./

# Copy all workspace members
COPY crates ./crates
COPY pkg ./pkg
COPY bin ./bin

WORKDIR /build
RUN cargo build --release --package testcontainer

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

# Copy from the workspace target location
COPY --from=builder /build/target/release/testcontainer /usr/local/bin/

RUN chown root:root /usr/local/bin/testcontainer && \
    chmod 0755 /usr/local/bin/testcontainer

USER reifydb

EXPOSE 8090
CMD ["testcontainer"]