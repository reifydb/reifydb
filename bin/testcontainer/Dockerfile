# Build stage
FROM rust:1.85 AS builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./

COPY crates ./crates
COPY pkg ./pkg
COPY bin ./bin
COPY testsuite ./testsuite

# Copy this specific binary
#COPY bin/test ./bin/test

# Try to build only the specific package (might not work with workspace structure)
RUN cd bin/testcontainer && cargo build --release

# Or copy the binary from the bin/testcontainer target directory
RUN cp bin/testcontainer/target/release/reifydb-testcontainer /usr/local/bin/reifydb-testcontainer || \
    find . -name "reifydb-testcontainer" -type f -executable -exec cp {} /usr/local/bin/reifydb-testcontainer \;

# Runtime stage
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