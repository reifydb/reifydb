---
sidebar_position: 1
---

# Installation

Install ReifyDB using your preferred method.

## Using Cargo

```bash
cargo install reifydb
```

## Using Docker

```bash
docker pull reifydb/reifydb:latest
docker run -p 5432:5432 reifydb/reifydb
```

## Building from Source

```bash
git clone https://github.com/reifydb/reifydb
cd reifydb
cargo build --release
```

## System Requirements

- **Rust**: 1.70.0 or higher
- **Memory**: Minimum 2GB RAM
- **Storage**: 100MB for binaries
- **OS**: Linux, macOS, Windows

## Verify Installation

```bash
reifydb --version
```
