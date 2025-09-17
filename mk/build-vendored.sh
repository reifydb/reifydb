#!/bin/bash
# Helper script for building with vendored dependencies

set -e

# Build with vendored dependencies in crates/ workspace
cd crates && cargo build --release --offline "$@"
