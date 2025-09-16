#!/bin/bash
# Helper script for building with vendored dependencies

set -e

# Build with vendored dependencies in db/ workspace
cd db && cargo build --release --offline "$@"
