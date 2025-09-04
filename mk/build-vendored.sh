#!/bin/bash
# Copyright (c) reifydb.com 2025
# This file is licensed under the AGPL-3.0-or-later, see license.md file

# Helper script for building with vendored dependencies

set -e

# Build with vendored dependencies
cargo build --release --offline "$@"
