#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB
# Build WebAssembly packages for ReifyDB
#
# Builds WASM bindings for web, Node.js, and bundler targets.
# Requires wasm-pack to be installed.
#
# Usage: ./scripts/build-wasm.sh
#
# Outputs:
#   - pkg/webassembly/dist/web/     - For browsers with ES modules
#   - pkg/webassembly/dist/node/    - For Node.js
#   - pkg/webassembly/dist/bundler/ - For webpack/Vite/etc

set -e

REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
WASM_DIR="$REPO_ROOT/pkg/webassembly"
PROFILE="${1:-release}"
if [ "$PROFILE" = "release" ]; then
    PROFILE_FLAG="--release"
else
    PROFILE_FLAG="--profile $PROFILE"
fi

echo "Building ReifyDB WASM packages..."
echo

cd "$WASM_DIR"

# Clean previous build artifacts to avoid wasm-pack re-read bugs
rm -rf dist/

# Build for web
echo "Building for web (ES modules)..."
wasm-pack build \
    --target web \
    --out-dir dist/web \
    $PROFILE_FLAG

# Build for Node.js
echo "Building for Node.js..."
wasm-pack build \
    --target nodejs \
    --out-dir dist/node \
    $PROFILE_FLAG

# Build for bundlers
echo "Building for bundlers (webpack, vite, etc.)..."
wasm-pack build \
    --target bundler \
    --out-dir dist/bundler \
    $PROFILE_FLAG

echo
echo "WASM build complete!"

if [ "$PROFILE" = "wasm-release" ]; then
    GZIP_LEVEL=9
else
    GZIP_LEVEL=1
fi
echo "Compressing web wasm (gzip -$GZIP_LEVEL)..."
gzip -f -"$GZIP_LEVEL" dist/web/reifydb_webassembly_bg.wasm

# Copy compressed web output to @reifydb/wasm package
TS_WASM_DIR="$REPO_ROOT/pkg/typescript/wasm/wasm"
echo "Copying web output to $TS_WASM_DIR..."
rm -rf "$TS_WASM_DIR"
mkdir -p "$TS_WASM_DIR"
cp -r dist/web/* "$TS_WASM_DIR/"

echo
echo "Outputs:"
echo "  - pkg/webassembly/dist/web/       - For browsers with ES modules (wasm gzipped)"
echo "  - pkg/webassembly/dist/node/      - For Node.js"
echo "  - pkg/webassembly/dist/bundler/   - For webpack/Vite/etc"
echo "  - pkg/typescript/wasm/wasm/       - Web output with gzipped wasm for @reifydb/wasm"
