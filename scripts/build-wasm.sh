#!/bin/bash
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

echo "Building ReifyDB WASM packages..."
echo

cd "$WASM_DIR"

# Build for web
echo "Building for web (ES modules)..."
wasm-pack build \
    --target web \
    --out-dir dist/web \
    --release

# Build for Node.js
echo "Building for Node.js..."
wasm-pack build \
    --target nodejs \
    --out-dir dist/node \
    --release

# Build for bundlers
echo "Building for bundlers (webpack, vite, etc.)..."
wasm-pack build \
    --target bundler \
    --out-dir dist/bundler \
    --release

echo
echo "WASM build complete!"
echo
echo "Outputs:"
echo "  - pkg/webassembly/dist/web/       - For browsers with ES modules"
echo "  - pkg/webassembly/dist/node/      - For Node.js"
echo "  - pkg/webassembly/dist/bundler/   - For webpack/Vite/etc"
