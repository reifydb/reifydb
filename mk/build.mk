# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

# =============================================================================
# Build Targets - Build all packages
# =============================================================================

.PHONY: build build-workspace build-pkg-typescript build-wasm check check-value-no-features

# Main build target - builds everything
build: build-workspace build-wasm build-pkg-typescript
	@echo "✅ All packages built successfully!"

# Build entire Rust workspace (includes crates/, bin/, and pkg/rust/)
build-workspace:
	@echo "🏗️ Building Rust workspace..."
	@if [ -d "vendor" ]; then \
		echo "Using vendored dependencies (offline mode)"; \
		MAKEFLAGS= cargo build --release --workspace --offline; \
	else \
		echo "Using network dependencies"; \
		MAKEFLAGS= cargo build --release --workspace; \
	fi
	@echo "🔍 Checking all-features compile..."
	@MAKEFLAGS= cargo check --workspace --all-features

check:
	@echo "🔍 Checking all features compile..."
	@MAKEFLAGS= cargo check --workspace --all-features --all-targets
	cd $(TEST_SUITE_DIR) && $(MAKE) check
	cd $(TEST_CRATE_DIR) && $(MAKE) check
	cd $(TEST_CHAOS_DIR) && $(MAKE) check
	cd $(TEST_QUERY_DIR) && $(MAKE) check

check-value-no-features:
	@echo "Checking reifydb-value tests compile without features..."
	@MAKEFLAGS= cargo test -p reifydb-value --no-run $(CARGO_OFFLINE)

# Build pkg/typescript packages
build-pkg-typescript:
	@echo "🏗️ Building pkg/typescript packages..."
	@set -e; if [ -d "pkg/typescript" ]; then \
		echo "  Installing dependencies..."; \
		cd pkg/typescript && pnpm install 2>/dev/null || npm install 2>/dev/null; \
		echo "  Building TypeScript packages..."; \
		cd pkg/typescript && pnpm build 2>/dev/null || npm run build 2>/dev/null; \
	fi

# Build with vendored dependencies
build-vendored:
	@echo "🏗️ Building with vendored dependencies..."
	@MAKEFLAGS= cargo build --release --workspace --offline

# Build WebAssembly packages
build-wasm:
	@echo "Building WebAssembly packages..."
	@if ! command -v wasm-pack >/dev/null 2>&1; then \
		echo "Error: wasm-pack is not installed."; \
		echo "   Install with: cargo install wasm-pack"; \
		exit 1; \
	fi
	@./scripts/build-wasm.sh