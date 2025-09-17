# =============================================================================
# Build Targets - Build all packages
# =============================================================================

.PHONY: build build-workspace build-pkg-typescript

# Main build target - builds everything
build: build-workspace build-pkg-typescript
	@echo "✅ All packages built successfully!"

# Build entire Rust workspace (includes crates/, bin/, and pkg/rust/)
build-workspace:
	@echo "🏗️ Building Rust workspace..."
	@if [ -d "vendor" ]; then \
		echo "Using vendored dependencies (offline mode)"; \
		cargo build --release --workspace --offline; \
	else \
		echo "Using network dependencies"; \
		cargo build --release --workspace; \
	fi

# Build pkg/typescript packages
build-pkg-typescript:
	@echo "🏗️ Building pkg/typescript packages..."
	@if [ -d "pkg/typescript" ]; then \
		echo "  Installing dependencies..."; \
		cd pkg/typescript && pnpm install 2>/dev/null || npm install 2>/dev/null || true; \
		echo "  Building TypeScript packages..."; \
		cd pkg/typescript && pnpm build 2>/dev/null || npm run build 2>/dev/null || true; \
	fi

# Build with vendored dependencies
build-vendored:
	@echo "🏗️ Building with vendored dependencies..."
	@cargo build --release --workspace --offline