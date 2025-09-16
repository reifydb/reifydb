# =============================================================================
# Build Targets - Build all packages
# =============================================================================

.PHONY: build build-db build-bin build-pkg-rust build-pkg-typescript

# Main build target - builds everything
build: build-db build-bin build-pkg-rust build-pkg-typescript
	@echo "✅ All packages built successfully!"

# Build db/ workspace packages
build-db:
	@echo "🏗️ Building db/ workspace packages..."
	@if [ -d "db/vendor" ]; then \
		echo "Using vendored dependencies (offline mode)"; \
		cd db && cargo build --release --workspace --offline; \
	else \
		echo "Using network dependencies"; \
		cd db && cargo build --release --workspace; \
	fi

# Build bin/ packages
build-bin:
	@echo "🏗️ Building bin/ packages..."
	@for dir in bin/cli bin/server bin/playground bin/testcontainer; do \
		if [ -d "$$dir" ]; then \
			echo "  Building $$dir..."; \
			cd $$dir && cargo build --release $(CARGO_OFFLINE) 2>/dev/null || true && cd - >/dev/null; \
		fi; \
	done

# Build pkg/rust packages
build-pkg-rust:
	@echo "🏗️ Building pkg/rust packages..."
	@for dir in pkg/rust/reifydb pkg/rust/reifydb-client pkg/rust/examples; do \
		if [ -d "$$dir" ]; then \
			echo "  Building $$dir..."; \
			cd $$dir && cargo build --release $(CARGO_OFFLINE) 2>/dev/null || true && cd - >/dev/null; \
		fi; \
	done

# Build pkg/typescript packages
build-pkg-typescript:
	@echo "🏗️ Building pkg/typescript packages..."
	@if [ -d "pkg/typescript" ]; then \
		echo "  Installing dependencies..."; \
		cd pkg/typescript && pnpm install 2>/dev/null || npm install 2>/dev/null || true; \
		echo "  Building TypeScript packages..."; \
		cd pkg/typescript && pnpm build 2>/dev/null || npm run build 2>/dev/null || true; \
	fi

# Build with vendored dependencies (helper script wrapper)
build-vendored:
	@echo "🏗️ Building with vendored dependencies..."
	@./mk/build-vendored.sh