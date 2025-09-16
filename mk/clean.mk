# =============================================================================
# Clean Targets - Remove build artifacts from all packages
# =============================================================================

.PHONY: clean clean-db clean-bin clean-pkg-rust clean-pkg-typescript

# Main clean target - cleans everything
clean: clean-db clean-bin clean-pkg-rust clean-pkg-typescript
	@echo "✅ All packages cleaned!"

# Clean db/ workspace packages
clean-db:
	@echo "📦 Cleaning db/ workspace packages..."
	@cd db && for pkg in $$(cargo metadata --format-version 1 --no-deps | jq -r '.packages[].name' | grep '^reifydb-'); do \
		echo "  Cleaning $$pkg"; \
		cargo clean -p $$pkg; \
	done

# Clean bin/ packages
clean-bin:
	@echo "📦 Cleaning bin/ packages..."
	@for dir in bin/cli bin/server bin/playground bin/testcontainer; do \
		if [ -d "$$dir" ]; then \
			echo "  Cleaning $$dir"; \
			cd $$dir && cargo clean 2>/dev/null || true && cd - >/dev/null; \
		fi; \
	done

# Clean pkg/rust packages
clean-pkg-rust:
	@echo "📦 Cleaning pkg/rust packages..."
	@for dir in pkg/rust/reifydb pkg/rust/reifydb-client pkg/rust/examples pkg/rust/tests/limit pkg/rust/tests/regression; do \
		if [ -d "$$dir" ]; then \
			echo "  Cleaning $$dir"; \
			cd $$dir && cargo clean 2>/dev/null || true && cd - >/dev/null; \
		fi; \
	done

# Clean pkg/typescript packages
clean-pkg-typescript:
	@echo "📦 Cleaning pkg/typescript packages..."
	@if [ -d "pkg/typescript" ]; then \
		echo "  Cleaning pkg/typescript"; \
		cd pkg/typescript && rm -rf node_modules */node_modules */*/node_modules 2>/dev/null || true; \
	fi