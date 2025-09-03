# =============================================================================
# Local Testing (reifydb crate tests)
# =============================================================================

.PHONY: clean-local
clean-local:
	@echo "🧹 Cleaning generated test files..."
	@find . -path "*/tests/generated_*" -type f -o -path "*/tests/generated_*" -type d | while read path; do \
		echo "  Removing $$path"; \
		rm -rf "$$path"; \
	done

.PHONY: build-before-test
build-before-test:
	@echo "🔨 Building project and tests to ensure generated test files exist..."
	@# Touch build.rs files if their generated_tests.rs is missing to force rebuild
	@for crate in reifydb-storage reifydb-rql reifydb-sub-flow reifydb-transaction; do \
		if [ ! -f "crates/$$crate/tests/generated_tests.rs" ]; then \
			touch "crates/$$crate/build.rs" 2>/dev/null || true; \
		fi; \
	done
	@cargo test --all-targets --no-run 2>&1 | grep -E "Compiling|Finished|Executable" | tail -20 || true

.PHONY: test-local
test-local: build-before-test
	@echo "🧪 Running local reifydb crate tests..."
	cargo nextest run --lib --bins --tests --examples --features test-stress --no-fail-fast --status-level fail --final-status-level fail
	@echo "📚 Running doc tests..."
	cargo test --doc

.PHONY: test-local-clean
test-local-clean: clean-local test-local
	@echo "✅ Clean test run completed!"