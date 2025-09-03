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

.PHONY: test-local
test-local:
	@echo "🧪 Running local reifydb crate tests..."
	cargo nextest run --lib --bins --tests --examples --features test-stress --no-fail-fast --status-level fail --final-status-level fail
	@echo "📚 Running doc tests..."
	cargo test --doc

.PHONY: test-local-clean
test-local-clean: clean-local test-local
	@echo "✅ Clean test run completed!"