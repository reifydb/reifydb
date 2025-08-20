# =============================================================================
# Local Testing (reifydb crate tests)
# =============================================================================

.PHONY: test-local
test-local:
	@echo "🧪 Running local reifydb crate tests..."
	cargo nextest run --all-targets --features test-stress --no-fail-fast --status-level fail --final-status-level fail
	@echo "📚 Running doc tests..."
	cargo test --doc