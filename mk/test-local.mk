# =============================================================================
# Local Testing (reifydb crate tests)
# =============================================================================

.PHONY: test-local
test-local:
	@echo "🧪 Running local reifydb crate tests..."
	cargo nextest run --workspace --exclude reifydb-py --lib --bins --tests --examples --features test-stress --no-fail-fast --status-level fail --final-status-level fail $(CARGO_OFFLINE)
	@echo "📚 Running doc tests..."
	cargo test --workspace --exclude reifydb-py --doc $(CARGO_OFFLINE)