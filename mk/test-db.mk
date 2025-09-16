# =============================================================================
# Database Testing (db/ workspace tests)
# =============================================================================

.PHONY: test-db
test-db:
	@echo "🧪 Running db/ tests..."
	cd db && cargo nextest run --workspace --exclude reifydb-py --lib --bins --tests --examples --features test-stress --no-fail-fast --status-level fail --final-status-level fail $(CARGO_OFFLINE)
	@echo "📚 Running doc tests..."
	cd db && cargo test --workspace --exclude reifydb-py --doc $(CARGO_OFFLINE)