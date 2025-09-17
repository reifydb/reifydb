# =============================================================================
# Database Testing (crates/ workspace tests)
# =============================================================================

.PHONY: test-crates
test-crates:
	@echo "🧪 Running crates/ tests..."
	cd crates && cargo nextest run --workspace --lib --bins --tests --examples --features test-stress --no-fail-fast --status-level fail --final-status-level fail $(CARGO_OFFLINE)
	@echo "📚 Running doc tests..."
	cd crates && cargo test --workspace --doc $(CARGO_OFFLINE)