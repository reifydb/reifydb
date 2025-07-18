# =============================================================================
# Local Testing (reifydb crate tests)
# =============================================================================

.PHONY: test-local
test-local:
	@echo "🧪 Running local reifydb crate tests..."
	cargo nextest run --all-targets --no-fail-fast --status-level fail --final-status-level fail