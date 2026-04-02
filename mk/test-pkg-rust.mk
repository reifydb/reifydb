# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 ReifyDB

# =============================================================================
# Rust Package Testing (pkg/rust tests)
# =============================================================================

.PHONY: test-pkg-rust test-rust-limit test-rust-regression

# Run all pkg/rust tests
test-pkg-rust: test-rust-limit test-rust-regression
	@echo "✅ All pkg/rust tests completed!"

# Run limit tests
test-rust-limit:
	@echo "🧪 Running limit tests..."
	@cd pkg/rust/tests/limit && cargo nextest run --release --no-fail-fast --status-level fail --final-status-level fail $(CARGO_OFFLINE)

# Run regression tests  
test-rust-regression:
	@echo "🧪 Running regression tests..."
	@cd pkg/rust/tests/regression && cargo nextest run --release --no-fail-fast --status-level fail --final-status-level fail $(CARGO_OFFLINE)