# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 ReifyDB

# =============================================================================
# Workspace Testing (all workspace tests)
# =============================================================================

.PHONY: test-workspace
test-workspace:
	@echo "🧪 Running workspace tests..."
	cargo nextest run --release --workspace --lib --bins --tests --examples --no-fail-fast --status-level fail --final-status-level fail $(CARGO_OFFLINE)
	@echo "📚 Running doc tests..."
	cargo test --release --workspace --doc $(CARGO_OFFLINE)