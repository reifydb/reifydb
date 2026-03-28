# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 ReifyDB

# =============================================================================
# Workspace Testing (all workspace tests)
# =============================================================================

.PHONY: test-workspace
test-workspace:
	@echo "🧪 Running workspace tests..."
	cargo nextest run --workspace --lib --bins --tests --examples --features test-stress --no-fail-fast --status-level fail --final-status-level fail $(CARGO_OFFLINE)
	@echo "📚 Running doc tests..."
	cargo test --workspace --doc $(CARGO_OFFLINE)