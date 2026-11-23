# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

# =============================================================================
# DST (Deterministic Simulation Testing)
# =============================================================================

.PHONY: test-dst test-dst-workspace
test-dst: test-dst-workspace
	cd $(TEST_DST_DIR) && $(MAKE) test
	@$(MAKE) --no-print-directory sweep-auto

test-dst-workspace:
	@echo "🧪 Running DST tests..."
	MAKEFLAGS= REIFYDB_DST=1 cargo test --profile gate-dst -p reifydb-runtime --no-fail-fast $(CARGO_OFFLINE)
	@$(MAKE) --no-print-directory sweep-auto
