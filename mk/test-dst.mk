# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 ReifyDB

# =============================================================================
# DST (Deterministic Simulation Testing)
# =============================================================================

.PHONY: test-dst
test-dst:
	@echo "🧪 Running DST tests..."
	REIFYDB_DST=1 cargo test --release -p reifydb-runtime --no-fail-fast $(CARGO_OFFLINE)
	REIFYDB_DST=1 cargo test --release -p reifydb-client --features dst --no-fail-fast $(CARGO_OFFLINE)
