# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2026 ReifyDB

# =============================================================================
# DST (Deterministic Simulation Testing)
# =============================================================================

.PHONY: test-dst
test-dst:
	@echo "🧪 Running DST tests..."
	REIFYDB_DST=1 cargo test --release -p reifydb-runtime --no-fail-fast $(CARGO_OFFLINE)
	REIFYDB_DST=1 cargo test --release -p reifydb-client --features dst --no-fail-fast $(CARGO_OFFLINE)
