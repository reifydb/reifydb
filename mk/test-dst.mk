# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

# =============================================================================
# DST (Deterministic Simulation Testing)
# =============================================================================

# REIFYDB_DST=1 flips the reifydb_dst cfg, so this must never share a target dir with the non-DST builds.
DST_TARGET_DIR := $(CURDIR)/target/dst

.PHONY: test-dst
test-dst:
	@echo "🧪 Running DST tests..."
	MAKEFLAGS= CARGO_TARGET_DIR=$(DST_TARGET_DIR) REIFYDB_DST=1 cargo test --release -p reifydb-runtime --no-fail-fast $(CARGO_OFFLINE)
	MAKEFLAGS= CARGO_TARGET_DIR=$(DST_TARGET_DIR) REIFYDB_DST=1 cargo test --release -p reifydb-client --features dst --no-fail-fast $(CARGO_OFFLINE)
	@$(MAKE) --no-print-directory sweep-auto
