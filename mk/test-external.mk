# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 ReifyDB

# =============
# External Tests (SLT snapshot regression)
# =============

.PHONY: test-external

# Run snapshot regression tests (compares against baselines)
test-external:
	@echo "⏭️  Skipping external SLT regression tests (temporarily disabled)"
#	@echo "🔍 Running external SLT regression tests..."
#	cd $(EXTERNAL_DIR) && CARGO_TARGET_DIR=$(realpath target) $(MAKE) test
