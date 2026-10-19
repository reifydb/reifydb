# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

.PHONY: test-fuzz

test-fuzz:
	@echo "🔬 Replaying fuzz crash files..."
	cd $(TEST_FUZZ_DIR) && $(MAKE) test
	@$(MAKE) --no-print-directory sweep-auto
