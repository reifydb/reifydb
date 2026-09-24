# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

.PHONY: test-regression

test-regression:
	@echo "🔍 Running bug regression tests..."
	cd $(TEST_REGRESSION_DIR) && $(MAKE) test
	@$(MAKE) --no-print-directory sweep-auto
