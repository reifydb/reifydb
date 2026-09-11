# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

.PHONY: test-crate test-crate-dev test-crate-loom

test-crate:
	@echo "🔍 Running crate mirror tests..."
	cd $(TEST_CRATE_DIR) && $(MAKE) test
	@$(MAKE) --no-print-directory sweep-auto

test-crate-dev:
	@echo "🚀 Running fast crate mirror tests..."
	cd $(TEST_CRATE_DIR) && $(MAKE) test-dev
	@$(MAKE) --no-print-directory sweep-auto

test-crate-loom:
	@echo "🔍 Running crate mirror loom tests..."
	cd $(TEST_CRATE_DIR) && $(MAKE) test-loom
	@$(MAKE) --no-print-directory sweep-auto
