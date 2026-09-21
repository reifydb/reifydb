# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

.PHONY: test-query test-query-dev

test-query:
	@echo "🧮 Running query layout tests..."
	cd $(TEST_QUERY_DIR) && $(MAKE) test MATRIX=$(or $(MATRIX),full)
	@$(MAKE) --no-print-directory sweep-auto

test-query-dev:
	@echo "🚀 Running fast query layout tests..."
	cd $(TEST_QUERY_DIR) && $(MAKE) test MATRIX=$(or $(MATRIX),dev)
	@$(MAKE) --no-print-directory sweep-auto
