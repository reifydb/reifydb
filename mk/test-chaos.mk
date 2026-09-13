# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

.PHONY: test-chaos

test-chaos:
	@echo "🌀 Running chaos mirror tests..."
	cd $(TEST_CHAOS_DIR) && $(MAKE) test
	@$(MAKE) --no-print-directory sweep-auto
