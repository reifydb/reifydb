# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

.PHONY: test-reference test-reference-dev

test-reference:
	@echo "🔍 Running reference conformance suites..."
	cd $(TEST_REFERENCE_DIR) && $(MAKE) test
	@$(MAKE) --no-print-directory sweep-auto

test-reference-dev:
	@echo "🚀 Running fast reference conformance tests..."
	cd $(TEST_REFERENCE_DIR) && $(MAKE) test-dev
	@$(MAKE) --no-print-directory sweep-auto
