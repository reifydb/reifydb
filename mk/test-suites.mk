# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 ReifyDB

# ============
# Test Suites
# ============

# Dynamically discover test suites from directories in TEST_SUITE_DIR
TEST_SUITES := $(shell find $(TEST_SUITE_DIR) -maxdepth 1 -type d -exec basename {} \; | grep -v testsuite | sort)

.PHONY: test-suite test-suite-dev $(TEST_SUITES)

# Run all test suites - delegate to testsuite Makefile
test-suite:
	@echo "🔍 Running all test suites..."
	cd $(TEST_SUITE_DIR) && $(MAKE) test

# Run fast development tests for all test suites - delegate to testsuite Makefile
test-suite-dev:
	@echo "🚀 Running fast development tests for all test suites..."
	cd $(TEST_SUITE_DIR) && $(MAKE) test-dev

# Individual test suite targets - delegate to testsuite Makefile
$(TEST_SUITES):
	@echo "🔍 Running $@ tests..."
	cd $(TEST_SUITE_DIR) && $(MAKE) $@