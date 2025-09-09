# =============================================================================
# Test Suites (smoke, compatibility, diagnostic, functional, integration, stress)
# =============================================================================

# List of available test suites
TEST_SUITES := \
	smoke \
	compatibility \
	diagnostic \
	error \
	functional \
	flow \
	integration \
	stress

.PHONY: testsuite testsuite-dev $(TEST_SUITES)

# Run all test suites in parallel
testsuite:
	@echo "🔍 Running all test suites in parallel..."
	$(MAKE) -j$(shell nproc) $(TEST_SUITES)

# Run fast development tests for all test suites
testsuite-dev:
	@echo "🚀 Running fast development tests for all test suites..."
	cd $(TEST_SUITE_DIR) && $(MAKE) test-dev

# Individual test suite targets
$(TEST_SUITES):
	@if [ -d "$(TEST_SUITE_DIR)/$@" ]; then \
		echo "🔍 Running $@ tests in $(TEST_SUITE_DIR)/$@ ..."; \
		cd $(TEST_SUITE_DIR)/$@ && cargo nextest run --no-fail-fast $(CARGO_OFFLINE); \
	else \
		echo "⚠️ Skipping $@ – directory $(TEST_SUITE_DIR)/$@ not found"; \
	fi