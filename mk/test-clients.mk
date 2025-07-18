# =============================================================================
# Test Clients (typescript, etc.)
# =============================================================================

# List of available test clients
TEST_CLIENTS := \
	typescript

.PHONY: testclient $(TEST_CLIENTS)

# Run all test clients in parallel
testclient:
	@echo "🧪 Running all test clients in parallel..."
	$(MAKE) -j$(shell nproc) $(TEST_CLIENTS)

# Individual test client targets
$(TEST_CLIENTS):
	@if [ -d "$(TEST_CLIENT_DIR)/$@" ]; then \
		echo "🔍 Running $@ tests in $(TEST_CLIENT_DIR)/$@ ..."; \
		cd $(TEST_CLIENT_DIR)/$@ && $(MAKE) test; \
	else \
		echo "⚠️ Skipping $@ – directory $(TEST_CLIENT_DIR)/$@ not found"; \
	fi