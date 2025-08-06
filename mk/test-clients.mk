# =============================================================================
# Test Clients (typescript, etc.)
# =============================================================================

# List of available test clients
TEST_CLIENTS := \
	typescript

.PHONY: testclient $(TEST_CLIENTS) ensure-testcontainer start-testcontainer

# Check if testcontainer is running and start it if needed
ensure-testcontainer:
	@if ! docker ps --format "table {{.Names}}" | grep -q "^reifydb-test$$"; then \
		echo "🐳 Test container not running. Starting reifydb-test..."; \
		$(MAKE) start-testcontainer; \
		echo "⏳ Waiting for test container to be ready..."; \
		sleep 3; \
	else \
		echo "✅ Test container reifydb-test is already running"; \
	fi

# Run all test clients in parallel
testclient: ensure-testcontainer
	@echo "🧪 Running all test clients in parallel..."
	$(MAKE) -j$(shell nproc) $(TEST_CLIENTS)

# Start the test container
start-testcontainer:
	@echo "🚀 Starting reifydb test container..."
	@docker rm -f reifydb-test 2>/dev/null || true
	@docker run -d \
		--name reifydb-test \
		-p 8090:8090 \
		reifydb/testcontainer

# Individual test client targets
$(TEST_CLIENTS): ensure-testcontainer
	@if [ -d "$(TEST_CLIENT_DIR)/$@" ]; then \
		echo "🔍 Running $@ tests in $(TEST_CLIENT_DIR)/$@ ..."; \
		cd $(TEST_CLIENT_DIR)/$@ && $(MAKE) test; \
	else \
		echo "⚠️ Skipping $@ – directory $(TEST_CLIENT_DIR)/$@ not found"; \
	fi