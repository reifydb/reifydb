# =============================================================================
# Test Clients (typescript, etc.)
# =============================================================================

# List of available test clients
TEST_PKGS := \
	typescript

.PHONY: testpkg TEST_PKGS ensure-testcontainer start-testcontainer

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

# Run all test in parallel
testpkg: ensure-testcontainer
	@echo "🧪 Running all test clients in parallel..."
	$(MAKE) -j$(shell nproc) $(TEST_PKGS)

# Start the test container
start-testcontainer:
	@echo "🚀 Starting reifydb test container..."
	@docker rm -f reifydb-test 2>/dev/null || true
	@docker run -d \
		--name reifydb-test \
		-p 8090:8090 \
		reifydb/testcontainer

# Individual test package targets
$(TEST_PKGS): ensure-testcontainer
	@if [ -d "$(TEST_PKG_DIR)/$@" ]; then \
		echo "🔍 Running $@ tests in $(TEST_PKG_DIR)/$@ ..."; \
		cd $(TEST_PKG_DIR)/$@ && $(MAKE) test; \
	else \
		echo "⚠️ Skipping $@ – directory $(TEST_PKG_DIR)/$@ not found"; \
	fi