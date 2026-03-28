# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 ReifyDB

# =============================================================================
# TypeScript Package Testing
# =============================================================================

.PHONY: test-pkg-typescript ensure-testcontainer start-testcontainer

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

# Run TypeScript tests
test-pkg-typescript: ensure-testcontainer
	@echo "🧪 Running TypeScript tests..."
	@if [ -d "pkg/typescript" ]; then \
		cd pkg/typescript && $(MAKE) test; \
	else \
		echo "⚠️ Skipping TypeScript tests – directory pkg/typescript not found"; \
	fi

# Start the test container
start-testcontainer:
	@echo "🚀 Starting reifydb test container..."
	@docker rm -f reifydb-test 2>/dev/null || true
	@docker run -d \
		--name reifydb-test \
		-p 8090:8090 \
		-p 8091:8091 \
		reifydb/testcontainer

# Alias for backward compatibility
.PHONY: testpkg
testpkg: test-pkg-typescript