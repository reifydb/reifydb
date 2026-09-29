# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

# =============================================================================
# TypeScript Package Testing
# =============================================================================

.PHONY: test-pkg-typescript start-testcontainer

# Run TypeScript tests against a testcontainer built from this workspace
test-pkg-typescript: build-wasm
	@echo "🧪 Running TypeScript tests..."
	@if [ -d "pkg/typescript" ]; then \
		./scripts/test-pkg-typescript.sh; \
	else \
		echo "⚠️ Skipping TypeScript tests – directory pkg/typescript not found"; \
	fi
	@$(MAKE) --no-print-directory sweep-auto

# Start the test container
start-testcontainer:
	@echo "🚀 Starting reifydb test container..."
	@docker rm -f reifydb-test 2>/dev/null || true
	@docker run -d \
		--name reifydb-test \
		-p 18090:18090 \
		-p 18091:18091 \
		reifydb/testcontainer

# Alias for backward compatibility
.PHONY: testpkg
testpkg: test-pkg-typescript
