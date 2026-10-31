# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

# =============================================================================
# TypeScript Package Testing
# =============================================================================

.PHONY: test-pkg-typescript

# Run TypeScript tests against a testcontainer built from this workspace
test-pkg-typescript: build-wasm
	@echo "🧪 Running TypeScript tests..."
	@if [ -d "pkg/typescript" ]; then \
		./scripts/test-pkg-typescript.sh; \
	else \
		echo "⚠️ Skipping TypeScript tests – directory pkg/typescript not found"; \
	fi
	@$(MAKE) --no-print-directory sweep-auto

# Alias for backward compatibility
.PHONY: testpkg
testpkg: test-pkg-typescript
