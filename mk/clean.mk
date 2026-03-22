# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 ReifyDB

# =============================================================================
# Clean Targets - Remove build artifacts from all packages
# =============================================================================

.PHONY: clean clean-workspace clean-pkg-typescript

# Main clean target - cleans everything
clean: clean-workspace clean-pkg-typescript
	@echo "✅ All packages cleaned!"

# Clean entire workspace (includes crates/, bin/, and pkg/rust/)
clean-workspace:
	@echo "📦 Cleaning workspace packages..."
	cargo clean

# Clean pkg/typescript packages
clean-pkg-typescript:
	@echo "📦 Cleaning pkg/typescript packages..."
	@if [ -d "pkg/typescript" ]; then \
		echo "  Cleaning pkg/typescript"; \
		cd pkg/typescript && rm -rf node_modules */node_modules */*/node_modules 2>/dev/null || true; \
	fi