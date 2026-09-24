# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

# =============================================================================
# Clean Targets - Remove build artifacts from all packages
# =============================================================================

.PHONY: clean clean-cargo clean-workspace clean-pkg-typescript clean-siblings

# Main clean target - cleans everything
clean: clean-siblings clean-workspace clean-cargo clean-pkg-typescript
	@echo "✅ All packages cleaned!"

clean-siblings:
	cd $(TEST_SUITE_DIR) && $(MAKE) clean
	cd $(TEST_CRATE_DIR) && $(MAKE) clean
	cd $(TEST_REGRESSION_DIR) && $(MAKE) clean
	cd $(TEST_CHAOS_DIR) && $(MAKE) clean
	cd $(TEST_QUERY_DIR) && $(MAKE) clean

# Clean the entire cargo target directory
clean-cargo:
	@echo "📦 Running cargo clean..."
	@cargo clean

# Clean only reifydb workspace member crates (preserves vendored dependency builds)
clean-workspace:
	@echo "📦 Cleaning workspace packages..."
	@cargo metadata --no-deps --format-version 1 --offline 2>/dev/null \
		| python3 -c "import json,sys; [print(p['name']) for p in json.load(sys.stdin)['packages']]" \
		| while read -r pkg; do cargo clean -p "$$pkg" --release 2>/dev/null || true; done

# Clean pkg/typescript packages
clean-pkg-typescript:
	@echo "📦 Cleaning pkg/typescript packages..."
	@if [ -d "pkg/typescript" ]; then \
		echo "  Cleaning pkg/typescript"; \
		cd pkg/typescript && rm -rf node_modules */node_modules */*/node_modules 2>/dev/null || true; \
	fi