# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 ReifyDB

# =============================================================================
# Clean Targets - Remove build artifacts from all packages
# =============================================================================

.PHONY: clean clean-workspace clean-pkg-typescript

# Main clean target - cleans everything
clean: clean-workspace clean-pkg-typescript
	@echo "✅ All packages cleaned!"

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