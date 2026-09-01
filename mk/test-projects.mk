# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

# =============================================================================
# Self-Hostable Project Testing (pkg/projects/*)
# =============================================================================

.PHONY: test-projects

# Run 'make test' in every pkg/projects/* directory that defines one
test-projects:
	@for dir in pkg/projects/*/; do \
		if [ -f "$$dir/Makefile" ]; then \
			echo "🧪 Running tests in $$dir..."; \
			$(MAKE) -C "$$dir" test || exit 1; \
		fi; \
	done
	@echo "✅ All pkg/projects tests completed!"
	@$(MAKE) --no-print-directory sweep-auto
