# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

# =============================================================================
# Workspace Testing (all workspace tests)
# =============================================================================

# Number of times to repeat test-workspace. Default 1; set N=K to repeat K times,
# stopping at the first failure and reporting the iteration number.
N ?= 1

.PHONY: test-workspace
test-workspace:
	@total_start=$$(date +%s); \
	for i in $$(seq 1 $(N)); do \
		iter_start=$$(date +%s); \
		if [ $(N) -gt 1 ]; then \
			echo ""; \
			echo "==> [iter $$i/$(N)] starting at $$(date -Iseconds)"; \
		fi; \
		echo "🧪 Running workspace tests..."; \
		MAKEFLAGS= cargo nextest run --release --workspace --lib --bins --tests --examples --no-fail-fast --status-level fail --final-status-level fail $(CARGO_OFFLINE) || { \
			rc=$$?; \
			iter_end=$$(date +%s); \
			total_end=$$(date +%s); \
			echo ""; \
			echo "==> FAILED at iteration $$i/$(N) (exit=$$rc, iter=$$((iter_end - iter_start))s, elapsed=$$((total_end - total_start))s)"; \
			exit $$rc; \
		}; \
		echo "📚 Running doc tests..."; \
		MAKEFLAGS= cargo test --release --workspace --doc $(CARGO_OFFLINE) || { \
			rc=$$?; \
			iter_end=$$(date +%s); \
			total_end=$$(date +%s); \
			echo ""; \
			echo "==> FAILED at iteration $$i/$(N) (exit=$$rc, iter=$$((iter_end - iter_start))s, elapsed=$$((total_end - total_start))s)"; \
			exit $$rc; \
		}; \
		iter_end=$$(date +%s); \
		if [ $(N) -gt 1 ]; then \
			echo "==> [iter $$i/$(N)] OK ($$((iter_end - iter_start))s)"; \
		fi; \
	done; \
	if [ $(N) -gt 1 ]; then \
		total_end=$$(date +%s); \
		echo ""; \
		echo "==> ALL $(N) iterations PASSED (total=$$((total_end - total_start))s)"; \
	fi
