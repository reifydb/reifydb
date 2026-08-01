# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

# =============================================================================
# Benchmark Testing Makefile
# =============================================================================

# Benchmark targets
.PHONY: bench bench-all bench-watermark bench-txn

bench: bench-all

bench-all:
	@echo "🏃‍♂️ Running all ReifyDB benchmarks..."
	cargo bench -p reifydb-benches $(CARGO_OFFLINE)

bench-watermark:
	@echo "🏃‍♂️ Running watermark benchmarks..."
	cargo bench -p reifydb-benches --bench watermark $(CARGO_OFFLINE)

bench-txn:
	@echo "🏃‍♂️ Running transaction benchmarks..."
	cargo bench -p reifydb-benches --bench txn $(CARGO_OFFLINE)

# Benchmark utilities
.PHONY: bench-report

bench-report:
	@echo "📈 Benchmark results:"
	@if [ -d "$(CARGO_TARGET_DIR)/bench-results" ]; then \
		ls -t "$(CARGO_TARGET_DIR)/bench-results" | head -20; \
	else \
		echo "No benchmark results found. Run 'make bench' first."; \
	fi
