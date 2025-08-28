# ReifyDB Benchmarks Makefile
# Run benchmarks with: make <benchmark-name>

.PHONY: help
help:
	@echo "ReifyDB Benchmarks - Available targets:"
	@echo ""
	@echo "Engine Benchmarks:"
	@echo "  make bench-memory-optimistic - Memory optimistic transaction benchmarks"
	@echo ""
	@echo "RQL Pipeline Benchmarks:"
	@echo "  make bench-rql-tokenize      - RQL tokenization performance"
	@echo "  make bench-rql-parse         - RQL parsing performance" 
	@echo "  make bench-rql-logical       - RQL logical planning performance"
	@echo ""
	@echo "Utility targets:"
	@echo "  make bench-all               - Run all benchmarks"
	@echo "  make bench-baseline          - Run benchmarks and save as baseline"
	@echo "  make bench-compare           - Run benchmarks and compare to baseline"
	@echo "  make clean                   - Clean build artifacts"
	@echo "  make build                   - Build all benchmarks"
	@echo ""
	@echo "Options:"
	@echo "  BENCH_FLAGS='--verbose'      - Pass additional flags to cargo bench"
	@echo "  BASELINE='main'              - Set baseline name for comparisons"

# Engine benchmarks
.PHONY: bench-memory-optimistic
bench-memory-optimistic:
	cargo bench --bench engine-memory-optimistic $(BENCH_FLAGS)

# Run with verbose outlier information
.PHONY: bench-memory-optimistic-verbose
bench-memory-optimistic-verbose:
	cargo bench --bench engine-memory-optimistic -- --verbose

# RQL pipeline benchmarks
.PHONY: bench-rql-tokenize
bench-rql-tokenize:
	cargo bench --bench rql-tokenize $(BENCH_FLAGS)

.PHONY: bench-rql-parse
bench-rql-parse:
	cargo bench --bench rql-parse $(BENCH_FLAGS)

.PHONY: bench-rql-logical
bench-rql-logical:
	cargo bench --bench rql-logical $(BENCH_FLAGS)

# Convenience aliases
.PHONY: memory-optimistic
memory-optimistic: bench-memory-optimistic

# Run all benchmarks
.PHONY: bench-all
bench-all:
	@echo "Running all ReifyDB benchmarks..."
	@echo ""
	@failed_benches=""; \
	first_error=0; \
	echo "=== Engine Benchmarks ==="; \
	echo ""; \
	for bench in bench-memory-optimistic bench-rql-tokenize bench-rql-parse bench-rql-logical; do \
		echo "--- Running $$bench ---"; \
		if $(MAKE) $$bench; then \
			true; \
		else \
			exit_code=$$?; \
			[ $$first_error -eq 0 ] && first_error=$$exit_code; \
			failed_benches="$$failed_benches $$bench"; \
		fi; \
		echo ""; \
	done; \
	echo "========================================"; \
	echo "=== BENCHMARK SUMMARY ==="; \
	echo "========================================"; \
	if [ -z "$$failed_benches" ]; then \
		echo "✓ All benchmarks completed successfully!"; \
		echo "Results are available in target/criterion/"; \
	else \
		echo "✗ Failed benchmarks:"; \
		for bench in $$failed_benches; do \
			echo "  - $$bench"; \
		done; \
		echo "Exit code: $$first_error"; \
		exit $$first_error; \
	fi

# Save current benchmark results as baseline
.PHONY: bench-baseline
bench-baseline:
	@BASELINE_NAME=$${BASELINE:-main}; \
	echo "Saving benchmark results as baseline: $$BASELINE_NAME"; \
	cargo bench --bench engine-memory-optimistic -- --save-baseline $$BASELINE_NAME

# Compare current benchmarks to baseline
.PHONY: bench-compare
bench-compare:
	@BASELINE_NAME=$${BASELINE:-main}; \
	echo "Comparing benchmark results to baseline: $$BASELINE_NAME"; \
	cargo bench --bench engine-memory-optimistic -- --baseline $$BASELINE_NAME

# Open benchmark reports in browser
.PHONY: bench-report
bench-report:
	@echo "Opening benchmark reports in browser..."
	@if [ -d "target/criterion" ]; then \
		if command -v xdg-open >/dev/null 2>&1; then \
			xdg-open target/criterion/report/index.html; \
		elif command -v open >/dev/null 2>&1; then \
			open target/criterion/report/index.html; \
		else \
			echo "Please open target/criterion/report/index.html in your browser"; \
		fi \
	else \
		echo "No benchmark reports found. Run 'make bench-all' first."; \
	fi

# List available benchmarks
.PHONY: list
list:
	@echo "Available benchmarks:"
	@cargo metadata --format-version 1 --no-deps | \
		jq -r '.packages[] | select(.name == "bench") | .targets[] | select(.kind[] == "bench") | .name' | \
		sort | \
		sed 's/^/  - /'

# Utility targets
.PHONY: clean
clean:
	cargo clean

.PHONY: build
build:
	cargo build --benches

# Development helpers
.PHONY: check
check:
	cargo check --benches

.PHONY: test
test:
	cargo test --lib