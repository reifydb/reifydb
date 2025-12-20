# =============================================================================
# Benchmark Testing Makefile
# =============================================================================

# Benchmark targets
.PHONY: bench bench-all bench-store bench-transaction

bench: bench-all

bench-all:
	@echo "🏃‍♂️ Running all ReifyDB benchmarks..."
	cargo bench -p reifydb-benches $(CARGO_OFFLINE)

bench-store:
	@echo "🏃‍♂️ Running store benchmarks..."
	cargo bench -p reifydb-benches --bench store $(CARGO_OFFLINE)

bench-transaction:
	@echo "🏃‍♂️ Running transaction benchmarks..."
	cargo bench -p reifydb-benches --bench transaction $(CARGO_OFFLINE)

# Benchmark utilities
.PHONY: bench-baseline bench-compare bench-report

bench-baseline:
	@echo "💾 Saving benchmark baseline..."
	cargo bench -p reifydb-benches $(CARGO_OFFLINE) -- --save-baseline main

bench-compare:
	@echo "📊 Comparing benchmarks to baseline..."
	cargo bench -p reifydb-benches $(CARGO_OFFLINE) -- --baseline main

bench-report:
	@echo "📈 Opening benchmark reports..."
	@if [ -d "target/criterion" ]; then \
		xdg-open target/criterion/report/index.html 2>/dev/null || \
		open target/criterion/report/index.html 2>/dev/null || \
		echo "Reports available at: target/criterion/report/index.html"; \
	else \
		echo "No benchmark reports found. Run 'make bench' first."; \
	fi
