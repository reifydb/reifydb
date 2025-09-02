# =============================================================================
# Benchmark Testing Makefile
# =============================================================================

# Benchmark targets
.PHONY: bench bench-all bench-memory-optimistic bench-rql bench-rql-tokenize bench-rql-parse bench-rql-logical

bench: bench-all

bench-all:
	@echo "🏃‍♂️ Running all ReifyDB benchmarks..."
	@cd ../reifydb-bench && $(MAKE) bench-all

bench-memory-optimistic:
	@echo "🏃‍♂️ Running memory optimistic transaction benchmarks..."
	@cd ../reifydb-bench && $(MAKE) bench-memory-optimistic

bench-rql: bench-rql-tokenize bench-rql-parse bench-rql-logical

bench-rql-tokenize:
	@echo "🏃‍♂️ Running RQL tokenization benchmarks..."
	@cd ../reifydb-bench && $(MAKE) bench-rql-tokenize

bench-rql-parse:
	@echo "🏃‍♂️ Running RQL parsing benchmarks..."
	@cd ../reifydb-bench && $(MAKE) bench-rql-parse

bench-rql-logical:
	@echo "🏃‍♂️ Running RQL logical planning benchmarks..."
	@cd ../reifydb-bench && $(MAKE) bench-rql-logical

# Benchmark utilities
.PHONY: bench-baseline bench-compare bench-report

bench-baseline:
	@echo "💾 Saving benchmark baseline..."
	@cd ../reifydb-bench && $(MAKE) bench-baseline

bench-compare:
	@echo "📊 Comparing benchmarks to baseline..."
	@cd ../reifydb-bench && $(MAKE) bench-compare

bench-report:
	@echo "📈 Opening benchmark reports..."
	@cd ../reifydb-bench && $(MAKE) bench-report

