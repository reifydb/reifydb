# =============================================================================
# Benchmark Testing Makefile
# =============================================================================

# Benchmark targets
.PHONY: bench bench-all bench-memory-optimistic bench-rql bench-rql-tokenize bench-rql-parse bench-rql-logical

bench: bench-all

bench-all:
	@echo "🏃‍♂️ Running all ReifyDB benchmarks..."
	@cd bin/bench && $(MAKE) bench-all

bench-memory-optimistic:
	@echo "🏃‍♂️ Running memory optimistic transaction benchmarks..."
	@cd bin/bench && $(MAKE) bench-memory-optimistic

bench-rql: bench-rql-tokenize bench-rql-parse bench-rql-logical

bench-rql-tokenize:
	@echo "🏃‍♂️ Running RQL tokenization benchmarks..."
	@cd bin/bench && $(MAKE) bench-rql-tokenize

bench-rql-parse:
	@echo "🏃‍♂️ Running RQL parsing benchmarks..."
	@cd bin/bench && $(MAKE) bench-rql-parse

bench-rql-logical:
	@echo "🏃‍♂️ Running RQL logical planning benchmarks..."
	@cd bin/bench && $(MAKE) bench-rql-logical

# Benchmark utilities
.PHONY: bench-baseline bench-compare bench-report bench-verbose

bench-baseline:
	@echo "💾 Saving benchmark baseline..."
	@cd bin/bench && $(MAKE) bench-baseline

bench-compare:
	@echo "📊 Comparing benchmarks to baseline..."
	@cd bin/bench && $(MAKE) bench-compare

bench-report:
	@echo "📈 Opening benchmark reports..."
	@cd bin/bench && $(MAKE) bench-report

bench-verbose:
	@echo "🔍 Running benchmarks with verbose outlier information..."
	@cd bin/bench && $(MAKE) bench-memory-optimistic-verbose