# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

# =============================================================================
# Benchmark Testing Makefile
# =============================================================================

# Benchmark targets
.PHONY: bench bench-all bench-query bench-query-help bench-txn bench-txn-help
.PHONY: bench-watermark bench-watermark-help

bench: bench-all

bench-all:
	@echo "🏃‍♂️ Running all ReifyDB benchmarks..."
	cargo bench -p reifydb-benches $(CARGO_OFFLINE)

# Passed to the benchmarks as environment variables. Unset ones export as empty strings, which every
# benchmark reads as "use the default" - so listing a variable here never pins its axis.
export SCENARIO QUERY TRANSPORTS IDENTITIES SCALES THREADS
export ITERATIONS BUDGET_SECONDS WARMUP REPEATS WIRE_FORMAT
export LAYOUTS MATRIX
export PHASES PIPELINE_OPS BURST_OPS WAIT_OPS

bench-query:
	@echo "🏃‍♂️ Running query pipeline benchmarks..."
	cargo bench -p reifydb-benches --bench query $(CARGO_OFFLINE)

bench-query-help:
	@echo ""
	@echo "  make bench-query [VAR=value ...]"
	@echo ""
	@echo "  Matrix axes (comma-separated; every combination is run)"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-16s %s\n" "SCENARIO"   "one scenario name (default: all)"
	@printf "  %-16s %s\n" "QUERY"      "one named query within the scenario (default: all)"
	@printf "  %-16s %s\n" "TRANSPORTS" "embedded,http,ws,grpc (default: all four)"
	@printf "  %-16s %s\n" "IDENTITIES" "anonymous,root,system (default: all three)"
	@printf "  %-16s %s\n" "SCALES"     "seeded row counts (default: 10000,100000)"
	@printf "  %-16s %s\n" "THREADS"    "concurrent workers (default: 1,4,16)"
	@echo ""
	@echo "  Tuning knobs"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-16s %s\n" "ITERATIONS"     "requests per sample (default: 20000)"
	@printf "  %-16s %s\n" "BUDGET_SECONDS" "wall-clock cap per sample (default: 3)"
	@printf "  %-16s %s\n" "WARMUP"         "unmeasured requests first (default: 200)"
	@printf "  %-16s %s\n" "REPEATS"        "samples per cell, median wins (default: 5)"
	@printf "  %-16s %s\n" "WIRE_FORMAT"    "rbcf or frames; grpc requires rbcf (default: rbcf)"
	@echo ""
	@echo "  Examples"
	@echo "  ───────────────────────────────────────────────────────────────"
	@echo "  make bench-query TRANSPORTS=embedded"
	@echo "  make bench-query SCENARIO=scan IDENTITIES=root,anonymous THREADS=1"
	@echo "  make bench-query SCENARIO=read TRANSPORTS=http SCALES=100000 REPEATS=1"
	@echo ""
	@echo "  Each result prints a repro= line pinning that exact cell, so a slow"
	@echo "  combination can be rerun on its own as many times as needed."
	@echo ""

bench-txn:
	@echo "🏃‍♂️ Running transaction benchmarks..."
	cargo bench -p reifydb-benches --bench txn $(CARGO_OFFLINE)

bench-txn-help:
	@echo ""
	@echo "  make bench-txn [VAR=value ...]"
	@echo ""
	@echo "  Matrix axes (comma-separated)"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-16s %s\n" "LAYOUTS" "shared_table,table_per_thread (default: both, or"
	@printf "  %-16s %s\n" ""        "shared_table alone once THREADS or MATRIX narrows the run)"
	@printf "  %-16s %s\n" "THREADS" "concurrent writers (default: 1,2,4,8,12,16,24,32)"
	@echo ""
	@echo "  Tuning knobs"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-16s %s\n" "ITERATIONS" "transactions per thread, max 1000000 (default: 50000)"
	@printf "  %-16s %s\n" "REPEATS"    "samples per cell, median wins (default: 5)"
	@printf "  %-16s %s\n" "MATRIX"     "set to sweep only the contended 16,24,32 range"
	@echo ""
	@echo "  Examples"
	@echo "  ───────────────────────────────────────────────────────────────"
	@echo "  make bench-txn THREADS=16"
	@echo "  make bench-txn LAYOUTS=table_per_thread THREADS=8,16 ITERATIONS=5000"
	@echo ""

bench-watermark:
	@echo "🏃‍♂️ Running watermark benchmarks..."
	cargo bench -p reifydb-benches --bench watermark $(CARGO_OFFLINE)

bench-watermark-help:
	@echo ""
	@echo "  make bench-watermark [VAR=value ...]"
	@echo ""
	@echo "  Matrix axes (comma-separated)"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-16s %s\n" "PHASES" "pipeline,pipeline_with_advancer,burst,burst_with_advancer,"
	@printf "  %-16s %s\n" ""       "wait_fast_path,mixed_poll (default: all six)"
	@printf "  %-16s %s\n" "THREADS" "overrides each selected phase's own sweep"
	@echo ""
	@echo "  Tuning knobs"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-16s %s\n" "PIPELINE_OPS" "ops for pipeline and mixed_poll (default: 1000000)"
	@printf "  %-16s %s\n" "BURST_OPS"    "ops per thread for burst phases (default: 5000)"
	@printf "  %-16s %s\n" "WAIT_OPS"     "ops for wait_fast_path (default: 1000000)"
	@echo ""
	@echo "  Examples"
	@echo "  ───────────────────────────────────────────────────────────────"
	@echo "  make bench-watermark PHASES=pipeline THREADS=16"
	@echo "  make bench-watermark PHASES=burst,burst_with_advancer BURST_OPS=1000"
	@echo ""

# Benchmark utilities
.PHONY: bench-report

bench-report:
	@echo "📈 Benchmark results:"
	@if [ -d "$(CARGO_TARGET_DIR)/bench-results" ]; then \
		ls -t "$(CARGO_TARGET_DIR)/bench-results" | head -20; \
	else \
		echo "No benchmark results found. Run 'make bench' first."; \
	fi
