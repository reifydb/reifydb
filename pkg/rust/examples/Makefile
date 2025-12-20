# ReifyDB Examples Makefile
# Run any example with: make <example-name>

.PHONY: help
help:
	@echo "ReifyDB Examples - Available targets:"
	@echo ""
	@echo "Basic Examples:"
	@echo "  make basic-hello-world   - Simple hello world example"
	@echo "  make basic-tables        - Create and define table, namespaces"
	@echo ""
	@echo "RQL (Query Language) Examples:"
	@echo "  make rql-from            - FROM operator: loading data from inline arrays and tables"
	@echo "  make rql-map             - MAP operator: projection and computed fields"
	@echo "  make rql-filter          - FILTER operator: filtering data with conditions"
	@echo "  make rql-sort            - SORT operator: sorting results"
	@echo "  make rql-take            - TAKE operator: limiting results"
	@echo "  make rql-aggregate       - AGGREGATE operator: aggregation functions"
	@echo "  make rql-join            - JOIN operator: different join types"
	@echo "  make rql-arithmetic      - Arithmetic expressions in queries"
	@echo "  make rql-comparison      - Comparison operators (==, !=, <, >, <=, >=)"
	@echo "  make rql-logical         - Logical operators (and, or, not, xor)"
	@echo ""
	@echo "Interceptor Examples:"
	@echo "  make intercept-table-view - Table and view interceptors (pre/post hooks)"
	@echo ""
	@echo "Utility targets:"
	@echo "  make run-all             - Run all examples in order (by directory, then by filename)"
	@echo "  make list                - List all available example binaries"
	@echo "  make clean               - Clean build artifacts"
	@echo "  make build               - Build all examples"

# Basic Examples
.PHONY: basic-hello-world
basic-hello-world:
	cargo run --bin basic-hello-world

.PHONY: basic-tables
basic-tables:
	cargo run --bin basic-tables

# RQL Examples
.PHONY: rql-from
rql-from:
	cargo run --bin rql-from

.PHONY: rql-map
rql-map:
	cargo run --bin rql-map

.PHONY: rql-filter
rql-filter:
	cargo run --bin rql-filter

.PHONY: rql-sort
rql-sort:
	cargo run --bin rql-sort

.PHONY: rql-take
rql-take:
	cargo run --bin rql-take

.PHONY: rql-aggregate
rql-aggregate:
	cargo run --bin rql-aggregate

.PHONY: rql-join
rql-join:
	cargo run --bin rql-join

.PHONY: rql-arithmetic
rql-arithmetic:
	cargo run --bin rql-arithmetic

.PHONY: rql-comparison
rql-comparison:
	cargo run --bin rql-comparison

.PHONY: rql-logical
rql-logical:
	cargo run --bin rql-logical

# Interceptor Examples
.PHONY: intercept-table-view
intercept-table-view:
	RUST_LOG=info cargo run --bin intercept-table-view

# Define example groups
BASIC_EXAMPLES := basic-hello-world basic-tables
RQL_EXAMPLES := rql-from rql-map rql-filter rql-sort rql-take rql-aggregate rql-join rql-arithmetic rql-comparison rql-logical
INTERCEPT_EXAMPLES := intercept-table-view

# Run all examples in order (by directory, then by filename)
.PHONY: run-all
run-all:
	@echo "Running all examples in order..."
	@echo ""
	@failed_examples=""; \
	panic_messages=""; \
	first_error=0; \
	echo "=== Basic Examples ==="; \
	echo ""; \
	for example in $(BASIC_EXAMPLES); do \
		echo "--- Running $$example ---"; \
		if cargo run --bin $$example; then \
			true; \
		else \
			exit_code=$$?; \
			output=$$(cargo run --bin $$example 2>&1); \
			panic=$$(echo "$$output" | grep -A2 "panicked at" | head -3 || echo "Unknown error"); \
			[ $$first_error -eq 0 ] && first_error=$$exit_code; \
			failed_examples="$$failed_examples $$example"; \
			panic_messages="$$panic_messages\n$$example:\n$$panic\n"; \
		fi; \
		echo ""; \
	done; \
	echo "=== RQL Examples ==="; \
	echo ""; \
	for example in $(RQL_EXAMPLES); do \
		echo "--- Running $$example ---"; \
		if cargo run --bin $$example; then \
			true; \
		else \
			exit_code=$$?; \
			output=$$(cargo run --bin $$example 2>&1); \
			panic=$$(echo "$$output" | grep -A2 "panicked at" | head -3 || echo "Unknown error"); \
			[ $$first_error -eq 0 ] && first_error=$$exit_code; \
			failed_examples="$$failed_examples $$example"; \
			panic_messages="$$panic_messages\n$$example:\n$$panic\n"; \
		fi; \
		echo ""; \
	done; \
	echo "=== Interceptor Examples ==="; \
	echo ""; \
	for example in $(INTERCEPT_EXAMPLES); do \
		echo "--- Running $$example ---"; \
		if RUST_LOG=info cargo run --bin $$example; then \
			true; \
		else \
			exit_code=$$?; \
			output=$$(RUST_LOG=info cargo run --bin $$example 2>&1); \
			panic=$$(echo "$$output" | grep -A2 "panicked at" | head -3 || echo "Unknown error"); \
			[ $$first_error -eq 0 ] && first_error=$$exit_code; \
			failed_examples="$$failed_examples $$example"; \
			panic_messages="$$panic_messages\n$$example:\n$$panic\n"; \
		fi; \
		echo ""; \
	done; \
	echo "========================================"; \
	echo "=== TEST SUMMARY ==="; \
	echo "========================================"; \
	if [ -z "$$failed_examples" ]; then \
		echo "✓ All examples passed successfully!"; \
	else \
		echo "✗ Failed examples:"; \
		for example in $$failed_examples; do \
			echo "  - $$example"; \
		done; \
		echo ""; \
		echo "Panic messages:"; \
		echo -e "$$panic_messages"; \
		echo "Exit code: $$first_error"; \
		exit $$first_error; \
	fi

# Utility targets
.PHONY: list
list:
	@echo "Available example binaries:"
	@cargo metadata --format-version 1 --no-deps | \
		jq -r '.packages[] | select(.name == "examples") | .targets[] | select(.kind[] == "bin") | .name' | \
		sort | \
		sed 's/^/  - /'

.PHONY: clean
clean:
	cargo clean

.PHONY: build
build:
	cargo build --bins