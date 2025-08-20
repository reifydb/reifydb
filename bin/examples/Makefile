# ReifyDB Examples Makefile
# Run any example with: make <example-name>

.PHONY: help
help:
	@echo "ReifyDB Examples - Available targets:"
	@echo ""
	@echo "Basic Examples:"
	@echo "  make hello-world         - Simple hello world example"
	@echo "  make basic-tables        - Create and define table schemas"
	@echo ""
	@echo "Utility targets:"
	@echo "  make list                - List all available example binaries"
	@echo "  make clean               - Clean build artifacts"
	@echo "  make build               - Build all examples"

# Basic Examples
.PHONY: hello-world
hello-world:
	cargo run --bin hello-world

.PHONY: basic-tables
basic-tables:
	cargo run --bin basic-tables

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