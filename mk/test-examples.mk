# Build and run all examples
.PHONY: test-examples
test-examples: build-examples run-examples

# Build all examples in release mode
.PHONY: build-examples
build-examples:
	@echo "Building examples (release mode)..."
	cd pkg/rust/examples && cargo build --bins $(CARGO_OFFLINE)

# Run all examples in release mode (in order by directory, then by filename)
.PHONY: run-examples
run-examples:
	@echo "Running examples in order (release mode)..."
	@cd pkg/rust/examples && make run-all

# Clean examples build artifacts
.PHONY: clean-examples
clean-examples:
	@echo "Cleaning examples..."
	cd pkg/rust/examples && cargo clean $(CARGO_OFFLINE)