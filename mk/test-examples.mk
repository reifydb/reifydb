# Build and run all examples
.PHONY: test-examples
test-examples: build-examples run-examples

# Build all examples in release mode
.PHONY: build-examples
build-examples:
	@echo "Building examples (release mode)..."
	cd bin/examples && cargo build --release --bins

# Run all examples in release mode (in order by directory, then by filename)
.PHONY: run-examples
run-examples:
	@echo "Running examples in order (release mode)..."
	@cd bin/examples && make run-all

# Clean examples build artifacts
.PHONY: clean-examples
clean-examples:
	@echo "Cleaning examples..."
	cd bin/examples && cargo clean