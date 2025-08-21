# Build and run all examples
.PHONY: test-examples
test-examples: build-examples run-examples

# Build all examples in release mode
.PHONY: build-examples
build-examples:
	@echo "Building examples (release mode)..."
	cd bin/examples && cargo build --release --bins

# Run all examples in release mode
.PHONY: run-examples
run-examples:
	@echo "Running examples (release mode)..."
	@for binary in $$(cd bin/examples && cargo build --release --bins --message-format=json 2>/dev/null | jq -r 'select(.reason=="compiler-artifact" and .target.kind[0]=="bin") | .target.name' | sort -u); do \
		echo "Running example: $$binary"; \
		(cd bin/examples && cargo run --release --bin $$binary) || exit 1; \
	done

# Clean examples build artifacts
.PHONY: clean-examples
clean-examples:
	@echo "Cleaning examples..."
	cd bin/examples && cargo clean