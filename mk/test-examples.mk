# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

# Build and run all examples
.PHONY: test-examples
test-examples: build-examples run-examples
	@$(MAKE) --no-print-directory sweep-auto

.PHONY: build-examples
build-examples:
	@echo "Building examples ($(PROFILE))..."
	cd pkg/rust/examples && MAKEFLAGS= cargo build --profile $(PROFILE) --bins $(CARGO_OFFLINE)

.PHONY: run-examples
run-examples:
	@echo "Running examples in order ($(PROFILE))..."
	@cd pkg/rust/examples && make run-all PROFILE=$(PROFILE)

# Clean examples build artifacts
.PHONY: clean-examples
clean-examples:
	@echo "Cleaning examples..."
	cd pkg/rust/examples && cargo clean $(CARGO_OFFLINE)
