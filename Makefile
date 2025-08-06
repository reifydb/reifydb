# =============================================================================
# ReifyDB Main Makefile
# =============================================================================

# Configuration
TEST_SUITE_DIR ?= ../testsuite
TEST_PKG_DIR := ./pkg

# Load .env file if it exists
ifneq (,$(wildcard ./.env))
    include ./.env
    export
endif

# Default target when just running 'make'
.DEFAULT_GOAL := help

# =============================================================================
# Help & Documentation
# =============================================================================

.PHONY: help
help:
	@echo "🚀 ReifyDB Development Commands"
	@echo "================================"
	@echo ""
	@echo "📋 Main Targets:"
	@echo "  make help          Show this help message"
	@echo "  make all           Full CI/CD pipeline (check, clean, build, test, push)"
	@echo ""
	@echo "🧪 Testing:"
	@echo "  make test-dev      Fast development tests (local + embedded_blocking only)"
	@echo "  make test          Full test suite (local + all test-suites + test clients)"
	@echo "  make test-full     Same as 'make test'"
	@echo "  make test-local    Run only local reifydb crate tests"
	@echo ""
	@echo "🔧 Test Components:"
	@echo "  make testsuite     Run all test suites (smoke, compatibility, diagnostic, functional, stress)"
	@echo "  make testsuite-dev Run fast development tests for all test suites"
	@echo "  make testpkg    	Run test packages (typescript)"
	@echo ""
	@echo "🏗️  Building:"
	@echo "  make build         Build release version"
	@echo "  make clean         Clean all reifydb packages"
	@echo ""
	@echo "🐳 Docker:"
	@echo "  make build-testcontainer   Build test container"
	@echo "  make push-testcontainer    Push test container to registry"
	@echo ""
	@echo "📊 Other:"
	@echo "  make coverage      Generate test coverage report"
	@echo "  make check         Check for uncommitted changes"
	@echo "  make push          Push changes to git (after check)"
	@echo ""
	@echo "💡 Quick Start:"
	@echo "  make test-dev      # Fast feedback during development"
	@echo "  make test          # Full test before committing"

# =============================================================================
# Main Pipeline Targets
# =============================================================================

.PHONY: all
all: check clean build-testcontainer test-full build push-testcontainer push

.PHONY: check
check:
	@if ! git diff-index --quiet HEAD --; then \
		echo "Error: You have uncommitted changes. Please commit or stash them before pushing."; \
		exit 1; \
	fi

.PHONY: clean
clean:
	@echo "🧹 Cleaning all reifydb packages..."
	@for pkg in $$(cargo metadata --format-version 1 --no-deps | jq -r '.packages[].name' | grep '^reifydb-'); do \
		echo "  Cleaning $$pkg"; \
		cargo clean -p $$pkg; \
	done

.PHONY: push
push: check
	@echo "📤 Pushing changes to git..."
	git push

# =============================================================================
# Testing Targets
# =============================================================================

.PHONY: test test-full test-dev
test: test-full

test-full: test-local testsuite testpkg
	@echo "✅ All tests completed successfully!"

test-dev: test-local testsuite-dev
	@echo "🚀 Development tests completed!"

# Include testing sub-makefiles
include mk/test-local.mk
include mk/test-pkg.mk
include mk/test-suites.mk

# =============================================================================
# Build Targets
# =============================================================================

.PHONY: build
build:
	@echo "🏗️  Building release version..."
	cargo build --release

.PHONY: coverage
coverage:
	@echo "📊 Generating test coverage report..."
	cargo tarpaulin -o html --all --output-dir target/coverage

# =============================================================================
# Docker Targets
# =============================================================================

.PHONY: build-testcontainer push-testcontainer
build-testcontainer:
	@echo "🐳 Building test container..."
	docker build --no-cache -f bin/testcontainer/Dockerfile -t reifydb/testcontainer .

push-testcontainer: check
	@echo "📤 Pushing test container to registry..."
	docker push reifydb/testcontainer