# =============================================================================
# ReifyDB Main Makefile
# =============================================================================

# Configuration
TEST_SUITE_DIR ?= ../testsuite
TEST_PKG_DIR := ./pkg

# Check if vendor directory exists and set offline flag
ifneq (,$(wildcard ./db/vendor))
    CARGO_OFFLINE := --offline
else
    CARGO_OFFLINE :=
endif

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
	@echo ""
	@echo "  ╔═══════════════════════════════════════════════════════════════╗"
	@echo "  ║                  🚀 ReifyDB Development Commands              ║"
	@echo "  ╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "  📋 Main Targets"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-25s %s\n" "help" "Show this help message"
	@printf "  %-25s %s\n" "all" "Full CI/CD pipeline (check, clean, build, test, push)"
	@echo ""
	@echo "  🧪 Testing"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-25s %s\n" "test-dev" "Fast development tests (db + embedded_blocking only)"
	@printf "  %-25s %s\n" "test" "Full test suite (db + all test-suites + test clients)"
	@printf "  %-25s %s\n" "test-full" "Same as 'make test'"
	@printf "  %-25s %s\n" "test-db" "Run only database workspace tests"
	@echo ""
	@echo "  🔧 Test Components"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-25s %s\n" "test-suite" "Run all test suites (smoke, compatibility, etc.)"
	@printf "  %-25s %s\n" "test-suite-dev" "Run fast development tests for all test suites"
	@printf "  %-25s %s\n" "test-pkg-rust" "Run test packages (rust)"
	@printf "  %-25s %s\n" "test-pkg-typescript" "Run test packages (typescript)"
	@printf "  %-25s %s\n" "test-examples" "Build and run all examples"
	@echo ""
	@echo "  🏎️  Benchmarking"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-25s %s\n" "bench" "Run all performance benchmarks"
	@echo ""
	@echo "  🏗️  Building"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-25s %s\n" "build" "Build release version"
	@printf "  %-25s %s\n" "clean" "Clean all reifydb packages"
	@printf "  %-25s %s\n" "format" "Format all code with rustfmt (nightly)"
	@echo ""
	@echo "  🐳 Docker"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-25s %s\n" "build-testcontainer" "Build test container"
	@printf "  %-25s %s\n" "push-testcontainer" "Push test container to registry"
	@echo ""
	@echo "  📊 Other"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-25s %s\n" "check" "Check for uncommitted changes"
	@printf "  %-25s %s\n" "push" "Push changes to git (after check)"
	@echo ""
	@echo "  💡 Quick Start"
	@echo "  ───────────────────────────────────────────────────────────────"
	@echo "  make test-dev      # Fast feedback during development"
	@echo "  make test          # Full test before committing"
	@echo ""

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

# Clean target is defined in mk/clean.mk

.PHONY: push
push: check
	@echo "📤 Pushing changes to git..."
	git push

# =============================================================================
# Testing Targets
# =============================================================================

.PHONY: test test-full test-dev
test: test-full

test-full: test-db test-pkg-rust test-examples test-suite test-pkg-typescript
	@echo "✅ All tests completed successfully!"

test-dev: test-db test-pkg-rust test-examples test-suite-dev
	@echo "🚀 Development tests completed!"

# Include testing sub-makefiles
include mk/test-db.mk
include mk/test-suites.mk
include mk/test-pkg-rust.mk
include mk/test-pkg-typescript.mk
include mk/test-examples.mk

# Only include benchmark makefile when benchmark targets are being run
ifneq ($(filter bench bench-% ,$(MAKECMDGOALS)),)
include mk/test-bench.mk
endif

include mk/vendor.mk
include mk/clean.mk
include mk/build.mk
include mk/format.mk
include mk/container.mk