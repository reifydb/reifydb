# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 ReifyDB

# =============================================================================
# ReifyDB Main Makefile
# =============================================================================

# Configuration
TEST_SUITE_DIR ?= ../testsuite
EXTERNAL_DIR ?= ../external
TEST_PKG_DIR := ./pkg

# Check if vendor directory exists and set offline flag
ifneq (,$(wildcard ./vendor))
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
	@printf "  %-25s %s\n" "all" "Full CI/CD pipeline (format, check, clean, build, test, push)"
	@echo ""
	@echo "  🧪 Testing"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-25s %s\n" "test-dev" "Fast development tests (db + embedded_blocking only)"
	@printf "  %-25s %s\n" "test" "Full test suite (db + all test-suites + test clients)"
	@printf "  %-25s %s\n" "test-full" "Same as 'make test'"
	@printf "  %-25s %s\n" "test-workspace" "Run only workspace tests"
	@echo ""
	@echo "  🔧 Test Components"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-25s %s\n" "test-suite" "Run all test suites (smoke, compatibility, etc.)"
	@printf "  %-25s %s\n" "test-suite-dev" "Run fast development tests for all test suites"
	@printf "  %-25s %s\n" "test-pkg-rust" "Run test packages (rust)"
	@printf "  %-25s %s\n" "test-pkg-typescript" "Run test packages (typescript)"
	@printf "  %-25s %s\n" "test-examples" "Build and run all examples"
	@printf "  %-25s %s\n" "test-external" "Run external SLT regression tests"
	@echo ""
	@echo "  🏎️  Benchmarking"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-25s %s\n" "bench" "Run all performance benchmarks"
	@echo ""
	@echo "  🔬 Fuzz Testing"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-25s %s\n" "fuzz-list" "List all available fuzz targets"
	@printf "  %-25s %s\n" "fuzz-run TARGET=<name>" "Run a fuzz target (DURATION=60)"
	@printf "  %-25s %s\n" "fuzz-smoke" "Smoke-test all fuzz targets (10s each)"
	@printf "  %-25s %s\n" "fuzz-regression" "Replay all checked-in fuzz artifacts"
	@echo ""
	@echo "  🏗️  Building"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-25s %s\n" "build" "Build release version"
	@printf "  %-25s %s\n" "clean" "Clean all reifydb packages"
	@printf "  %-25s %s\n" "format" "Format all code with rustfmt (nightly)"
	@printf "  %-25s %s\n" "format-check" "Format code and fail if files changed"
	@printf "  %-25s %s\n" "build-wasm" "Build WebAssembly packages (requires wasm-pack)"
	@echo ""
	@echo "  🐳 Docker"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-25s %s\n" "build-testcontainer" "Build test container"
	@printf "  %-25s %s\n" "push-testcontainer" "Push test container to registry"
	@echo ""
	@echo "  📦 Release Management"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-25s %s\n" "show-version" "Display current version"
	@printf "  %-25s %s\n" "release" "Release with auto-increment patch version"
	@printf "  %-25s %s\n" "release VERSION=x.y.z" "Release specific version"
	@printf "  %-25s %s\n" "release-patch" "Quick patch release"
	@printf "  %-25s %s\n" "release-minor" "Quick minor release"
	@printf "  %-25s %s\n" "release-major" "Quick major release"
	@printf "  %-25s %s\n" "release-dry-run" "Preview release without executing"
	@printf "  %-25s %s\n" "help-release" "Show detailed release help"
	@echo ""
	@echo "  📊 Code Quality"
	@echo "  ───────────────────────────────────────────────────────────────"
	@printf "  %-25s %s\n" "check-code-quality" "Validate code quality standards"
	@printf "  %-25s %s\n" "check" "Check for uncommitted changes"
	@printf "  %-25s %s\n" "push" "Push changes to git (after check)"
	@echo ""
	@echo "  💡 Quick Start"
	@echo "  ───────────────────────────────────────────────────────────────"
	@echo "  make test-dev      # Fast feedback during development"
	@echo "  make test          # Full test before committing"
	@echo "  make release       # Release a new version"
	@echo ""

# =============================================================================
# Main Pipeline Targets
# =============================================================================

.PHONY: all
all: format-check check-code-quality check clean build build-testcontainer test-full push-testcontainer push

.PHONY: check-code-quality
check-code-quality:
	@echo "🔍 Checking code quality standards..."
	@./scripts/check-internal-reexports.sh
	@./scripts/check-toplevel-imports.sh
	@./scripts/check-inline-qualifications.sh
	@./scripts/check-license-headers.sh
	@./scripts/check-section-comments.sh
	@cargo clippy --release -- -D warnings

.PHONY: check
check:
	@echo "🔍 Checking repository status..."
	@if ! git diff-index --quiet HEAD --; then \
		echo "❌ Error: You have uncommitted changes. Please commit or stash them before pushing."; \
		exit 1; \
	fi
	@echo "📡 Fetching from remote..."
	@git fetch origin --quiet
	@LOCAL=$$(git rev-parse @); \
	REMOTE=$$(git rev-parse @{u} 2>/dev/null || echo ""); \
	BASE=$$(git merge-base @ @{u} 2>/dev/null || echo ""); \
	if [ -n "$$REMOTE" ]; then \
		if [ "$$LOCAL" = "$$BASE" ] && [ "$$LOCAL" != "$$REMOTE" ]; then \
			echo "❌ Error: Your branch is behind the remote. Please pull the latest changes."; \
			echo "   Run: git pull"; \
			exit 1; \
		elif [ "$$LOCAL" != "$$REMOTE" ] && [ "$$REMOTE" != "$$BASE" ]; then \
			echo "❌ Error: Your branch has diverged from the remote. Please reconcile the branches."; \
			echo "   Run: git pull --rebase or git pull --merge"; \
			exit 1; \
		fi; \
	fi
	@echo "✅ Repository check passed."

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

test-full: test-workspace test-pkg-rust test-examples test-suite test-external test-pkg-typescript fuzz-regression
	@echo "✅ All tests completed successfully!"

test-dev: test-workspace test-pkg-rust test-examples test-suite-dev
	@echo "🚀 Development tests completed!"

# Include testing sub-makefiles
include mk/test-workspace.mk
include mk/test-suites.mk
include mk/test-external.mk
include mk/test-pkg-rust.mk
include mk/test-pkg-typescript.mk
include mk/test-examples.mk

# Only include benchmark makefile when benchmark targets are being run
ifneq ($(filter bench bench-% ,$(MAKECMDGOALS)),)
include mk/test-bench.mk
endif

include mk/clean.mk
include mk/build.mk
include mk/format.mk
include mk/container.mk
include mk/release.mk
include mk/fuzz.mk