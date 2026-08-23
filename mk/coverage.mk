# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

# =============================================================================
# Coverage (requires cargo-llvm-cov + llvm-tools-preview)
# =============================================================================
#
#   make coverage-install                                    # one-time bootstrap
#   make coverage                                            # whole workspace
#   make coverage PACKAGE=reifydb-store-cdc                  # one crate
#   make coverage-chaos PACKAGE=reifydb-store-cdc N=1000     # chaos, 1000 seeds
#   make coverage-chaos PACKAGES="reifydb-store-cdc reifydb-cdc"
#   make coverage-summary PACKAGE=reifydb-store-cdc
#   make coverage-open
#
# N, FILTER, SEED, PACKAGE and PACKAGES mean exactly what they mean in
# mk/test-chaos.mk, and coverage-chaos reuses that file's test selection and
# package list, so this file must be included after it. N is the per-workload
# iteration count: each iteration draws its own seed, so raising N widens the
# state space a single run explores. It is read at compile time and recompiles
# the chaos crates.
#
# PACKAGE and PACKAGES also scope the report. Running only chaos tests leaves
# most of the workspace untouched, and those unhit crates would otherwise drown
# the numbers for the crate under test, so every crate outside the selection is
# filtered out of the report. The filter is built as the complement of the
# selection because llvm-cov only takes an ignore pattern and Rust's regex crate
# has no lookahead, so "keep only these" cannot be written directly.
#
# A package name maps to its directory by dropping the reifydb- prefix; a name
# with no matching directory under crates/ is a hard error, because a silently
# unmatched name filters out every real crate and reports nothing.
#
# `coverage` honours PACKAGE and PACKAGES only when they actually came from the
# caller. mk/test-chaos.mk defaults PACKAGES to its own crate list, so reading it
# unconditionally would make a bare `make coverage` cover the chaos crates
# instead of the workspace. `coverage-chaos` wants that default and takes it.
#
# Every llvm-cov invocation carries COVERAGE_PROFILE. The report step looks for
# object files under the profile it is given, so a summary that omits the flag
# the run used searches the wrong directory and finds nothing.
#
# coverage-install runs cargo from outside the repo. .cargo/config.toml replaces
# crates-io with the local vendor directory and forces offline, so an install run
# from here can only resolve crates that are already vendored and fails.
#
# Instrumented builds land in their own target directory, so running coverage
# never invalidates the normal build cache.

COVERAGE_PROFILE := --release
COVERAGE_HTML := target/llvm-cov/html/index.html

COVERAGE_EMPTY :=
COVERAGE_SPACE := $(COVERAGE_EMPTY) $(COVERAGE_EMPTY)
COVERAGE_ALL_DIRS := $(notdir $(patsubst %/,%,$(dir $(wildcard crates/*/Cargo.toml))))

COV_FROM_CALLER = $(filter command line environment,$(origin PACKAGES) $(origin PACKAGE))
COV_PKGS = $(if $(COV_FROM_CALLER),$(strip $(PACKAGES) $(PACKAGE)),)
COV_SELECT = $(if $(COV_PKGS),$(foreach p,$(COV_PKGS),-p $(p)),--workspace)

COVERAGE_KEEP_DIRS = $(patsubst reifydb-%,%,$(COV_PKGS))
COVERAGE_DROP_DIRS = $(filter-out $(COVERAGE_KEEP_DIRS),$(COVERAGE_ALL_DIRS))
COVERAGE_DROP_RE = $(if $(COVERAGE_KEEP_DIRS),|crates/($(subst $(COVERAGE_SPACE),|,$(strip $(COVERAGE_DROP_DIRS))))/,)
COVERAGE_IGNORE = --ignore-filename-regex '(^|/)(tests|benches|examples)/|/vendor/|/\.cargo/registry/$(COVERAGE_DROP_RE)'

ifneq ($(filter coverage coverage-chaos coverage-summary,$(MAKECMDGOALS)),)
COVERAGE_UNKNOWN := $(filter-out $(COVERAGE_ALL_DIRS),$(patsubst reifydb-%,%,$(strip $(PACKAGES) $(PACKAGE))))
ifneq ($(strip $(COVERAGE_UNKNOWN)),)
$(error no crates/ directory for coverage package: $(COVERAGE_UNKNOWN))
endif
endif

.PHONY: coverage-install
coverage-install:
	@echo "📈 Installing coverage tooling"
	rustup component add llvm-tools-preview
	cd $(HOME) && cargo install cargo-llvm-cov --locked

.PHONY: coverage
coverage:
	@echo "📈 Coverage: $(if $(COV_PKGS),$(COV_PKGS),workspace)"
	cargo llvm-cov clean --workspace
	cargo llvm-cov nextest $(COVERAGE_PROFILE) $(COV_SELECT) $(COVERAGE_IGNORE) \
		--no-fail-fast --html $(CARGO_OFFLINE)
	@echo "   report: $(COVERAGE_HTML)"

.PHONY: coverage-chaos
coverage-chaos: COV_PKGS = $(strip $(PACKAGES) $(PACKAGE))
coverage-chaos:
	@echo "📈 Chaos coverage (N=$(if $(ITERATIONS),$(ITERATIONS),32)$(if $(SEED), SEED=$(SEED),)$(if $(FILTER), FILTER=$(FILTER),))"
	@echo "   packages: $(COV_PKGS)"
	cargo llvm-cov clean --workspace
	@$(if $(ITERATIONS),ITERATIONS=$(ITERATIONS),) $(if $(SEED),SEED=$(SEED),) \
		cargo llvm-cov nextest $(COVERAGE_PROFILE) $(COV_SELECT) \
		--features chaos -E '$(SELECT)' $(COVERAGE_IGNORE) \
		--no-fail-fast --status-level fail --final-status-level fail --html $(CARGO_OFFLINE)
	@echo "   report: $(COVERAGE_HTML)"

.PHONY: coverage-summary
coverage-summary:
	@cargo llvm-cov report $(COVERAGE_PROFILE) --summary-only $(COVERAGE_IGNORE)

.PHONY: coverage-open
coverage-open:
	@xdg-open $(COVERAGE_HTML) 2>/dev/null || echo "open $(COVERAGE_HTML)"
