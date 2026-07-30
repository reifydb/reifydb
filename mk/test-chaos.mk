# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

# =============================================================================
# Chaos Testing (randomized, seed-reproducible)
# =============================================================================
#
# Each chaos workload expands to N separate tests, one per index:
# bank_transfers_chaos_0, _1, ... They run in parallel under nextest. Each test
# draws a fresh random seed every run (exploration), so the names are stable but
# the seeds differ run to run. On failure a test prints its seed and a replay
# command.
#
#   make test-chaos                      # 32 tests per workload, fresh seeds
#   make test-chaos N=64                 # 64 tests per workload (recompiles)
#   make test-chaos FILTER=bank_transfers   # only matching chaos tests
#   make test-chaos SEED=987 FILTER=bank_transfers_chaos_2   # replay one failure
#   make test-chaos PACKAGE=reifydb-sub-flow            # one crate only
#   make test-chaos PACKAGES="reifydb-testing-sdk reifydb-sub-flow"  # several crates
#   make list-chaos                      # list the selection instead of running
#
# N is read at COMPILE time (baked into the macro via ITERATIONS), so the
# count is a true per-workload set of separate tests; changing N recompiles the
# chaos crates. Unset N falls back to the macro's own default of 32. Per-test
# pins via the macro's 3-arg form ignore N. Chaos tests are gated behind the
# `chaos` cargo feature so they never run in the normal suites. PACKAGES lists
# the crates that define that feature; append to it as more crates grow chaos
# tests. The selection covers each crate's chaos integration binary plus any
# unit test whose name contains `chaos`; FILTER narrows it to tests whose name
# contains the given substring. SEED pins every selected test to that exact seed
# for reproduction (pair it with FILTER to target one test).

FILTER ?=
PACKAGE ?=
PACKAGES ?=

# `N` is also a knob in mk/test-workspace.mk (repeat count), and that file is
# included first, so a bare `N ?=` here inherits its default of 1 and silently
# runs one iteration per workload instead of 32. Honour N only when it actually
# came from the caller; anything reaching us from another makefile is not ours.
ITERATIONS = $(if $(filter command line environment,$(origin N)),$(strip $(N)),)

# PACKAGE and PACKAGES are both accepted and compose: give either, or both. The
# built-in list applies only when neither is set, so PACKAGE=x narrows to x
# rather than appending x to every crate.
ifeq ($(strip $(PACKAGES)$(PACKAGE)),)
PACKAGES = reifydb-testing-chaos reifydb-testing-sdk reifydb-transaction reifydb-store-multi reifydb-sub-flow
endif

SELECT = (binary(chaos) or test(chaos))$(if $(FILTER), and test($(FILTER)),)

.PHONY: test-chaos list-chaos
test-chaos:
	@echo "🌀 Running chaos tests (N=$(if $(ITERATIONS),$(ITERATIONS),32)$(if $(SEED), SEED=$(SEED),)$(if $(FILTER), FILTER=$(FILTER),))"
	@echo "   packages: $(strip $(PACKAGES) $(PACKAGE))"
	@$(if $(ITERATIONS),ITERATIONS=$(ITERATIONS),) $(if $(SEED),SEED=$(SEED),) \
		cargo nextest run --release \
		$(foreach p,$(strip $(PACKAGES) $(PACKAGE)),-p $(p)) \
		--features chaos -E '$(SELECT)' \
		--no-fail-fast --status-level fail --final-status-level fail $(CARGO_OFFLINE)

list-chaos:
	@echo "   packages: $(strip $(PACKAGES) $(PACKAGE))"
	@$(if $(ITERATIONS),ITERATIONS=$(ITERATIONS),) \
		cargo nextest list --release \
		$(foreach p,$(strip $(PACKAGES) $(PACKAGE)),-p $(p)) \
		--features chaos -E '$(SELECT)' $(CARGO_OFFLINE)
