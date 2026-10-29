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
#   make test-workspace-chaos                      # 32 tests per workload, fresh seeds
#   make test-workspace-chaos N=64                 # 64 tests per workload (recompiles)
#   make test-workspace-chaos FILTER=bank_transfers   # only matching chaos tests
#   make test-workspace-chaos SEED=987 FILTER=bank_transfers_chaos_2   # replay one failure
#   make test-workspace-chaos SUITE=sub-flow                      # one crate only
#   make list-chaos                      # list the selection instead of running
#
# N is read at COMPILE time (baked into the macro via ITERATIONS), so the
# count is a true per-workload set of separate tests; changing N recompiles the
# chaos crates. Unset N falls back to the macro's own default of 32. Per-test
# pins via the macro's 3-arg form ignore N. Chaos tests are gated behind the
# `chaos` cargo feature so they never run in the normal suites. CHAOS_PACKAGES lists
# the crates that define that feature; append to it as more crates grow chaos
# tests. The selection covers each crate's chaos integration binary plus any
# unit test whose name contains `chaos`; FILTER narrows it to tests whose name
# contains the given substring. SEED pins every selected test to that exact seed
# for reproduction (pair it with FILTER to target one test).

FILTER ?=
SUITE ?=

ITERATIONS = $(strip $(N))

# The built-in list applies only when SUITE is unset, so SUITE=x narrows to x
# rather than appending x to every crate.
CHAOS_PACKAGES := reifydb-testing-chaos reifydb-testing-sdk reifydb-transaction reifydb-store-multi reifydb-store-operator reifydb-sub-flow
CHAOS_SELECTED = $(if $(SUITE),reifydb-$(SUITE),$(CHAOS_PACKAGES))

SELECT = (binary(chaos) or test(chaos))$(if $(FILTER), and test($(FILTER)),)

.PHONY: test-workspace-chaos list-chaos
test-workspace-chaos:
	@echo "🌀 Running chaos tests (N=$(if $(ITERATIONS),$(ITERATIONS),32)$(if $(SEED), SEED=$(SEED),)$(if $(FILTER), FILTER=$(FILTER),))"
	@echo "   packages: $(CHAOS_SELECTED)"
	@REIFYDB_ASSERTIONS=1 $(if $(ITERATIONS),ITERATIONS=$(ITERATIONS),) $(if $(SEED),SEED=$(SEED),) \
		cargo nextest run --cargo-profile gate-chaos \
		$(foreach p,$(CHAOS_SELECTED),-p $(p)) \
		--features chaos -E '$(SELECT)' \
		--no-fail-fast --status-level fail --final-status-level fail $(CARGO_OFFLINE)
	@$(MAKE) --no-print-directory sweep-auto

list-chaos:
	@echo "   packages: $(CHAOS_SELECTED)"
	@REIFYDB_ASSERTIONS=1 $(if $(ITERATIONS),ITERATIONS=$(ITERATIONS),) \
		cargo nextest list --cargo-profile gate-chaos \
		$(foreach p,$(CHAOS_SELECTED),-p $(p)) \
		--features chaos -E '$(SELECT)' $(CARGO_OFFLINE)
