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
#   make list-chaos                      # list the selection instead of running
#
# N is read at COMPILE time (baked into the macro via CHAOS_ITERATIONS), so the
# count is a true per-workload set of separate tests; changing N recompiles the
# chaos crates. Unset N falls back to 32. Per-test pins via the macro's 3-arg
# form ignore N. Chaos tests are gated behind the `chaos` cargo feature so they
# never run in the normal suites. CHAOS_PACKAGES lists the crates that define
# that feature; append to it as more crates grow chaos tests. The selection
# covers the chaos integration binary plus the framework's chaos unit tests;
# FILTER narrows it to tests whose name contains the given substring. SEED pins
# every selected test to that exact seed for reproduction (pair it with FILTER
# to target one test).

N ?=
FILTER ?=
CHAOS_PACKAGES ?= reifydb-sdk reifydb-transaction reifydb-store-multi

CHAOS_SELECT = (binary(chaos) or test(chaos))$(if $(FILTER), and test($(FILTER)),)

.PHONY: test-chaos list-chaos test-chaos-concurrency
test-chaos:
	@echo "🌀 Running chaos tests ($(if $(N),N=$(N),N=32)$(if $(SEED), SEED=$(SEED),)$(if $(FILTER), FILTER=$(FILTER),))..."
	@$(if $(N),CHAOS_ITERATIONS=$(N),) $(if $(SEED),CHAOS_SEED=$(SEED),) \
		cargo nextest run --release \
		$(foreach p,$(CHAOS_PACKAGES),-p $(p)) \
		--features chaos -E '$(CHAOS_SELECT)' \
		--no-fail-fast --status-level fail --final-status-level fail $(CARGO_OFFLINE)

list-chaos:
	@$(if $(N),CHAOS_ITERATIONS=$(N),) \
		cargo nextest list --release \
		$(foreach p,$(CHAOS_PACKAGES),-p $(p)) \
		--features chaos -E '$(CHAOS_SELECT)' $(CARGO_OFFLINE)
