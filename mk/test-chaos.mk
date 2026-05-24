# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (c) 2026 ReifyDB

# =============================================================================
# Chaos Testing (randomized, seed-reproducible)
# =============================================================================
#
# Each chaos test runs N randomized iterations in-process, one per derived
# seed. On failure the runner prints the base seed and a replay command.
#
#   make test-chaos              # 100 iterations, random base seed
#   make test-chaos N=1234       # 1234 iterations
#   make test-chaos SEED=987 N=1234   # replay a reported failure
#   make test-chaos FILTER=passthrough   # only chaos tests whose name matches
#   make list-chaos                      # list the selection instead of running
#
# Chaos tests are gated behind the `chaos` cargo feature so they never run in
# the normal suites. CHAOS_PACKAGES lists the crates that define that feature;
# append to it as more crates grow chaos tests. The selection covers the chaos
# integration binary plus the framework's chaos unit tests; FILTER narrows it to
# tests whose name contains the given substring.

N ?= 100
FILTER ?=
CHAOS_PACKAGES ?= reifydb-sdk

CHAOS_SELECT = (binary(chaos) or test(chaos))$(if $(FILTER), and test($(FILTER)),)

.PHONY: test-chaos list-chaos
test-chaos:
	@echo "🌀 Running chaos tests (N=$(N)$(if $(SEED), SEED=$(SEED),)$(if $(FILTER), FILTER=$(FILTER),))..."
	@CHAOS_ITERATIONS=$(N) $(if $(SEED),CHAOS_SEED=$(SEED),) \
		cargo nextest run --release \
		$(foreach p,$(CHAOS_PACKAGES),-p $(p)) \
		--features chaos -E '$(CHAOS_SELECT)' \
		--no-fail-fast --status-level fail --final-status-level fail $(CARGO_OFFLINE)

list-chaos:
	@cargo nextest list --release \
		$(foreach p,$(CHAOS_PACKAGES),-p $(p)) \
		--features chaos -E '$(CHAOS_SELECT)' $(CARGO_OFFLINE)
