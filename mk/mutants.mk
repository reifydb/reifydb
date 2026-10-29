# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

.PHONY: test-mutant

test-mutant:
	@$(if $(SUITE),,$(error SUITE is required, e.g. make test-mutant SUITE=wal))
	@MUTANT_BUILD= MUTANT_TEST=test-workspace scripts/mutants.sh $(SUITE)
