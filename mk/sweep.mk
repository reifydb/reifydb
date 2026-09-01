# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

SWEEP_DAYS ?= 1
SWEEP_DIRS := $(CARGO_TARGET_DIR) $(CARGO_TARGET_DIR)/dst
SWEEP_FLAGS :=
SWEEP_PRUNE := -exec rm -rf {} +

.PHONY: sweep sweep-dry sweep-auto sweep-install

sweep-install:
	@echo "📦 Installing cargo-sweep..."
	@cd $(HOME) && cargo install cargo-sweep --locked

# cargo-sweep never touches incremental/, so those caches are pruned here against the same cutoff.
sweep:
	@cargo sweep --help >/dev/null 2>&1 || { echo "cargo-sweep is not installed. Run: make sweep-install"; exit 1; }
	@for dir in $(SWEEP_DIRS); do \
		[ -d "$$dir" ] || continue; \
		CARGO_TARGET_DIR=$$dir cargo sweep $(SWEEP_FLAGS) --time $(SWEEP_DAYS) $(CURDIR) || exit 1; \
		for inc in "$$dir"/*/incremental; do \
			[ -d "$$inc" ] || continue; \
			find "$$inc" -mindepth 1 -maxdepth 1 -type d -mmin +$$(( $(SWEEP_DAYS) * 1440 )) $(SWEEP_PRUNE); \
		done; \
	done

sweep-dry: SWEEP_FLAGS := --dry-run
sweep-dry: SWEEP_PRUNE := -exec du -sh {} +
sweep-dry: sweep

sweep-auto:
	@cargo sweep --help >/dev/null 2>&1 \
		&& $(MAKE) --no-print-directory sweep \
		|| echo "⏭️  Skipping sweep (cargo-sweep not installed; run: make sweep-install)"
