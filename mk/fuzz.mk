# =============================================================================
# Fuzz Testing (requires cargo-fuzz + nightly toolchain)
# =============================================================================
#
# The workspace uses vendored dependencies (.cargo/config.toml), but the fuzz
# crate needs packages not in the vendor dir (libfuzzer-sys, arbitrary).
# Fuzz targets temporarily hide the vendor config during builds.

DURATION ?= 60
FUZZ_CFG := .cargo/config.toml
FUZZ_CFG_BAK := .cargo/config.toml.fuzz-bak

.PHONY: fuzz-list
fuzz-list:
	@if [ -f $(FUZZ_CFG) ]; then mv $(FUZZ_CFG) $(FUZZ_CFG_BAK); fi; \
	cargo +nightly fuzz list; ret=$$?; \
	if [ -f $(FUZZ_CFG_BAK) ]; then mv $(FUZZ_CFG_BAK) $(FUZZ_CFG); fi; \
	exit $$ret

.PHONY: fuzz-run
fuzz-run:
ifndef TARGET
	$(error TARGET is required. Usage: make fuzz-run TARGET=sql_transpile)
endif
	@if [ -f $(FUZZ_CFG) ]; then mv $(FUZZ_CFG) $(FUZZ_CFG_BAK); fi; \
	cargo +nightly fuzz run $(TARGET) -- -max_total_time=$(DURATION) -rss_limit_mb=4096; ret=$$?; \
	if [ -f $(FUZZ_CFG_BAK) ]; then mv $(FUZZ_CFG_BAK) $(FUZZ_CFG); fi; \
	exit $$ret

.PHONY: fuzz-smoke
fuzz-smoke:
	@if [ -f $(FUZZ_CFG) ]; then mv $(FUZZ_CFG) $(FUZZ_CFG_BAK); fi; \
	echo "Running fuzz smoke tests (10s each)..."; \
	failed=0; \
	for target in $$(cargo +nightly fuzz list 2>/dev/null); do \
		echo "  Fuzzing $$target..."; \
		cargo +nightly fuzz run $$target -- -max_total_time=10 -rss_limit_mb=4096 || { failed=1; break; }; \
	done; \
	if [ -f $(FUZZ_CFG_BAK) ]; then mv $(FUZZ_CFG_BAK) $(FUZZ_CFG); fi; \
	if [ $$failed -ne 0 ]; then echo "Fuzz smoke tests FAILED"; exit 1; fi; \
	echo "All fuzz smoke tests passed."

.PHONY: fuzz-regression
fuzz-regression:
	@if [ -f $(FUZZ_CFG) ]; then mv $(FUZZ_CFG) $(FUZZ_CFG_BAK); fi; \
	artifacts=$$(find fuzz/artifacts -type f ! -name '.*' 2>/dev/null); \
	if [ -z "$$artifacts" ]; then \
		echo "No fuzz regression artifacts found."; \
		if [ -f $(FUZZ_CFG_BAK) ]; then mv $(FUZZ_CFG_BAK) $(FUZZ_CFG); fi; \
		exit 0; \
	fi; \
	failed=0; \
	for artifact in $$artifacts; do \
		target=$$(basename $$(dirname $$artifact)); \
		echo "  Replaying $$artifact against $$target..."; \
		cargo +nightly fuzz run $$target $$artifact -- -rss_limit_mb=4096 || { failed=1; break; }; \
	done; \
	if [ -f $(FUZZ_CFG_BAK) ]; then mv $(FUZZ_CFG_BAK) $(FUZZ_CFG); fi; \
	if [ $$failed -ne 0 ]; then echo "Fuzz regression FAILED"; exit 1; fi; \
	echo "All fuzz regression tests passed."
