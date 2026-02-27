# =============================================================================
# Fuzz Testing (requires cargo-fuzz + nightly toolchain)
# =============================================================================
#
# The workspace uses vendored dependencies (.cargo/config.toml), but the fuzz
# crate needs packages not in the vendor dir (libfuzzer-sys, arbitrary).
# Fuzz targets temporarily hide the vendor config during builds.

DURATION ?= 60
FUZZ_SMOKE_DURATION ?= 10
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
	cargo +nightly fuzz run $(TARGET) -- -max_total_time=$(DURATION); ret=$$?; \
	if [ -f $(FUZZ_CFG_BAK) ]; then mv $(FUZZ_CFG_BAK) $(FUZZ_CFG); fi; \
	exit $$ret

.PHONY: fuzz-smoke
fuzz-smoke:
	@if [ -f $(FUZZ_CFG) ]; then mv $(FUZZ_CFG) $(FUZZ_CFG_BAK); fi; \
	echo "Running fuzz smoke tests ($(FUZZ_SMOKE_DURATION)s each)..."; \
	failed=0; \
	for target in $$(cargo +nightly fuzz list 2>/dev/null); do \
		echo "  Fuzzing $$target..."; \
		cargo +nightly fuzz run $$target -- -max_total_time=$(FUZZ_SMOKE_DURATION) || { failed=1; break; }; \
	done; \
	if [ -f $(FUZZ_CFG_BAK) ]; then mv $(FUZZ_CFG_BAK) $(FUZZ_CFG); fi; \
	if [ $$failed -ne 0 ]; then echo "Fuzz smoke tests FAILED"; exit 1; fi; \
	echo "All fuzz smoke tests passed."
