# =============
# External Tests (SLT snapshot regression)
# =============

.PHONY: test-external

# Run snapshot regression tests (compares against baselines)
test-external:
	@echo "🔍 Running external SLT regression tests..."
	cd $(EXTERNAL_DIR) && CARGO_TARGET_DIR=$(realpath target) $(MAKE) test
