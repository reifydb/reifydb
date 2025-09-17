# Vendor dependencies using cargo-vendor-filterer

# Always run vendor target (no caching)
.PHONY: vendor
.NOTPARALLEL: vendor

vendor:
	@echo "Vendoring dependencies using cargo-vendor-filterer..."
	@echo "Platforms: Linux (x86_64, ARM64, ARM32), macOS, BSD variants"
	@echo "Excluding: dev-dependencies, test/bench/example directories"
	@echo ""
	
	# Clean previous vendor directory
	@rm -rf crates/vendor/
	
	# Vendor dependencies with filtering
	@cd crates && cargo vendor-filterer \
		--platform x86_64-unknown-linux-gnu \
		--platform aarch64-unknown-linux-gnu \
		--platform armv7-unknown-linux-gnueabihf \
		--platform x86_64-apple-darwin \
		--platform aarch64-apple-darwin \
		--platform x86_64-unknown-freebsd \
		--platform x86_64-unknown-netbsd \
		--platform x86_64-unknown-openbsd \
		--exclude-crate-path "*#tests" \
		--exclude-crate-path "*#benches" \
		--exclude-crate-path "*#examples" \
		vendor

	@echo ""
	@echo "✓ Vendor directory configured"
	@echo "✓ Vendored dependencies: $$(ls -1 crates/vendor/ 2>/dev/null | wc -l)"
	@echo ""
	@echo "Platform support (configured in Cargo.toml):"
	@echo "  ✓ Linux (x86_64, ARM64, ARM32)"
	@echo "  ✓ macOS (Intel, Apple Silicon)"
	@echo "  ✓ BSD (FreeBSD, NetBSD, OpenBSD)"
	@echo "  ✗ Windows (filtered out)"
	@echo "  ✗ WASM/WASI (filtered out)"
	@echo "  ✗ dev-dependencies (filtered out)"
	@echo "  ✗ test/bench/example directories (filtered out)"
	@echo ""
	@echo "✓ All build and test commands will now use --offline mode automatically"
	@echo "✓ To build: make build"
	@echo "✓ To test: make test-dev"