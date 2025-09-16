# =============================================================================
# Format Targets - Format all Rust code with rustfmt
# =============================================================================

.PHONY: format format-rust format-db format-bin format-pkg-rust

# Main format target - formats everything
format: format-rust
	@echo "✅ All code formatting complete!"

# Format all Rust code
format-rust: ensure-rustfmt format-db format-bin format-pkg-rust
	@echo "✅ Rust formatting complete!"

# Ensure rustfmt nightly is installed
.PHONY: ensure-rustfmt
ensure-rustfmt:
	@if ! rustup toolchain list | grep -q "nightly"; then \
		echo "Installing nightly toolchain..."; \
		rustup toolchain install nightly; \
	fi
	@if ! rustup component list --toolchain nightly | grep -q "rustfmt"; then \
		echo "Installing rustfmt for nightly..."; \
		rustup component add rustfmt --toolchain nightly; \
	fi

# Format db/ workspace
format-db:
	@echo "🎨 Formatting db/ workspace..."
	@cd db && cargo +nightly fmt --all

# Format bin/ packages
format-bin:
	@echo "🎨 Formatting bin/ packages..."
	@for dir in bin/cli bin/server bin/playground bin/testcontainer; do \
		if [ -d "$$dir" ] && [ -f "$$dir/Cargo.toml" ]; then \
			echo "  Formatting $$dir..."; \
			cd $$dir && cargo +nightly fmt 2>/dev/null || true && cd - >/dev/null; \
		fi; \
	done

# Format pkg/rust packages
format-pkg-rust:
	@echo "🎨 Formatting pkg/rust packages..."
	@for dir in pkg/rust/reifydb pkg/rust/reifydb-client pkg/rust/examples pkg/rust/tests/limit pkg/rust/tests/regression; do \
		if [ -d "$$dir" ] && [ -f "$$dir/Cargo.toml" ]; then \
			echo "  Formatting $$dir..."; \
			cd $$dir && cargo +nightly fmt 2>/dev/null || true && cd - >/dev/null; \
		fi; \
	done