# =============================================================================
# Format Targets - Format all Rust code with rustfmt
# =============================================================================

.PHONY: format format-rust format-workspace format-check

# Main format target - formats everything
format: format-rust
	@echo "✅ All code formatting complete!"

# Format all Rust code
format-rust: ensure-rustfmt format-workspace
	@echo "✅ Rust formatting complete!"

# Format and fail if files changed (for CI/make all)
format-check: ensure-rustfmt
	@echo "🎨 Formatting code..."
	@cargo +nightly fmt --all
	@if ! git diff --quiet; then \
		echo ""; \
		echo "❌ Error: Code formatting changed files. Please commit the formatting changes and try again."; \
		echo ""; \
		echo "   Changed files:"; \
		git diff --name-only | sed 's/^/     /'; \
		echo ""; \
		exit 1; \
	fi
	@echo "✅ Code formatting check passed."

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

# Format entire workspace (includes crates/, bin/, and pkg/rust/)
format-workspace:
	@echo "🎨 Formatting entire workspace..."
	cargo +nightly fmt --all