# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB

# =============================================================================
# Format Targets - Format all Rust code with rustfmt
# =============================================================================

FORMAT_TOOLCHAIN := nightly-2026-08-15

.PHONY: format format-rust format-workspace format-check strip-comments

# Main format target - formats everything
format: strip-comments format-rust
	@echo "✅ All code formatting complete!"

# Strip comments
strip-comments:
	@echo "🧹 Stripping comments..."
	@./scripts/comments.sh strip

# Format all Rust code
format-rust: ensure-rustfmt format-workspace
	@echo "✅ Rust formatting complete!"

# Format and fail if files changed (for CI/make all)
format-check: ensure-rustfmt
	@echo "🎨 Formatting code..."
	@./scripts/comments.sh strip
	@cargo +$(FORMAT_TOOLCHAIN) fmt --all
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
	@if ! rustup toolchain list | grep -q "^$(FORMAT_TOOLCHAIN)"; then \
		echo "Installing nightly toolchain..."; \
		rustup toolchain install $(FORMAT_TOOLCHAIN); \
	fi
	@if ! rustup component list --toolchain $(FORMAT_TOOLCHAIN) | grep -q "rustfmt"; then \
		echo "Installing rustfmt for nightly..."; \
		rustup component add rustfmt --toolchain $(FORMAT_TOOLCHAIN); \
	fi

# Format entire workspace (includes crates/, bin/, and pkg/rust/)
format-workspace:
	@echo "🎨 Formatting entire workspace..."
	cargo +$(FORMAT_TOOLCHAIN) fmt --all