// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Builder for configuring console logging backend

use super::console::{ConsoleBackend, FormatStyle};

/// Builder for configuring console logging backend with fluent API
#[derive(Debug, Clone)]
pub struct ConsoleBuilder {
	use_color: bool,
	stderr_for_errors: bool,
	format_style: FormatStyle,
}

impl ConsoleBuilder {
	/// Create a new console builder with default settings
	pub fn new() -> Self {
		Self {
			use_color: true,
			stderr_for_errors: true,
			format_style: FormatStyle::Timeline,
		}
	}

	/// Enable or disable colored output
	///
	/// # Arguments
	/// * `enabled` - true to enable colors, false for plain text
	///
	/// # Example
	/// ```
	/// # use reifydb_sub_logging::ConsoleBuilder;
	/// ConsoleBuilder::new().color(true);
	/// ```
	pub fn color(mut self, enabled: bool) -> Self {
		self.use_color = enabled;
		self
	}

	/// Use stderr for error and critical level logs
	///
	/// # Arguments
	/// * `enabled` - true to send errors to stderr, false to send all to
	///   stdout
	///
	/// # Example
	/// ```
	/// # use reifydb_sub_logging::ConsoleBuilder;
	/// ConsoleBuilder::new().stderr_for_errors(true);
	/// ```
	pub fn stderr_for_errors(mut self, enabled: bool) -> Self {
		self.stderr_for_errors = enabled;
		self
	}

	/// Set the format style for log output
	///
	/// # Arguments
	/// * `style` - The format style to use (Box or Timeline)
	///
	/// # Example
	/// ```
	/// # use reifydb_sub_logging::{ConsoleBuilder, FormatStyle};
	/// ConsoleBuilder::new().format_style(FormatStyle::Timeline);
	/// ```
	pub fn format_style(mut self, style: FormatStyle) -> Self {
		self.format_style = style;
		self
	}

	/// Build the console backend with the configured settings
	pub(crate) fn build(self) -> ConsoleBackend {
		ConsoleBackend::new()
			.with_color(self.use_color)
			.with_stderr_for_errors(self.stderr_for_errors)
			.with_format_style(self.format_style)
	}
}

impl Default for ConsoleBuilder {
	fn default() -> Self {
		Self::new()
	}
}
