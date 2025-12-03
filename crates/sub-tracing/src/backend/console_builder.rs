// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Builder for configuring console output settings

/// Builder for configuring console output with fluent API
///
/// This builder configures how tracing_subscriber outputs to the console.
#[derive(Debug, Clone)]
pub struct ConsoleBuilder {
	use_color: bool,
	stderr_for_errors: bool,
}

impl ConsoleBuilder {
	/// Create a new console builder with default settings
	pub fn new() -> Self {
		Self {
			use_color: true,
			stderr_for_errors: true,
		}
	}

	/// Enable or disable colored output
	///
	/// # Arguments
	/// * `enabled` - true to enable colors, false for plain text
	///
	/// # Example
	/// ```
	/// # use reifydb_sub_tracing::ConsoleBuilder;
	/// ConsoleBuilder::new().color(true);
	/// ```
	pub fn color(mut self, enabled: bool) -> Self {
		self.use_color = enabled;
		self
	}

	/// Use stderr for error and critical level logs
	///
	/// Note: This setting is provided for API compatibility but is not
	/// currently used by tracing_subscriber's default fmt layer.
	///
	/// # Arguments
	/// * `enabled` - true to send errors to stderr, false to send all to stdout
	///
	/// # Example
	/// ```
	/// # use reifydb_sub_tracing::ConsoleBuilder;
	/// ConsoleBuilder::new().stderr_for_errors(true);
	/// ```
	pub fn stderr_for_errors(mut self, enabled: bool) -> Self {
		self.stderr_for_errors = enabled;
		self
	}

	/// Get the color setting
	pub fn use_color(&self) -> bool {
		self.use_color
	}

	/// Get the stderr for errors setting
	pub fn use_stderr_for_errors(&self) -> bool {
		self.stderr_for_errors
	}
}

impl Default for ConsoleBuilder {
	fn default() -> Self {
		Self::new()
	}
}
