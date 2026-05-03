// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

/// Builder for configuring console output with fluent API
///
/// This builder configures how tracing_subscriber outputs to the console.
#[derive(Debug, Clone)]
pub struct ConsoleBuilder {
	use_color: bool,
	stderr_for_errors: bool,
}

impl ConsoleBuilder {
	pub fn new() -> Self {
		Self {
			use_color: true,
			stderr_for_errors: true,
		}
	}

	pub fn color(mut self, enabled: bool) -> Self {
		self.use_color = enabled;
		self
	}

	pub fn stderr_for_errors(mut self, enabled: bool) -> Self {
		self.stderr_for_errors = enabled;
		self
	}

	pub fn use_color(&self) -> bool {
		self.use_color
	}

	pub fn use_stderr_for_errors(&self) -> bool {
		self.stderr_for_errors
	}
}

impl Default for ConsoleBuilder {
	fn default() -> Self {
		Self::new()
	}
}
