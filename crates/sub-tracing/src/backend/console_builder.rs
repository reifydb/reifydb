// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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
