// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Configuration types for the unified actor system.

/// Configuration for actor behavior.
#[derive(Debug, Clone)]
pub struct ActorConfig {
	/// Mailbox capacity. 0 = unbounded.
	///
	/// Default: 0 (unbounded)
	pub mailbox_capacity: usize,
}

impl Default for ActorConfig {
	fn default() -> Self {
		Self {
			mailbox_capacity: 0,
		}
	}
}

impl ActorConfig {
	/// Create a new config with default values.
	pub fn new() -> Self {
		Self::default()
	}

	/// Set the mailbox capacity. 0 = unbounded.
	pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
		self.mailbox_capacity = capacity;
		self
	}
}
