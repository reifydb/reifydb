// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Configuration types for the unified actor system.

/// Threading model for an actor.
///
/// Determines how the actor is scheduled in the native runtime.
/// In WASM, both models degrade to inline processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ThreadingModel {
	/// Run on shared work-stealing pool (default).
	///
	/// Requires `State: Send`. Multiple actors share the same rayon thread pool,
	/// with work-stealing for good CPU utilization.
	#[default]
	SharedPool,

	/// Run on a dedicated OS thread.
	///
	/// Allows non-Send state (Rc, RefCell, etc.). Each actor gets its own
	/// OS thread with a blocking message receive loop.
	DedicatedThread,
}

/// Configuration for actor behavior.
#[derive(Debug, Clone)]
pub struct ActorConfig {
	/// Mailbox capacity. 0 = unbounded.
	///
	/// Default: 0 (unbounded)
	pub mailbox_capacity: usize,

	/// Threading model for this actor.
	///
	/// Default: SharedPool
	pub threading: ThreadingModel,
}

impl Default for ActorConfig {
	fn default() -> Self {
		Self {
			mailbox_capacity: 0,
			threading: ThreadingModel::default(),
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

	/// Set the threading model.
	pub fn threading(mut self, threading: ThreadingModel) -> Self {
		self.threading = threading;
		self
	}
}
