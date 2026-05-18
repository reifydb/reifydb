// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[cfg(reifydb_target = "dst")]
pub mod dst;

#[cfg(all(not(reifydb_single_threaded), not(reifydb_target = "dst")))]
pub mod native;

#[cfg(all(reifydb_single_threaded, not(reifydb_target = "dst")))]
pub mod wasm;

#[cfg(reifydb_target = "dst")]
pub use dst::{ActorHandle, ActorSystem, JoinError};
#[cfg(all(not(reifydb_single_threaded), not(reifydb_target = "dst")))]
pub use native::{ActorHandle, ActorSystem, JoinError};
#[cfg(all(reifydb_single_threaded, not(reifydb_target = "dst")))]
pub use wasm::{ActorHandle, ActorSystem, JoinError};

#[derive(Debug, Clone, Default)]
pub struct ActorConfig {
	pub mailbox_capacity: Option<usize>,
}

impl ActorConfig {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
		self.mailbox_capacity = Some(capacity);
		self
	}
}
