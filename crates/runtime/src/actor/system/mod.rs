// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(reifydb_dst)]
pub mod dst;

#[cfg(all(not(reifydb_single_threaded), not(reifydb_dst)))]
pub mod native;

#[cfg(all(reifydb_single_threaded, not(reifydb_dst)))]
pub mod wasm;

#[cfg(reifydb_dst)]
pub use dst::{ActorHandle, ActorSpawner, ActorSystem, JoinError};
#[cfg(all(not(reifydb_single_threaded), not(reifydb_dst)))]
pub use native::{ActorHandle, ActorSpawner, ActorSystem, JoinError};
#[cfg(all(reifydb_single_threaded, not(reifydb_dst)))]
pub use wasm::{ActorHandle, ActorSpawner, ActorSystem, JoinError};

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
