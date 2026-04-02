// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Unified actor system for ReifyDB.
//!
//! This module provides a unified system for all concurrent work:
//! - **Actor spawning** on a shared work-stealing pool
//! - **CPU-bound compute** with admission control
//!
//! # Platform Differences
//!
//! - **Native**: Rayon thread pool for all actors
//! - **WASM**: All operations execute inline (synchronously)

#[cfg(not(reifydb_single_threaded))]
pub mod native;

#[cfg(reifydb_single_threaded)]
pub mod wasm;

#[cfg(not(reifydb_single_threaded))]
pub use native::{ActorHandle, ActorSystem, ActorSystemConfig, JoinError};
#[cfg(reifydb_single_threaded)]
pub use wasm::{ActorHandle, ActorSystem, ActorSystemConfig, JoinError};

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
