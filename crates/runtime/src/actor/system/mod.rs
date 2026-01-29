// SPDX-License-Identifier: AGPL-3.0-or-later
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

#[cfg(reifydb_target = "native")]
pub mod native;

#[cfg(reifydb_target = "wasm")]
pub mod wasm;

#[cfg(reifydb_target = "native")]
pub use native::{ActorHandle, ActorSystem, ActorSystemConfig, JoinError};
#[cfg(reifydb_target = "wasm")]
pub use wasm::{ActorHandle, ActorSystem, ActorSystemConfig, JoinError};

#[derive(Debug, Clone)]
pub struct ActorConfig {
	pub mailbox_capacity: Option<usize>,
}

impl Default for ActorConfig {
	fn default() -> Self {
		Self {
			mailbox_capacity: None,
		}
	}
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
