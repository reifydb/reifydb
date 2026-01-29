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
//! - **Native**: Rayon thread pool for all actors (requires `State: Send`)
//! - **WASM**: All operations execute inline (synchronously)
//!
//! # Example
//!
//! ```ignore
//! use reifydb_runtime::{SharedRuntimeConfig, actor::system::ActorSystem};
//!
//! let system = ActorSystem::new(SharedRuntimeConfig::default().actor_system_config());
//!
//! // Spawn an actor on the shared pool
//! let counter_ref = system.spawn("counter", CounterActor::new());
//!
//! // Run CPU-bound work
//! let result = system.install(|| expensive_calculation());
//!
//! // Run async CPU-bound work with admission control
//! let result = system.compute(|| another_calculation()).await?;
//! ```

pub mod config;

#[cfg(reifydb_target = "native")]
pub mod native;

#[cfg(reifydb_target = "wasm")]
pub mod wasm;

pub use config::ActorConfig;
#[cfg(reifydb_target = "native")]
pub use native::{ActorHandle, ActorSystem, ActorSystemConfig, JoinError};
#[cfg(reifydb_target = "wasm")]
pub use wasm::{ActorHandle, ActorSystem, ActorSystemConfig, JoinError};
