// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Unified actor system for ReifyDB.
//!
//! This module provides a unified system for all concurrent work:
//! - **Actor spawning** with configurable threading models
//! - **CPU-bound compute** with admission control
//!
//! # Platform Differences
//!
//! - **Native**: Rayon thread pool + optional dedicated threads for non-Send actors
//! - **WASM**: All operations execute inline (synchronously)
//!
//! # Threading Models
//!
//! Actors can be configured with different threading models:
//! - [`ThreadingModel::SharedPool`]: Run on shared work-stealing pool (requires `State: Send`)
//! - [`ThreadingModel::DedicatedThread`]: Run on dedicated OS thread (allows non-Send state)
//!
//! In WASM, both models degrade to inline processing.
//!
//! # Example
//!
//! ```ignore
//! use reifydb_runtime::system::{ActorSystem, ActorSystemConfig};
//!
//! let system = ActorSystem::new(ActorSystemConfig::default());
//!
//! // Spawn an actor with default (shared pool) threading
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

pub use config::{ActorConfig, ThreadingModel};
#[cfg(reifydb_target = "native")]
pub use native::{ActorHandle, ActorSystem, ActorSystemConfig, JoinError};
#[cfg(reifydb_target = "wasm")]
pub use wasm::{ActorHandle, ActorSystem, ActorSystemConfig, JoinError};
