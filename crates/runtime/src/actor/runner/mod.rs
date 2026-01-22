// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Actor runner - the internal execution loop for actors.
//!
//! This module contains the [`ActorRunner`] which drives actor execution
//! through its lifecycle: initialization, message processing, and cleanup.
//!
//! # Platform Differences
//!
//! - **Native**: The runner drives actor execution on its own OS thread with a blocking message receive loop.
//! - **WASM**: No runner is needed - actors process messages inline (synchronously) when sent.

#[cfg(reifydb_target = "native")]
pub(crate) mod native;
#[cfg(reifydb_target = "wasm")]
pub(crate) mod wasm;

#[cfg(reifydb_target = "native")]
pub(crate) use native::ActorRunner;
