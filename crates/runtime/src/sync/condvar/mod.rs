// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Condvar synchronization primitive.

#[cfg(feature = "native")]
pub mod native;
#[cfg(feature = "wasm")]
pub mod wasm;

#[cfg(feature = "native")]
pub use native::{Condvar, WaitTimeoutResult};
#[cfg(feature = "wasm")]
pub use wasm::{Condvar, WaitTimeoutResult};
