// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Mutex synchronization primitive.

#[cfg(feature = "native")]
pub mod native;
#[cfg(feature = "wasm")]
pub mod wasm;

#[cfg(feature = "native")]
pub use native::{Mutex, MutexGuard};
#[cfg(feature = "wasm")]
pub use wasm::{Mutex, MutexGuard};
