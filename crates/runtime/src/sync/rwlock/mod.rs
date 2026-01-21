// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! RwLock synchronization primitive.

#[cfg(feature = "native")]
pub mod native;
#[cfg(feature = "wasm")]
pub mod wasm;

#[cfg(feature = "native")]
pub use native::{RwLock, RwLockReadGuard, RwLockWriteGuard};
#[cfg(feature = "wasm")]
pub use wasm::{RwLock, RwLockReadGuard, RwLockWriteGuard};
