// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Compute pool implementations for different platforms.

// Platform-specific compute pool implementations
#[cfg(feature = "native")]
pub mod native;

#[cfg(feature = "wasm")]
pub mod wasm;
