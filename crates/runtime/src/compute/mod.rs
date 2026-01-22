// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Compute pool implementations for different platforms.

// Platform-specific compute pool implementations
#[cfg(reifydb_target = "native")]
pub mod native;

#[cfg(reifydb_target = "wasm")]
pub mod wasm;

cfg_if::cfg_if! {
    if #[cfg(reifydb_target = "native")] {
	pub type ComputePool = native::NativeComputePool;
    } else {
	pub type ComputePool = wasm::WasmComputePool;
    }
}
