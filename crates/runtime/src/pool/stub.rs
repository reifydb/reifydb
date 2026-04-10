// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Stub thread pool implementation for DST and WASM targets.
//!
//! Pools is a zero-size marker since these targets use single-threaded execution.

use super::PoolConfig;

/// Handle to the runtime's thread pools.
///
/// Zero-size marker in DST/WASM mode (no real thread pools).
#[derive(Clone)]
pub struct Pools;

impl Default for Pools {
	fn default() -> Self {
		Self
	}
}

impl Pools {
	/// Create pools from configuration (no-op in DST/WASM).
	pub fn new(_config: PoolConfig) -> Self {
		Self
	}
}
