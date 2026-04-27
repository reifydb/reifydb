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

	/// Get a reference to the system pool (stub).
	pub fn system_pool(&self) -> StubPool {
		StubPool
	}

	/// Number of threads in the system pool (always 1 in DST/WASM).
	pub fn system_thread_count(&self) -> usize {
		1
	}

	/// Get a reference to the query pool (stub).
	pub fn query_pool(&self) -> StubPool {
		StubPool
	}
}

/// Stub thread pool that executes closures inline.
#[derive(Clone)]
pub struct StubPool;

impl StubPool {
	/// Run `f` directly on the current thread.
	pub fn install<F, R>(&self, f: F) -> R
	where
		F: FnOnce() -> R,
	{
		f()
	}
}
