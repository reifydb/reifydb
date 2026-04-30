// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[cfg(all(not(reifydb_single_threaded), not(reifydb_target = "dst")))]
mod native;

#[cfg(any(reifydb_single_threaded, reifydb_target = "dst"))]
mod wasm;

#[cfg(all(not(reifydb_single_threaded), not(reifydb_target = "dst")))]
pub use native::Pools;
#[cfg(any(reifydb_single_threaded, reifydb_target = "dst"))]
pub use wasm::Pools;

/// Configuration for thread pool sizes.
#[derive(Debug, Clone)]
pub struct PoolConfig {
	/// Threads for the system pool (lightweight actors).
	pub system_threads: usize,
	/// Threads for the query pool (execution-heavy actors).
	pub query_threads: usize,
	/// Threads for the async pool (tokio runtime).
	pub async_threads: usize,
}

impl Default for PoolConfig {
	fn default() -> Self {
		Self {
			system_threads: 1,
			query_threads: 1,
			async_threads: 0,
		}
	}
}
