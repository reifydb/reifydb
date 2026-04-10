// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Thread pool management for ReifyDB.
//!
//! Provides named, isolated thread pools for different workload classes:
//! - **System pool**: lightweight system actors (flow, CDC, watermark, metrics)
//! - **Query pool**: heavy query execution actors (WS, gRPC, HTTP)
//!
//! # Platform differences
//!
//! - **Native**: each pool is a separate `rayon::ThreadPool` with its own OS threads
//! - **DST/WASM**: `Pools` is a zero-size marker (no real thread pools)

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
