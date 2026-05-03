// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Thread-pool abstraction. Splits work across three named pools - async I/O, system, and query - so the runtime
//! can size each independently. Native targets get the tokio-backed implementation; single-threaded and DST targets
//! get the in-memory variant. The `Pools` type both impls hand back is what `SharedRuntime` carries around.

#[cfg(all(not(reifydb_single_threaded), not(reifydb_target = "dst")))]
mod native;

#[cfg(any(reifydb_single_threaded, reifydb_target = "dst"))]
mod wasm;

#[cfg(all(not(reifydb_single_threaded), not(reifydb_target = "dst")))]
pub use native::Pools;
#[cfg(any(reifydb_single_threaded, reifydb_target = "dst"))]
pub use wasm::Pools;

#[derive(Debug, Clone)]
pub struct PoolConfig {
	pub system_threads: usize,

	pub query_threads: usize,

	pub async_threads: usize,
}

impl Default for PoolConfig {
	fn default() -> Self {
		Self {
			async_threads: 1,
			system_threads: 2,
			query_threads: 1,
		}
	}
}

impl PoolConfig {
	pub fn sync_only() -> Self {
		Self {
			async_threads: 0,
			system_threads: 1,
			query_threads: 1,
		}
	}
}
