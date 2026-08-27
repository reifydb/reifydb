// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(not(reifydb_single_threaded), not(reifydb_dst)))]
pub(crate) mod actor_pool;

#[cfg(all(not(reifydb_single_threaded), not(reifydb_dst)))]
pub mod compute;

#[cfg(all(not(reifydb_single_threaded), not(reifydb_dst)))]
mod host;

#[cfg(all(not(reifydb_single_threaded), not(reifydb_dst)))]
pub(crate) mod task;

#[cfg(any(reifydb_single_threaded, reifydb_dst))]
mod wasm;

#[cfg(all(not(reifydb_single_threaded), not(reifydb_dst)))]
pub use host::Pools;
#[cfg(any(reifydb_single_threaded, reifydb_dst))]
pub use wasm::Pools;

#[derive(Debug, Clone)]
pub struct PoolConfig {
	pub coordination_threads: usize,

	pub flow_threads: usize,

	pub maintenance_threads: usize,

	pub task_threads: usize,

	pub compute_threads: usize,

	pub async_threads: usize,
}

impl Default for PoolConfig {
	fn default() -> Self {
		Self {
			coordination_threads: 2,
			flow_threads: 2,
			maintenance_threads: 1,
			task_threads: 2,
			compute_threads: 2,
			async_threads: 1,
		}
	}
}

impl PoolConfig {
	pub fn sync_only() -> Self {
		Self {
			coordination_threads: 1,
			flow_threads: 1,
			maintenance_threads: 1,
			task_threads: 1,
			compute_threads: 1,
			async_threads: 0,
		}
	}
}
