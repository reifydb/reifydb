// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Runtime management for ReifyDB.
//!
//! This module provides shared runtime infrastructure:
//!
//! - [`SharedRuntime`]: A cloneable wrapper around a tokio runtime for async I/O
//!   with an embedded [`ComputePool`] for CPU-bound work
//! - [`SharedRuntimeConfig`]: Configuration for creating a SharedRuntime
//! - [`ComputePool`]: A rayon-based pool for CPU-bound work with admission control
//!
//! # Example
//!
//! ```ignore
//! use reifydb_core::{SharedRuntime, SharedRuntimeConfig};
//!
//! // Create a runtime with default configuration
//! let runtime = SharedRuntime::from_config(SharedRuntimeConfig::default());
//!
//! // Or with custom configuration
//! let config = SharedRuntimeConfig::default()
//!     .async_threads(4)
//!     .compute_threads(4)
//!     .compute_max_in_flight(16);
//! let runtime = SharedRuntime::from_config(config);
//!
//! // Spawn async work
//! runtime.spawn(async {
//!     // async work here
//! });
//!
//! // Get the compute pool for CPU-bound work
//! let result = runtime.compute_pool().compute(|| expensive_calculation()).await;
//! ```

mod compute;

pub use compute::ComputePool;

use std::future::Future;
use std::sync::Arc;

use tokio::{
	runtime::{Handle, Runtime},
	task::JoinHandle,
};

/// Configuration for creating a [`SharedRuntime`].
#[derive(Clone, Debug)]
pub struct SharedRuntimeConfig {
	/// Number of worker threads for the async tokio runtime.
	pub async_threads: usize,
	/// Number of worker threads for the rayon compute pool.
	pub compute_threads: usize,
	/// Maximum concurrent tasks for the compute pool (admission control).
	pub compute_max_in_flight: usize,
}

impl Default for SharedRuntimeConfig {
	fn default() -> Self {
		Self {
			async_threads: 4,
			compute_threads: 4,
			compute_max_in_flight: 16,
		}
	}
}

impl SharedRuntimeConfig {
	/// Set the number of async worker threads.
	pub fn async_threads(mut self, threads: usize) -> Self {
		self.async_threads = threads;
		self
	}

	/// Set the number of compute worker threads.
	pub fn compute_threads(mut self, threads: usize) -> Self {
		self.compute_threads = threads;
		self
	}

	/// Set the maximum number of in-flight compute tasks.
	pub fn compute_max_in_flight(mut self, max: usize) -> Self {
		self.compute_max_in_flight = max;
		self
	}
}

/// Inner shared runtime state.
struct Inner {
	runtime: Runtime,
	compute_pool: ComputePool,
}

impl std::fmt::Debug for Inner {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Inner")
			.field("runtime", &"Runtime { .. }")
			.field("compute_pool", &"ComputePool { .. }")
			.finish()
	}
}

/// Shared tokio runtime that can be cloned and passed across subsystems.
///
/// Includes an embedded [`ComputePool`] for CPU-bound work. Uses Arc internally,
/// so cloning is cheap and all clones share the same underlying runtime and thread pools.
#[derive(Clone, Debug)]
pub struct SharedRuntime(Arc<Inner>);

impl SharedRuntime {
	/// Create a new shared runtime from configuration.
	///
	/// # Panics
	///
	/// Panics if the runtime cannot be created.
	pub fn from_config(config: SharedRuntimeConfig) -> Self {
		let runtime = tokio::runtime::Builder::new_multi_thread()
			.worker_threads(config.async_threads)
			.thread_name("async")
			.enable_all()
			.build()
			.expect("Failed to create tokio runtime");

		let compute_pool = ComputePool::new(config.compute_threads, config.compute_max_in_flight);

		Self(Arc::new(Inner { runtime, compute_pool }))
	}

	/// Get a handle to the async runtime.
	///
	/// This can be used to spawn tasks or enter the runtime context.
	pub fn handle(&self) -> Handle {
		self.0.runtime.handle().clone()
	}

	/// Get the compute pool for CPU-bound work.
	pub fn compute_pool(&self) -> ComputePool {
		self.0.compute_pool.clone()
	}

	/// Spawn a future onto the runtime.
	///
	/// This is equivalent to `tokio::spawn` but uses this specific runtime.
	pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
	where
		F: Future + Send + 'static,
		F::Output: Send + 'static,
	{
		self.0.runtime.spawn(future)
	}

	/// Block the current thread until the future completes.
	///
	/// This runs the provided future to completion on the runtime,
	/// blocking the current thread until it finishes.
	pub fn block_on<F>(&self, future: F) -> F::Output
	where
		F: Future,
	{
		self.0.runtime.block_on(future)
	}

	/// Executes a closure on the rayon compute pool directly.
	///
	/// This is a convenience method that delegates to the compute pool's `install`.
	/// Synchronous and bypasses admission control.
	pub fn install<R, F>(&self, f: F) -> R
	where
		R: Send,
		F: FnOnce() -> R + Send,
	{
		self.0.compute_pool.install(f)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn test_config() -> SharedRuntimeConfig {
		SharedRuntimeConfig::default()
			.async_threads(2)
			.compute_threads(2)
			.compute_max_in_flight(4)
	}

	#[test]
	fn test_runtime_creation() {
		let runtime = SharedRuntime::from_config(test_config());
		let result = runtime.0.runtime.block_on(async { 42 });
		assert_eq!(result, 42);
	}

	#[test]
	fn test_runtime_clone_shares_same_runtime() {
		let rt1 = SharedRuntime::from_config(test_config());
		let rt2 = rt1.clone();
		assert!(Arc::ptr_eq(&rt1.0, &rt2.0));
	}

	#[test]
	fn test_spawn() {
		let runtime = SharedRuntime::from_config(test_config());
		let handle = runtime.spawn(async { 123 });
		let result = runtime.0.runtime.block_on(handle).unwrap();
		assert_eq!(result, 123);
	}

	#[test]
	fn test_compute_pool_accessible() {
		let runtime = SharedRuntime::from_config(test_config());
		let pool = runtime.compute_pool();
		// Just verify we can get the pool - actual compute tests are in compute.rs
		let _ = pool;
	}
}
