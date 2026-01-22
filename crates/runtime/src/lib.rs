// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Runtime management for ReifyDB.
//!
//! This crate provides a facade over platform-specific runtime implementations:
//! - **Native**: tokio multi-threaded runtime + rayon compute pool
//! - **WASM**: Single-threaded execution with sequential processing
//!
//! The API is identical across platforms, with compile-time dispatch ensuring
//! zero runtime overhead.
//!
//! # Example
//!
//! ```ignore
//! use reifydb_runtime::{SharedRuntime, SharedRuntimeConfig};
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
//! #[cfg(feature = "native")]
//! let pool: reifydb_runtime::compute::native::NativeComputePool = runtime.compute_pool();
//! #[cfg(feature = "wasm")]
//! let pool: reifydb_runtime::compute::wasm::WasmComputePool = runtime.compute_pool();
//! let result = pool.compute(|| expensive_calculation()).await;
//! ```

#![allow(dead_code)]

// Ensure at least one runtime feature is enabled
#[cfg(not(any(feature = "native", feature = "wasm")))]
compile_error!("Either feature \"native\" or feature \"wasm\" must be enabled for reifydb-runtime");

#[cfg(feature = "native")]
pub mod runtime;
#[cfg(feature = "wasm")]
pub mod runtime;

pub mod compute;

pub mod hash;

pub mod sync;

pub mod time;

pub mod actor;

pub mod concurrent_map;

use std::{future::Future, sync::Arc};
use cfg_if::cfg_if;
use futures_util::task::SpawnExt;

/// Configuration for creating a [`SharedRuntime`].
#[derive(Clone, Debug)]
pub struct SharedRuntimeConfig {
	/// Number of worker threads for async runtime (ignored in WASM)
	pub async_threads: usize,
	/// Number of worker threads for compute pool (ignored in WASM)
	pub compute_threads: usize,
	/// Maximum concurrent compute tasks (ignored in WASM)
	pub compute_max_in_flight: usize,
}

impl Default for SharedRuntimeConfig {
	fn default() -> Self {
		Self {
			async_threads: 1,
			compute_threads: 1,
			compute_max_in_flight: 32,
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

cfg_if! {
    if #[cfg(feature = "native")] {
        type RuntimeImpl = runtime::NativeRuntime;
    } else if #[cfg(feature = "wasm")] {
        type RuntimeImpl = runtime::WasmRuntime;
    }
}

/// Shared runtime that can be cloned and passed across subsystems.
///
/// Platform-agnostic facade over:
/// - Native: tokio multi-threaded runtime + rayon compute pool
/// - WASM: Single-threaded execution
///
/// Uses Arc internally, so cloning is cheap and all clones share the same
/// underlying runtime and thread pools.
#[derive(Clone)]
pub struct SharedRuntime(Arc<RuntimeImpl>);

impl SharedRuntime {
	/// Create a new shared runtime from configuration.
	///
	/// # Panics
	///
	/// Panics if the runtime cannot be created (native only).
	pub fn from_config(config: SharedRuntimeConfig) -> Self {
		#[cfg(feature = "native")]
		let runtime = runtime::NativeRuntime::new(
			config.async_threads,
			config.compute_threads,
			config.compute_max_in_flight,
		);

		#[cfg(feature = "wasm")]
		let runtime = runtime::WasmRuntime::new(
			config.async_threads,
			config.compute_threads,
			config.compute_max_in_flight,
		);

		Self(Arc::new(runtime))
	}

	/// Get a handle to the async runtime.
	///
	/// Returns a platform-specific handle type:
	/// - Native: `tokio::runtime::Handle`
	/// - WASM: `WasmHandle`
	#[cfg(feature = "native")]
	pub fn handle(&self) -> tokio::runtime::Handle {
		self.0.handle()
	}

	/// Get a handle to the async runtime.
	#[cfg(feature = "wasm")]
	pub fn handle(&self) -> runtime::WasmHandle {
		self.0.handle()
	}

	/// Get the compute pool for CPU-bound work.
	#[cfg(feature = "native")]
	pub fn compute_pool(&self) -> compute::native::NativeComputePool {
		self.0.compute_pool()
	}

	/// Get the compute pool for CPU-bound work.
	#[cfg(feature = "wasm")]
	pub fn compute_pool(&self) -> compute::wasm::WasmComputePool {
		self.0.compute_pool()
	}

	/// Spawn a future onto the runtime.
	///
	/// Returns a platform-specific join handle type:
	/// - Native: `tokio::task::JoinHandle`
	/// - WASM: `WasmJoinHandle`
	#[cfg(feature = "native")]
	pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
	where
		F: Future + Send + 'static,
		F::Output: Send + 'static,
	{
		self.0.spawn(future)
	}

	/// Spawn a future onto the runtime.
	#[cfg(feature = "wasm")]
	pub fn spawn<F>(&self, future: F) -> runtime::WasmJoinHandle<F::Output>
	where
		F: Future + 'static,
		F::Output: 'static,
	{
		self.0.spawn(future)
	}

	/// Block the current thread until the future completes.
	///
	/// **Note:** Not supported in WASM builds - will panic.
	pub fn block_on<F>(&self, future: F) -> F::Output
	where
		F: Future,
	{
		self.0.block_on(future)
	}

	/// Executes a closure on the compute pool directly.
	///
	/// This is a convenience method that delegates to the compute pool's `install`.
	/// Synchronous and bypasses admission control.
	pub fn install<R, F>(&self, f: F) -> R
	where
		R: Send,
		F: FnOnce() -> R + Send,
	{
		self.0.compute_pool().install(f)
	}
}

impl std::fmt::Debug for SharedRuntime {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("SharedRuntime").finish_non_exhaustive()
	}
}

// Keep existing tests but gate them by feature
#[cfg(all(test, feature = "native"))]
mod tests {
	use super::*;

	fn test_config() -> SharedRuntimeConfig {
		SharedRuntimeConfig::default().async_threads(2).compute_threads(2).compute_max_in_flight(4)
	}

	#[test]
	fn test_runtime_creation() {
		let runtime = SharedRuntime::from_config(test_config());
		let result = runtime.block_on(async { 42 });
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
		let result = runtime.block_on(handle).unwrap();
		assert_eq!(result, 123);
	}

	#[test]
	fn test_compute_pool_accessible() {
		let runtime = SharedRuntime::from_config(test_config());
		let pool = runtime.compute_pool();
		let _ = pool;
	}
}

#[cfg(all(test, feature = "wasm"))]
mod wasm_tests {
	use super::*;

	#[test]
	fn test_wasm_runtime_creation() {
		let runtime = SharedRuntime::from_config(SharedRuntimeConfig::default());
		let pool = runtime.compute_pool();
		let result = pool.install(|| 42);
		assert_eq!(result, 42);
	}
}
