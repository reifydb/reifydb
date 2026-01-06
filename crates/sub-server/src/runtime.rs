// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Shared tokio runtime management for server subsystems.
//!
//! This module provides `SharedRuntime`, a cloneable wrapper around a tokio runtime
//! that can be shared across multiple subsystems. When a subsystem doesn't provide
//! its own runtime, it falls back to the global `DEFAULT_RUNTIME`.
//!
//! # Example
//!
//! ```ignore
//! use reifydb_sub_server::{SharedRuntime, DEFAULT_RUNTIME};
//!
//! // Create a custom runtime
//! let runtime = SharedRuntime::new(4);
//!
//! // Or use the default
//! let runtime = DEFAULT_RUNTIME.clone();
//!
//! // Spawn async work
//! runtime.spawn(async {
//!     // async work here
//! });
//! ```

use std::sync::Arc;

use once_cell::sync::Lazy;
use tokio::{
	runtime::{Handle, Runtime},
	task::JoinHandle,
};

/// Inner shared runtime state.
#[derive(Debug)]
struct Inner {
	runtime: Runtime,
}

/// Shared tokio runtime that can be cloned and passed across subsystems.
///
/// Uses Arc internally, so cloning is cheap and all clones share the same
/// underlying runtime and thread pool.
#[derive(Clone, Debug)]
pub struct SharedRuntime(Arc<Inner>);

impl SharedRuntime {
	/// Create a new shared runtime with the specified number of worker threads.
	///
	/// # Arguments
	///
	/// * `worker_threads` - Number of worker threads for the runtime
	///
	/// # Panics
	///
	/// Panics if the runtime cannot be created.
	pub fn new(worker_threads: usize) -> Self {
		let runtime = tokio::runtime::Builder::new_multi_thread()
			.worker_threads(worker_threads)
			.thread_name("async")
			.enable_all()
			.build()
			.expect("Failed to create tokio runtime");

		Self(Arc::new(Inner {
			runtime,
		}))
	}

	/// Get a handle to the runtime.
	///
	/// This can be used to spawn tasks or enter the runtime context.
	pub fn handle(&self) -> Handle {
		self.0.runtime.handle().clone()
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
}

impl Default for SharedRuntime {
	fn default() -> Self {
		Self::new(num_cpus::get())
	}
}

/// Global default runtime used when no runtime is explicitly provided.
///
/// This runtime uses the number of available CPUs as the worker thread count.
/// All subsystems that don't receive a custom runtime will share this instance.
pub static DEFAULT_RUNTIME: Lazy<SharedRuntime> = Lazy::new(|| SharedRuntime::new(num_cpus::get()));

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_runtime_creation() {
		let runtime = SharedRuntime::new(2);
		let result = runtime.0.runtime.block_on(async { 42 });
		assert_eq!(result, 42);
	}

	#[test]
	fn test_runtime_clone_shares_same_runtime() {
		let rt1 = SharedRuntime::new(2);
		let rt2 = rt1.clone();
		assert!(Arc::ptr_eq(&rt1.0, &rt2.0));
	}

	#[test]
	fn test_default_runtime_is_singleton() {
		let rt1 = DEFAULT_RUNTIME.clone();
		let rt2 = DEFAULT_RUNTIME.clone();
		assert!(Arc::ptr_eq(&rt1.0, &rt2.0));
	}

	#[test]
	fn test_spawn() {
		let runtime = SharedRuntime::new(2);
		let handle = runtime.spawn(async { 123 });
		let result = runtime.0.runtime.block_on(handle).unwrap();
		assert_eq!(result, 123);
	}
}
