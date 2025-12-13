// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! Shared tokio runtime for all network subsystems.
//!
//! This module provides a centralized tokio runtime that can be shared across
//! HTTP, WebSocket, and admin subsystems for efficient resource utilization.

use tokio::runtime::{Handle, Runtime};

/// Shared tokio runtime for all network subsystems.
///
/// Created once at server startup and passed to all subsystems via Handle clones.
/// This ensures efficient work-stealing across HTTP, WebSocket, and admin servers.
///
/// # Example
///
/// ```ignore
/// let runtime = SharedRuntime::new(num_cpus::get());
/// let handle = runtime.handle();
///
/// // Pass handle to subsystems
/// let http = HttpSubsystem::new(addr, state, handle.clone());
/// let ws = WsSubsystem::new(addr, state, handle.clone());
/// ```
pub struct SharedRuntime {
	runtime: Runtime,
}

impl SharedRuntime {
	/// Create a new shared runtime with the specified number of worker threads.
	///
	/// # Arguments
	///
	/// * `worker_threads` - Number of tokio worker threads. Typically `num_cpus::get()`.
	///
	/// # Panics
	///
	/// Panics if the tokio runtime cannot be created.
	pub fn new(worker_threads: usize) -> Self {
		let runtime = tokio::runtime::Builder::new_multi_thread()
			.worker_threads(worker_threads)
			.thread_name("server")
			.enable_all()
			.build()
			.expect("Failed to create tokio runtime");

		Self { runtime }
	}

	/// Get a handle for spawning tasks on this runtime.
	///
	/// The handle can be cloned and passed to multiple subsystems.
	/// Tasks spawned via the handle run on the shared runtime's thread pool.
	pub fn handle(&self) -> Handle {
		self.runtime.handle().clone()
	}

	/// Block the current thread until the future completes.
	///
	/// Used during server startup/shutdown from synchronous context.
	/// Should not be called from within an async context.
	pub fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
		self.runtime.block_on(future)
	}

	/// Spawn a future on this runtime.
	///
	/// Returns a JoinHandle that can be used to await the result.
	pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
	where
		F: std::future::Future + Send + 'static,
		F::Output: Send + 'static,
	{
		self.runtime.spawn(future)
	}
}

impl Default for SharedRuntime {
	fn default() -> Self {
		Self::new(num_cpus::get())
	}
}

// Re-export num_cpus::get for convenience
pub use num_cpus::get as get_num_cpus;

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_runtime_creation() {
		let runtime = SharedRuntime::new(2);
		let handle = runtime.handle();

		// Verify we can spawn and await a task
		let result = runtime.block_on(async {
			handle.spawn(async { 42 }).await.unwrap()
		});

		assert_eq!(result, 42);
	}

	#[test]
	fn test_runtime_default() {
		let runtime = SharedRuntime::default();
		assert!(runtime.handle().runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread);
	}
}
