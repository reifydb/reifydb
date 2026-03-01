// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Runtime management for ReifyDB.
//!
//! This crate provides a facade over platform-specific runtime implementations:
//! - **Native**: tokio multi-threaded runtime + rayon-based actor system
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
//! // Use the actor system for spawning actors and compute
//! let system = runtime.actor_system();
//! let handle = system.spawn("my-actor", MyActor::new());
//!
//! // Run CPU-bound work
//! let result = system.install(|| expensive_calculation());
//! ```

#![allow(dead_code)]

pub mod clock;

pub mod hash;

pub mod sync;

pub mod actor;

use std::{future::Future, mem::ManuallyDrop, sync::Arc, time::Duration};

use crate::{
	actor::system::{ActorSystem, ActorSystemConfig},
	clock::{Clock, MockClock},
};

/// Configuration for creating a [`SharedRuntime`].
#[derive(Clone)]
pub struct SharedRuntimeConfig {
	/// Number of worker threads for async runtime (ignored in WASM)
	pub async_threads: usize,
	/// Number of worker threads for compute/actor pool (ignored in WASM)
	pub compute_threads: usize,
	/// Maximum concurrent compute tasks (ignored in WASM)
	pub compute_max_in_flight: usize,
	/// Clock for time operations (defaults to real system clock)
	pub clock: Clock,
}

impl Default for SharedRuntimeConfig {
	fn default() -> Self {
		Self {
			async_threads: 1,
			compute_threads: 1,
			compute_max_in_flight: 32,
			clock: Clock::Real,
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

	/// Use a mock clock starting at the given milliseconds.
	pub fn mock_clock(mut self, initial_millis: u64) -> Self {
		self.clock = Clock::Mock(MockClock::from_millis(initial_millis));
		self
	}

	/// Use a custom clock.
	pub fn clock(mut self, clock: Clock) -> Self {
		self.clock = clock;
		self
	}

	/// Derive an [`ActorSystemConfig`] from this runtime config.
	pub fn actor_system_config(&self) -> ActorSystemConfig {
		ActorSystemConfig {
			pool_threads: self.compute_threads,
			max_in_flight: self.compute_max_in_flight,
		}
	}
}

// WASM runtime types - single-threaded execution support
use std::fmt;
#[cfg(target_arch = "wasm32")]
use std::{pin::Pin, task::Poll};

#[cfg(target_arch = "wasm32")]
use futures_util::future::LocalBoxFuture;
#[cfg(not(target_arch = "wasm32"))]
use tokio::runtime::{self as tokio_runtime, Runtime};
#[cfg(not(target_arch = "wasm32"))]
use tokio::task::JoinHandle;

/// WASM-compatible handle (placeholder).
#[cfg(target_arch = "wasm32")]
#[derive(Clone, Copy, Debug)]
pub struct WasmHandle;

/// WASM-compatible join handle.
///
/// Implements Future to be compatible with async/await.
#[cfg(target_arch = "wasm32")]
pub struct WasmJoinHandle<T> {
	future: LocalBoxFuture<'static, T>,
}

#[cfg(target_arch = "wasm32")]
impl<T> Future for WasmJoinHandle<T> {
	type Output = Result<T, WasmJoinError>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		match self.future.as_mut().poll(cx) {
			Poll::Ready(v) => Poll::Ready(Ok(v)),
			Poll::Pending => Poll::Pending,
		}
	}
}

/// WASM join error (compatible with tokio::task::JoinError API).
#[cfg(target_arch = "wasm32")]
#[derive(Debug)]
pub struct WasmJoinError;

#[cfg(target_arch = "wasm32")]
impl fmt::Display for WasmJoinError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "WASM task failed")
	}
}

#[cfg(target_arch = "wasm32")]
impl error::Error for WasmJoinError {}

/// Inner shared state for the runtime (native).
#[cfg(not(target_arch = "wasm32"))]
struct SharedRuntimeInner {
	tokio: ManuallyDrop<Runtime>,
	system: ActorSystem,
	clock: Clock,
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for SharedRuntimeInner {
	fn drop(&mut self) {
		// SAFETY: drop is called exactly once; taking the Runtime here
		// prevents its default Drop (which calls shutdown_background and
		// does NOT wait). We call shutdown_timeout instead so that worker
		// threads and I/O resources (epoll fd, timer fd, etc.) are fully
		// reclaimed before this function returns.
		let rt = unsafe { ManuallyDrop::take(&mut self.tokio) };
		rt.shutdown_timeout(Duration::from_secs(5));
	}
}

/// Inner shared state for the runtime (WASM).
#[cfg(target_arch = "wasm32")]
struct SharedRuntimeInner {
	system: ActorSystem,
	clock: Clock,
}

/// Shared runtime that can be cloned and passed across subsystems.
///
/// Platform-agnostic facade over:
/// - Native: tokio multi-threaded runtime + unified actor system
/// - WASM: Single-threaded execution
///
/// Uses Arc internally, so cloning is cheap and all clones share the same
/// underlying runtime and actor system.
#[derive(Clone)]
pub struct SharedRuntime(Arc<SharedRuntimeInner>);

impl SharedRuntime {
	/// Create a new shared runtime from configuration.
	///
	/// # Panics
	///
	/// Panics if the runtime cannot be created (native only).
	#[cfg(not(target_arch = "wasm32"))]
	pub fn from_config(config: SharedRuntimeConfig) -> Self {
		let tokio = tokio_runtime::Builder::new_multi_thread()
			.worker_threads(config.async_threads)
			.thread_name("async")
			.enable_all()
			.build()
			.expect("Failed to create tokio runtime");

		let system = ActorSystem::new(config.actor_system_config());

		Self(Arc::new(SharedRuntimeInner {
			tokio: ManuallyDrop::new(tokio),
			system,
			clock: config.clock,
		}))
	}

	/// Create a new shared runtime from configuration.
	#[cfg(target_arch = "wasm32")]
	pub fn from_config(config: SharedRuntimeConfig) -> Self {
		let system = ActorSystem::new(config.actor_system_config());

		Self(Arc::new(SharedRuntimeInner {
			system,
			clock: config.clock,
		}))
	}

	/// Get the unified actor system for spawning actors and compute.
	pub fn actor_system(&self) -> ActorSystem {
		self.0.system.clone()
	}

	/// Get the clock for this runtime (shared across all threads).
	pub fn clock(&self) -> &Clock {
		&self.0.clock
	}

	/// Get a handle to the async runtime.
	///
	/// Returns a platform-specific handle type:
	/// - Native: `tokio_runtime::Handle`
	/// - WASM: `WasmHandle`
	#[cfg(not(target_arch = "wasm32"))]
	pub fn handle(&self) -> tokio_runtime::Handle {
		self.0.tokio.handle().clone()
	}

	/// Get a handle to the async runtime.
	#[cfg(target_arch = "wasm32")]
	pub fn handle(&self) -> WasmHandle {
		WasmHandle
	}

	/// Spawn a future onto the runtime.
	///
	/// Returns a platform-specific join handle type:
	/// - Native: `JoinHandle`
	/// - WASM: `WasmJoinHandle`
	#[cfg(not(target_arch = "wasm32"))]
	pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
	where
		F: Future + Send + 'static,
		F::Output: Send + 'static,
	{
		self.0.tokio.spawn(future)
	}

	/// Spawn a future onto the runtime.
	#[cfg(target_arch = "wasm32")]
	pub fn spawn<F>(&self, future: F) -> WasmJoinHandle<F::Output>
	where
		F: Future + 'static,
		F::Output: 'static,
	{
		WasmJoinHandle {
			future: Box::pin(future),
		}
	}

	/// Block the current thread until the future completes.
	///
	/// **Note:** Not supported in WASM builds - will panic.
	#[cfg(not(target_arch = "wasm32"))]
	pub fn block_on<F>(&self, future: F) -> F::Output
	where
		F: Future,
	{
		self.0.tokio.block_on(future)
	}

	/// Block the current thread until the future completes.
	///
	/// **Note:** Not supported in WASM builds - will panic.
	#[cfg(target_arch = "wasm32")]
	pub fn block_on<F>(&self, _future: F) -> F::Output
	where
		F: Future,
	{
		unimplemented!("block_on not supported in WASM - use async execution instead")
	}

	/// Executes a closure on the actor system's pool directly.
	///
	/// This is a convenience method that delegates to the actor system's `install`.
	/// Synchronous and bypasses admission control.
	pub fn install<R, F>(&self, f: F) -> R
	where
		R: Send,
		F: FnOnce() -> R + Send,
	{
		self.0.system.install(f)
	}
}

impl fmt::Debug for SharedRuntime {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("SharedRuntime").finish_non_exhaustive()
	}
}

// Keep existing tests but gate them by target
#[cfg(all(test, reifydb_target = "native"))]
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
	fn test_actor_system_accessible() {
		let runtime = SharedRuntime::from_config(test_config());
		let system = runtime.actor_system();
		let result = system.install(|| 42);
		assert_eq!(result, 42);
	}

	#[test]
	fn test_install() {
		let runtime = SharedRuntime::from_config(test_config());
		let result = runtime.install(|| 42);
		assert_eq!(result, 42);
	}
}

#[cfg(all(test, reifydb_target = "wasm"))]
mod wasm_tests {
	use super::*;

	#[test]
	fn test_wasm_runtime_creation() {
		let runtime = SharedRuntime::from_config(SharedRuntimeConfig::default());
		let system = runtime.actor_system();
		let result = system.install(|| 42);
		assert_eq!(result, 42);
	}
}
