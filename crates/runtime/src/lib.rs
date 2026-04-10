// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![allow(clippy::tabs_in_doc_comments)]

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
//!     .system_threads(4)
//!     .query_threads(4);
//! let runtime = SharedRuntime::from_config(config);
//!
//! // Spawn async work
//! runtime.spawn(async {
//!     // async work here
//! });
//!
//! // Use the actor system for spawning actors
//! let system = runtime.actor_system();
//! let handle = system.spawn("my-actor", MyActor::new());
//! ```

#![allow(dead_code)]

pub mod context;

pub mod hash;

pub mod pool;

pub mod sync;

pub mod actor;

#[cfg(not(reifydb_target = "dst"))]
use std::future::Future;
use std::{sync::Arc, thread::available_parallelism};

use crate::{
	actor::system::ActorSystem,
	context::clock::{Clock, MockClock},
	pool::{PoolConfig, Pools},
};

/// Configuration for creating a [`SharedRuntime`].
#[derive(Clone)]
pub struct SharedRuntimeConfig {
	/// Number of worker threads for async runtime (ignored in WASM)
	pub async_threads: usize,
	/// Number of worker threads for the system pool (lightweight actors).
	pub system_threads: usize,
	/// Number of worker threads for the query pool (execution-heavy actors).
	pub query_threads: usize,
	/// Clock for time operations (defaults to real system clock)
	pub clock: Clock,
	/// Random number generator (defaults to OS entropy)
	pub rng: context::rng::Rng,
}

impl Default for SharedRuntimeConfig {
	fn default() -> Self {
		let cpus = available_parallelism().map_or(1, |n| n.get());
		Self {
			async_threads: 1,
			system_threads: cpus.min(4),
			query_threads: cpus,
			clock: Clock::Real,
			rng: context::rng::Rng::default(),
		}
	}
}

impl SharedRuntimeConfig {
	/// Set the number of async worker threads.
	pub fn async_threads(mut self, threads: usize) -> Self {
		self.async_threads = threads;
		self
	}

	/// Set the number of system pool threads (lightweight actors).
	pub fn system_threads(mut self, threads: usize) -> Self {
		self.system_threads = threads;
		self
	}

	/// Set the number of query pool threads (execution-heavy actors).
	pub fn query_threads(mut self, threads: usize) -> Self {
		self.query_threads = threads;
		self
	}

	/// Configure for deterministic testing with the given seed.
	/// Sets a mock clock starting at `seed` milliseconds and a seeded RNG.
	pub fn deterministic_testing(mut self, seed: u64) -> Self {
		self.clock = Clock::Mock(MockClock::from_millis(seed));
		self.rng = context::rng::Rng::seeded(seed);
		self
	}
}

// WASM runtime types - single-threaded execution support
use std::fmt;
#[cfg(target_arch = "wasm32")]
use std::{
	pin::Pin,
	task::{Context, Poll},
};

#[cfg(target_arch = "wasm32")]
use futures_util::future::LocalBoxFuture;
#[cfg(all(not(target_arch = "wasm32"), not(reifydb_target = "dst")))]
use tokio::runtime as tokio_runtime;
#[cfg(all(not(target_arch = "wasm32"), not(reifydb_target = "dst")))]
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
use std::error::Error;

#[cfg(target_arch = "wasm32")]
impl Error for WasmJoinError {}

/// Inner shared state for the runtime (native).
#[cfg(all(not(target_arch = "wasm32"), not(reifydb_target = "dst")))]
struct SharedRuntimeInner {
	system: ActorSystem,
	pools: Pools,
	clock: Clock,
	rng: context::rng::Rng,
}

#[cfg(all(not(target_arch = "wasm32"), not(reifydb_target = "dst")))]
impl Drop for SharedRuntimeInner {
	fn drop(&mut self) {
		self.system.shutdown();
		let _ = self.system.join();
	}
}

/// Inner shared state for the runtime (WASM).
#[cfg(target_arch = "wasm32")]
struct SharedRuntimeInner {
	system: ActorSystem,
	pools: Pools,
	clock: Clock,
	rng: context::rng::Rng,
}

/// Inner shared state for the runtime (DST).
#[cfg(reifydb_target = "dst")]
struct SharedRuntimeInner {
	system: ActorSystem,
	pools: Pools,
	clock: Clock,
	rng: context::rng::Rng,
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
	pub fn from_config(config: SharedRuntimeConfig) -> Self {
		let pools = Pools::new(PoolConfig {
			system_threads: config.system_threads,
			query_threads: config.query_threads,
			async_threads: config.async_threads,
		});
		let system = ActorSystem::new(pools.clone(), config.clock.clone());

		Self(Arc::new(SharedRuntimeInner {
			system,
			pools,
			clock: config.clock,
			rng: config.rng,
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

	/// Get the RNG for this runtime (shared across all threads).
	pub fn rng(&self) -> &context::rng::Rng {
		&self.0.rng
	}

	/// Get the pools.
	pub fn pools(&self) -> Pools {
		self.0.pools.clone()
	}

	/// Get a handle to the async runtime.
	#[cfg(all(not(target_arch = "wasm32"), not(reifydb_target = "dst")))]
	pub fn handle(&self) -> tokio_runtime::Handle {
		self.0.pools.handle()
	}

	/// Get a handle to the async runtime.
	#[cfg(target_arch = "wasm32")]
	pub fn handle(&self) -> WasmHandle {
		WasmHandle
	}

	/// Spawn a future onto the runtime.
	#[cfg(all(not(target_arch = "wasm32"), not(reifydb_target = "dst")))]
	pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
	where
		F: Future + Send + 'static,
		F::Output: Send + 'static,
	{
		self.0.pools.spawn(future)
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
	#[cfg(all(not(target_arch = "wasm32"), not(reifydb_target = "dst")))]
	pub fn block_on<F>(&self, future: F) -> F::Output
	where
		F: Future,
	{
		self.0.pools.block_on(future)
	}

	/// Block the current thread until the future completes.
	#[cfg(target_arch = "wasm32")]
	pub fn block_on<F>(&self, _future: F) -> F::Output
	where
		F: Future,
	{
		unimplemented!("block_on not supported in WASM - use async execution instead")
	}
}

impl fmt::Debug for SharedRuntime {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("SharedRuntime").finish_non_exhaustive()
	}
}

// Keep existing tests but gate them by target
#[cfg(all(test, not(reifydb_single_threaded)))]
mod tests {
	use super::*;

	fn test_config() -> SharedRuntimeConfig {
		SharedRuntimeConfig::default().async_threads(2).system_threads(2).query_threads(2)
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
		let _system = runtime.actor_system();
	}
}
