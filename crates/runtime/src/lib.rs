// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

//! Process-level runtime: actor system, thread pools, async executors, time and randomness, and the synchronisation
//! primitives the rest of the workspace builds on. The `SharedRuntime` handle carries the actor system, the pool set,
//! the clock, and the seeded RNG together so any subsystem that needs to spawn work, sleep, or generate ids gets a
//! consistent view of the world.
//!
//! The crate abstracts platform differences: native targets get a tokio-backed pool, WebAssembly gets a single-task
//! executor, the deterministic-simulation target (`reifydb_target = "dst"`) gets a virtual scheduler. All three sit
//! behind the same `SharedRuntime` API so callers do not branch on platform.
//!
//! Invariant: `SharedRuntime::seeded(...)` is what produces a deterministic ReifyDB - same seed, same trace. Any
//! source of non-determinism inside the runtime (an unmocked clock, an unseeded RNG, a pool that schedules outside
//! the seeded executor) defeats DST replays and breaks the simulation harness.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![allow(clippy::tabs_in_doc_comments)]
#![allow(dead_code)]

pub mod context;

pub mod hash;

pub mod pool;

pub mod sync;

pub mod actor;

#[cfg(not(reifydb_target = "dst"))]
use std::future::Future;

use crate::{
	actor::system::ActorSystem,
	context::clock::{Clock, MockClock},
	pool::{PoolConfig, Pools},
};

#[derive(Clone)]
pub struct RuntimeConfig {
	pub clock: Clock,
	pub rng: context::rng::Rng,
}

impl Default for RuntimeConfig {
	fn default() -> Self {
		Self {
			clock: Clock::Real,
			rng: context::rng::Rng::default(),
		}
	}
}

impl RuntimeConfig {
	pub fn seeded(mut self, seed: u64) -> Self {
		self.clock = Clock::Mock(MockClock::from_millis(seed));
		self.rng = context::rng::Rng::seeded(seed);
		self
	}
}

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

#[cfg(target_arch = "wasm32")]
#[derive(Clone, Copy, Debug)]
pub struct WasmHandle;

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

use crate::actor::system::ActorSpawner;

pub struct Runtime {
	system: ActorSystem,
	pools: Pools,
	clock: Clock,
	rng: context::rng::Rng,
}

impl Runtime {
	pub fn from_config(config: RuntimeConfig, pools: PoolConfig) -> Self {
		let pools = Pools::new(pools);
		let system = ActorSystem::new(pools.clone(), config.clock.clone());

		Self {
			system,
			pools,
			clock: config.clock,
			rng: config.rng,
		}
	}

	pub fn actor_system(&self) -> ActorSystem {
		self.system.clone()
	}

	pub fn spawner(&self) -> ActorSpawner {
		self.system.spawner()
	}

	pub fn shutdown(&self) {
		self.system.shutdown();
		let _ = self.system.join();
		self.pools.shutdown();
	}

	pub fn clock(&self) -> &Clock {
		&self.clock
	}

	pub fn rng(&self) -> &context::rng::Rng {
		&self.rng
	}

	#[cfg(all(not(target_arch = "wasm32"), not(reifydb_target = "dst")))]
	pub fn handle(&self) -> tokio_runtime::Handle {
		self.pools.handle()
	}

	#[cfg(target_arch = "wasm32")]
	pub fn handle(&self) -> WasmHandle {
		WasmHandle
	}

	#[cfg(all(not(target_arch = "wasm32"), not(reifydb_target = "dst")))]
	pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
	where
		F: Future + Send + 'static,
		F::Output: Send + 'static,
	{
		self.pools.spawn(future)
	}

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

	#[cfg(all(not(target_arch = "wasm32"), not(reifydb_target = "dst")))]
	pub fn block_on<F>(&self, future: F) -> F::Output
	where
		F: Future,
	{
		self.pools.block_on(future)
	}

	#[cfg(target_arch = "wasm32")]
	pub fn block_on<F>(&self, _future: F) -> F::Output
	where
		F: Future,
	{
		unimplemented!("block_on not supported in WASM - use async execution instead")
	}
}

impl Drop for Runtime {
	fn drop(&mut self) {
		eprintln!("[chaos-leak] Runtime::drop running");
		self.shutdown();
	}
}

impl fmt::Debug for Runtime {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Runtime").finish_non_exhaustive()
	}
}

#[cfg(all(test, not(reifydb_single_threaded)))]
mod tests {
	use super::*;

	fn test_config() -> RuntimeConfig {
		RuntimeConfig::default()
	}

	fn test_pools() -> PoolConfig {
		PoolConfig {
			async_threads: 2,
			system_threads: 2,
			query_threads: 2,
			commit_threads: 2,
			background_threads: 1,
		}
	}

	#[test]
	fn test_runtime_creation() {
		let runtime = Runtime::from_config(test_config(), test_pools());
		let result = runtime.block_on(async { 42 });
		assert_eq!(result, 42);
	}

	#[test]
	fn test_spawn() {
		let runtime = Runtime::from_config(test_config(), test_pools());
		let handle = runtime.spawn(async { 123 });
		let result = runtime.block_on(handle).unwrap();
		assert_eq!(result, 123);
	}

	#[test]
	fn test_actor_system_accessible() {
		let runtime = Runtime::from_config(test_config(), test_pools());
		let _system = runtime.actor_system();
	}

	#[test]
	fn test_shutdown_drops_runtime() {
		let runtime = Runtime::from_config(test_config(), test_pools());
		let spawner = runtime.spawner();
		assert!(spawner.is_alive());
		drop(runtime);
		assert!(!spawner.is_alive());
	}
}
