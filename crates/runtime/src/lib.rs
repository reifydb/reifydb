// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod cache;

pub mod context;

pub mod fatal;

pub mod pool;

pub mod shutdown;

pub mod sync;

pub mod actor;

pub mod version_epoch;

#[cfg(reifydb_dst)]
pub mod testing;

#[cfg(not(reifydb_dst))]
use std::future::Future;

use crate::{
	actor::system::ActorSystem,
	context::clock::{Clock, MockClock},
	pool::{PoolConfig, Pools},
	shutdown::Shutdown,
};

#[derive(Clone)]
pub struct RuntimeConfig {
	pub clock: Clock,
	pub rng: context::rng::Rng,
	pub fatal: fatal::FatalConfig,
}

impl Default for RuntimeConfig {
	fn default() -> Self {
		Self {
			clock: Clock::Real,
			rng: context::rng::Rng::default(),
			fatal: fatal::FatalConfig::default(),
		}
	}
}

impl RuntimeConfig {
	pub fn seeded(mut self, seed: u64) -> Self {
		self.clock = Clock::Mock(MockClock::from_millis(seed));
		self.rng = context::rng::Rng::seeded(seed);
		self
	}

	pub fn fatal(mut self, config: fatal::FatalConfig) -> Self {
		self.fatal = config;
		self
	}

	pub fn clock(mut self, clock: Clock) -> Self {
		self.clock = clock;
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
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use tokio::runtime as tokio_runtime;
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
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

	pub fn handle(&self) -> RuntimeHandle {
		RuntimeHandle {
			system: self.system.clone(),
			pools: self.pools.clone(),
			clock: self.clock.clone(),
			rng: self.rng.clone(),
		}
	}

	pub fn actor_system(&self) -> ActorSystem {
		self.system.clone()
	}

	pub fn spawner(&self) -> ActorSpawner {
		self.system.spawner()
	}

	pub fn clock(&self) -> &Clock {
		&self.clock
	}

	pub fn rng(&self) -> &context::rng::Rng {
		&self.rng
	}

	#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
	pub fn tokio(&self) -> tokio_runtime::Handle {
		self.pools.handle()
	}

	#[cfg(target_arch = "wasm32")]
	pub fn tokio(&self) -> WasmHandle {
		WasmHandle
	}

	#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
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

	#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
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

impl Shutdown for Runtime {
	fn shutdown(&self) {
		self.system.shutdown();
		let _ = self.system.join();
		self.pools.shutdown();
	}
}

impl Drop for Runtime {
	fn drop(&mut self) {
		self.shutdown();
	}
}

impl fmt::Debug for Runtime {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Runtime").finish_non_exhaustive()
	}
}

#[derive(Clone)]
pub struct RuntimeHandle {
	system: ActorSystem,
	pools: Pools,
	clock: Clock,
	rng: context::rng::Rng,
}

impl RuntimeHandle {
	pub fn actor_system(&self) -> ActorSystem {
		self.system.clone()
	}

	pub fn spawner(&self) -> ActorSpawner {
		self.system.spawner()
	}

	pub fn pools(&self) -> Pools {
		self.pools.clone()
	}

	pub fn clock(&self) -> &Clock {
		&self.clock
	}

	pub fn rng(&self) -> &context::rng::Rng {
		&self.rng
	}

	#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
	pub fn tokio(&self) -> tokio_runtime::Handle {
		self.pools.handle()
	}

	#[cfg(target_arch = "wasm32")]
	pub fn tokio(&self) -> WasmHandle {
		WasmHandle
	}

	#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
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

	#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
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

impl fmt::Debug for RuntimeHandle {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("RuntimeHandle").finish_non_exhaustive()
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
			coordination_threads: 2,
			flow_threads: 2,
			maintenance_threads: 1,
			task_threads: 2,
			compute_threads: 2,
			async_threads: 2,
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
