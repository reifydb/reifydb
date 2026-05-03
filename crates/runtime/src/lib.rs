// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
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
use std::sync::Arc;

use crate::{
	actor::system::ActorSystem,
	context::clock::{Clock, MockClock},
	pool::{PoolConfig, Pools},
};

#[derive(Clone)]
pub struct SharedRuntimeConfig {
	pub clock: Clock,
	pub rng: context::rng::Rng,
}

impl Default for SharedRuntimeConfig {
	fn default() -> Self {
		Self {
			clock: Clock::Real,
			rng: context::rng::Rng::default(),
		}
	}
}

impl SharedRuntimeConfig {
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

#[cfg(target_arch = "wasm32")]
struct SharedRuntimeInner {
	system: ActorSystem,
	pools: Pools,
	clock: Clock,
	rng: context::rng::Rng,
}

#[cfg(reifydb_target = "dst")]
struct SharedRuntimeInner {
	system: ActorSystem,
	pools: Pools,
	clock: Clock,
	rng: context::rng::Rng,
}

#[derive(Clone)]
pub struct SharedRuntime(Arc<SharedRuntimeInner>);

impl SharedRuntime {
	pub fn from_config(config: SharedRuntimeConfig, pools: PoolConfig) -> Self {
		let pools = Pools::new(pools);
		let system = ActorSystem::new(pools.clone(), config.clock.clone());

		Self(Arc::new(SharedRuntimeInner {
			system,
			pools,
			clock: config.clock,
			rng: config.rng,
		}))
	}

	pub fn actor_system(&self) -> ActorSystem {
		self.0.system.clone()
	}

	pub fn clock(&self) -> &Clock {
		&self.0.clock
	}

	pub fn rng(&self) -> &context::rng::Rng {
		&self.0.rng
	}

	pub fn pools(&self) -> Pools {
		self.0.pools.clone()
	}

	#[cfg(all(not(target_arch = "wasm32"), not(reifydb_target = "dst")))]
	pub fn handle(&self) -> tokio_runtime::Handle {
		self.0.pools.handle()
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
		self.0.pools.spawn(future)
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
		self.0.pools.block_on(future)
	}

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

#[cfg(all(test, not(reifydb_single_threaded)))]
mod tests {
	use super::*;

	fn test_config() -> SharedRuntimeConfig {
		SharedRuntimeConfig::default()
	}

	fn test_pools() -> PoolConfig {
		PoolConfig {
			async_threads: 2,
			system_threads: 2,
			query_threads: 2,
		}
	}

	#[test]
	fn test_runtime_creation() {
		let runtime = SharedRuntime::from_config(test_config(), test_pools());
		let result = runtime.block_on(async { 42 });
		assert_eq!(result, 42);
	}

	#[test]
	fn test_runtime_clone_shares_same_runtime() {
		let rt1 = SharedRuntime::from_config(test_config(), test_pools());
		let rt2 = rt1.clone();
		assert!(Arc::ptr_eq(&rt1.0, &rt2.0));
	}

	#[test]
	fn test_spawn() {
		let runtime = SharedRuntime::from_config(test_config(), test_pools());
		let handle = runtime.spawn(async { 123 });
		let result = runtime.block_on(handle).unwrap();
		assert_eq!(result, 123);
	}

	#[test]
	fn test_actor_system_accessible() {
		let runtime = SharedRuntime::from_config(test_config(), test_pools());
		let _system = runtime.actor_system();
	}
}
