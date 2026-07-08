// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![allow(clippy::disallowed_types)]

use std::{future::Future, sync::Arc, time::Duration};

use reifydb_value::reifydb_assertions;
use tokio::{
	runtime::{self, Handle, Runtime},
	task::JoinHandle,
};

use super::PoolConfig;
use crate::{
	pool::{
		actor_pool::{ActorPool, Schedule},
		compute::ComputePool,
		task::TaskPool,
	},
	sync::mutex::Mutex,
};

struct PoolsInner {
	actors: ActorPool,
	task: TaskPool,
	compute: ComputePool,
	tokio_handle: Option<Handle>,
	tokio: Mutex<Option<Runtime>>,
}

impl PoolsInner {
	fn take_tokio(&self) -> Option<Runtime> {
		self.tokio.lock().take()
	}
}

impl Drop for PoolsInner {
	fn drop(&mut self) {
		if let Some(rt) = self.take_tokio() {
			if runtime::Handle::try_current().is_err() {
				rt.shutdown_timeout(Duration::from_secs(5));
			} else {
				rt.shutdown_background();
			}
		}
		self.actors.shutdown();
		self.task.shutdown();
	}
}

#[derive(Clone)]
pub struct Pools {
	inner: Arc<PoolsInner>,
}

impl Default for Pools {
	fn default() -> Self {
		Self::new(PoolConfig::default())
	}
}

impl Pools {
	pub fn new(config: PoolConfig) -> Self {
		let actors = ActorPool::new(config.coordination_threads, config.flow_threads);
		let task = TaskPool::new(config.task_threads, "task");
		let compute = ComputePool::new(config.compute_threads, "compute");
		let (tokio_handle, tokio) = Self::build_async_runtime(config.async_threads);

		Self {
			inner: Arc::new(PoolsInner {
				actors,
				task,
				compute,
				tokio_handle,
				tokio,
			}),
		}
	}

	#[inline]
	fn build_async_runtime(threads: usize) -> (Option<Handle>, Mutex<Option<Runtime>>) {
		let (tokio_handle, tokio) = if threads > 0 {
			let rt = runtime::Builder::new_multi_thread()
				.worker_threads(threads)
				.thread_name("async")
				.enable_all()
				.build()
				.expect("failed to build tokio runtime");
			let handle = rt.handle().clone();
			(Some(handle), Mutex::new(Some(rt)))
		} else {
			(None, Mutex::new(None))
		};

		reifydb_assertions! {
			let handle_present = tokio_handle.is_some();
			let runtime_present = tokio.lock().is_some();
			assert!(
				handle_present == runtime_present,
				"async handle/runtime presence must agree (handle_present={handle_present}, runtime_present={runtime_present}); \
				 a handle without its runtime makes tokio_handle() dispatch onto a runtime that Drop never shuts down (thread leak), \
				 and a runtime without a handle makes spawn()/block_on() panic via the expect in tokio_handle()"
			);
		}

		(tokio_handle, tokio)
	}

	pub fn shutdown(&self) {
		if let Some(rt) = self.inner.take_tokio() {
			if runtime::Handle::try_current().is_err() {
				rt.shutdown_timeout(Duration::from_secs(5));
			} else {
				rt.shutdown_background();
			}
		}
		self.inner.actors.shutdown();
		self.inner.task.shutdown();
	}

	pub fn spawn_task(&self, job: impl FnOnce() + Send + 'static) {
		self.inner.task.spawn(job);
	}

	pub fn task_thread_count(&self) -> usize {
		self.inner.task.thread_count()
	}

	pub fn compute(&self) -> &ComputePool {
		&self.inner.compute
	}

	pub fn compute_thread_count(&self) -> usize {
		self.inner.compute.thread_count()
	}

	pub fn coordination_thread_count(&self) -> usize {
		self.inner.actors.coordination().thread_count()
	}

	pub fn flow_thread_count(&self) -> usize {
		self.inner.actors.flow().thread_count()
	}

	pub(crate) fn actor_pool(&self) -> &ActorPool {
		&self.inner.actors
	}

	pub(crate) fn task_injector(&self) -> Schedule {
		Schedule::Injector(self.inner.task.injector())
	}

	fn tokio_handle(&self) -> Handle {
		self.inner.tokio_handle.clone().expect("no tokio runtime configured (async_threads = 0)")
	}

	pub fn handle(&self) -> Handle {
		self.tokio_handle()
	}

	pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
	where
		F: Future + Send + 'static,
		F::Output: Send + 'static,
	{
		self.tokio_handle().spawn(future)
	}

	pub fn block_on<F>(&self, future: F) -> F::Output
	where
		F: Future,
	{
		self.tokio_handle().block_on(future)
	}
}
