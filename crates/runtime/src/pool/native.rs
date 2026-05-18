// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{future::Future, mem::ManuallyDrop, sync::Arc, time::Duration};

use rayon::{ThreadPool, ThreadPoolBuilder};
use tokio::{
	runtime::{self, Runtime},
	task::JoinHandle,
};

use super::PoolConfig;

struct PoolsInner {
	system: Arc<ThreadPool>,
	query: Arc<ThreadPool>,
	tokio: Option<ManuallyDrop<Runtime>>,
}

impl Drop for PoolsInner {
	fn drop(&mut self) {
		if let Some(rt) = self.tokio.as_mut() {
			let rt = unsafe { ManuallyDrop::take(rt) };
			rt.shutdown_timeout(Duration::from_secs(5));
		}
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
		let system = Arc::new(
			ThreadPoolBuilder::new()
				.num_threads(config.system_threads)
				.thread_name(|i| format!("system-pool-{i}"))
				.build()
				.expect("failed to build system thread pool"),
		);
		let query = Arc::new(
			ThreadPoolBuilder::new()
				.num_threads(config.query_threads)
				.thread_name(|i| format!("query-pool-{i}"))
				.build()
				.expect("failed to build query thread pool"),
		);
		let tokio = if config.async_threads > 0 {
			let rt = runtime::Builder::new_multi_thread()
				.worker_threads(config.async_threads)
				.thread_name("async")
				.enable_all()
				.build()
				.expect("failed to build tokio runtime");
			Some(ManuallyDrop::new(rt))
		} else {
			None
		};

		Self {
			inner: Arc::new(PoolsInner {
				system,
				query,
				tokio,
			}),
		}
	}

	pub fn system_pool(&self) -> &Arc<ThreadPool> {
		&self.inner.system
	}

	pub fn system_thread_count(&self) -> usize {
		self.inner.system.current_num_threads()
	}

	pub fn query_pool(&self) -> &Arc<ThreadPool> {
		&self.inner.query
	}

	fn tokio(&self) -> &Runtime {
		self.inner.tokio.as_ref().expect("no tokio runtime configured (async_threads = 0)")
	}

	pub fn handle(&self) -> runtime::Handle {
		self.tokio().handle().clone()
	}

	pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
	where
		F: Future + Send + 'static,
		F::Output: Send + 'static,
	{
		self.tokio().spawn(future)
	}

	pub fn block_on<F>(&self, future: F) -> F::Output
	where
		F: Future,
	{
		self.tokio().block_on(future)
	}
}
