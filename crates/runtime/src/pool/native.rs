// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

#![allow(clippy::disallowed_types)]

use std::{future::Future, sync::Arc, time::Duration};

use rayon::{ThreadPool, ThreadPoolBuilder};
use reifydb_value::reifydb_assertions;
use tokio::{
	runtime::{self, Handle, Runtime},
	task::JoinHandle,
};

use super::PoolConfig;
use crate::sync::mutex::Mutex;

struct PoolsInner {
	system: Arc<ThreadPool>,
	query: Arc<ThreadPool>,
	commit: Arc<ThreadPool>,
	background: Arc<ThreadPool>,
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
		let system = Self::build_pool(config.system_threads, "system-pool");
		let query = Self::build_pool(config.query_threads, "query-pool");
		let commit = Self::build_pool(config.commit_threads, "commit-pool");
		let background = Self::build_pool(config.background_threads, "background-pool");
		let (tokio_handle, tokio) = Self::build_async_runtime(config.async_threads);

		Self {
			inner: Arc::new(PoolsInner {
				system,
				query,
				commit,
				background,
				tokio_handle,
				tokio,
			}),
		}
	}

	fn build_pool(threads: usize, name_prefix: &'static str) -> Arc<ThreadPool> {
		Arc::new(
			ThreadPoolBuilder::new()
				.num_threads(threads)
				.thread_name(move |i| format!("{name_prefix}-{i}"))
				.build()
				.unwrap_or_else(|_| panic!("failed to build {name_prefix} thread pool")),
		)
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

	pub fn query_thread_count(&self) -> usize {
		self.inner.query.current_num_threads()
	}

	pub fn commit_pool(&self) -> &Arc<ThreadPool> {
		&self.inner.commit
	}

	pub fn commit_thread_count(&self) -> usize {
		self.inner.commit.current_num_threads()
	}

	pub fn background_pool(&self) -> &Arc<ThreadPool> {
		&self.inner.background
	}

	pub fn background_thread_count(&self) -> usize {
		self.inner.background.current_num_threads()
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
