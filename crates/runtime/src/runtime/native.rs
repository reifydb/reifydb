// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Native runtime implementation using tokio multi-threaded runtime.

use std::{future::Future, sync::Arc};

use tokio::{
	runtime::{Handle, Runtime},
	task::JoinHandle,
};

use crate::compute::native::NativeComputePool;

/// Inner shared runtime state.
struct Inner {
	runtime: Runtime,
	compute_pool: NativeComputePool,
}

/// Native runtime implementation using tokio.
pub(crate) struct NativeRuntime {
	inner: Arc<Inner>,
}

impl NativeRuntime {
	/// Create a new native runtime.
	pub(crate) fn new(async_threads: usize, compute_threads: usize, compute_max_in_flight: usize) -> Self {
		let runtime = tokio::runtime::Builder::new_multi_thread()
			.worker_threads(async_threads)
			.thread_name("async")
			.enable_all()
			.build()
			.expect("Failed to create tokio runtime");

		let compute_pool = NativeComputePool::new(compute_threads, compute_max_in_flight);

		Self {
			inner: Arc::new(Inner {
				runtime,
				compute_pool,
			}),
		}
	}

	/// Get a handle to the async runtime.
	pub(crate) fn handle(&self) -> Handle {
		self.inner.runtime.handle().clone()
	}

	/// Get the compute pool for CPU-bound work.
	pub(crate) fn compute_pool(&self) -> NativeComputePool {
		self.inner.compute_pool.clone()
	}

	/// Spawn a future onto the runtime.
	pub(crate) fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
	where
		F: Future + Send + 'static,
		F::Output: Send + 'static,
	{
		self.inner.runtime.spawn(future)
	}

	/// Block the current thread until the future completes.
	pub(crate) fn block_on<F>(&self, future: F) -> F::Output
	where
		F: Future,
	{
		self.inner.runtime.block_on(future)
	}
}

impl Clone for NativeRuntime {
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
		}
	}
}
