// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Native compute pool implementation using rayon.

use std::sync::Arc;

use rayon::{ThreadPool, ThreadPoolBuilder};
use tokio::{sync::Semaphore, task};

struct Inner {
	pool: ThreadPool,
	permits: Arc<Semaphore>,
}

/// A compute pool for running CPU-bound tasks with admission control.
///
/// Wraps a dedicated rayon [`ThreadPool`] with a [`Semaphore`] to limit
/// the number of concurrent in-flight tasks, preventing resource exhaustion
/// under high load.
#[derive(Clone)]
pub struct NativeComputePool {
	inner: Arc<Inner>,
}

impl NativeComputePool {
	/// Creates a new compute pool.
	///
	/// # Arguments
	///
	/// * `threads` - Number of worker threads in the rayon pool
	/// * `max_in_flight` - Maximum concurrent tasks (admission control)
	///
	/// # Panics
	///
	/// Panics if the rayon thread pool fails to build.
	pub(crate) fn new(threads: usize, max_in_flight: usize) -> Self {
		let pool = ThreadPoolBuilder::new()
			.num_threads(threads)
			.thread_name(|i| format!("compute-{i}"))
			.build()
			.expect("failed to build rayon pool");

		Self {
			inner: Arc::new(Inner {
				pool,
				permits: Arc::new(Semaphore::new(max_in_flight)),
			}),
		}
	}

	/// Executes a closure on the rayon thread pool directly.
	///
	/// Unlike [`compute`], this is synchronous and bypasses admission control.
	/// Use this when you're already in a synchronous context and need parallel execution.
	pub fn install<R, F>(&self, f: F) -> R
	where
		R: Send,
		F: FnOnce() -> R + Send,
	{
		self.inner.pool.install(f)
	}

	/// Runs a CPU-bound function on the compute pool.
	///
	/// The task is scheduled via `spawn_blocking` and executed on the
	/// dedicated rayon pool using `install`. Admission control ensures
	/// no more than `max_in_flight` tasks run concurrently.
	///
	/// # Panics
	///
	/// Panics if the semaphore is closed (should not happen in normal use).
	pub async fn compute<R, F>(&self, f: F) -> Result<R, task::JoinError>
	where
		R: Send + 'static,
		F: FnOnce() -> R + Send + 'static,
	{
		let permit = self.inner.permits.clone().acquire_owned().await.expect("semaphore closed");
		let inner = self.inner.clone();

		let handle = task::spawn_blocking(move || {
			let _permit = permit; // released when closure returns
			inner.pool.install(f)
		});

		handle.await
	}
}
