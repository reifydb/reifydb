// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

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
///
/// # Example
///
/// ```ignore
/// let pool = ComputePool::new(4, 16); // 4 threads, max 16 in-flight
///
/// let result = pool.compute(|| {
///     expensive_calculation()
/// }).await?;
/// ```
#[derive(Clone)]
pub struct ComputePool {
	inner: Arc<Inner>,
}

impl ComputePool {
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
	pub fn new(threads: usize, max_in_flight: usize) -> Self {
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

#[cfg(test)]
mod tests {
	use std::{
		sync::atomic::{AtomicUsize, Ordering},
		time::Duration,
	};

	use super::*;

	#[tokio::test]
	async fn test_compute_returns_result() {
		let pool = ComputePool::new(2, 4);
		let result = pool.compute(|| 42).await.unwrap();
		assert_eq!(result, 42);
	}

	#[tokio::test]
	async fn test_compute_runs_on_rayon_thread() {
		let pool = ComputePool::new(2, 4);
		let thread_index = pool.compute(|| rayon::current_thread_index()).await.unwrap();
		assert!(thread_index.is_some());
	}

	#[tokio::test]
	async fn test_admission_control_limits_concurrency() {
		let pool = ComputePool::new(4, 2); // 4 threads but only 2 in-flight
		let concurrent = Arc::new(AtomicUsize::new(0));
		let max_concurrent = Arc::new(AtomicUsize::new(0));

		let mut handles = vec![];
		for _ in 0..10 {
			let pool = pool.clone();
			let concurrent = concurrent.clone();
			let max_concurrent = max_concurrent.clone();

			handles.push(tokio::spawn(async move {
				pool.compute(move || {
					let current = concurrent.fetch_add(1, Ordering::SeqCst) + 1;
					max_concurrent.fetch_max(current, Ordering::SeqCst);
					std::thread::sleep(Duration::from_millis(50));
					concurrent.fetch_sub(1, Ordering::SeqCst);
				})
				.await
				.unwrap();
			}));
		}

		for h in handles {
			h.await.unwrap();
		}

		assert!(max_concurrent.load(Ordering::SeqCst) <= 2);
	}

	#[tokio::test]
	async fn test_compute_propagates_result_types() {
		let pool = ComputePool::new(1, 1);

		let string = pool.compute(|| String::from("hello")).await.unwrap();
		assert_eq!(string, "hello");

		let vec = pool.compute(|| vec![1, 2, 3]).await.unwrap();
		assert_eq!(vec, vec![1, 2, 3]);
	}

	#[tokio::test]
	async fn test_compute_propagates_panic() {
		let pool = ComputePool::new(1, 1);

		let result = pool
			.compute(|| {
				panic!("intentional panic");
			})
			.await;

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert!(err.is_panic());
	}
}
