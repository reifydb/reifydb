// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{
	fmt::Debug,
	ops::Deref,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	time::Duration,
};

use reifydb_core::CommitVersion;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::instrument;

use crate::multi::watermark::Closer;

pub struct WatermarkInner {
	pub(crate) done_until: AtomicU64,
	pub(crate) last_index: AtomicU64,
	pub(crate) tx: mpsc::UnboundedSender<Mark>,
	pub(crate) processor_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

#[derive(Debug)]
pub(crate) struct Mark {
	pub(crate) version: u64,
	pub(crate) waiter: Option<oneshot::Sender<()>>,
	pub(crate) done: bool,
}

/// WaterMark is used to keep track of the minimum un-finished index. Typically,
/// an index k becomes finished or "done" according to a WaterMark once
/// `done(k)` has been called
///  1. as many times as `begin(k)` has, AND
///  2. a positive number of times.
pub struct WaterMark(Arc<WatermarkInner>);

impl Debug for WaterMark {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("WaterMark")
			.field("done_until", &self.done_until.load(Ordering::Relaxed))
			.field("last_index", &self.last_index.load(Ordering::Relaxed))
			.finish()
	}
}

impl Deref for WaterMark {
	type Target = WatermarkInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl WaterMark {
	/// Create a new WaterMark with given name and closer.
	#[instrument(name = "transaction::watermark::new", level = "debug", skip(closer), fields(task_name = %task_name))]
	pub async fn new(task_name: String, closer: Closer) -> Self {
		let (tx, rx) = mpsc::unbounded_channel();

		let inner = Arc::new(WatermarkInner {
			done_until: AtomicU64::new(0),
			last_index: AtomicU64::new(0),
			tx,
			processor_task: Mutex::new(None),
		});

		// Subscribe to shutdown signal BEFORE spawning to avoid race condition
		let shutdown_rx = closer.listen();

		let processing_inner = inner.clone();
		let task_handle = tokio::spawn(async move {
			processing_inner.process(rx, closer, shutdown_rx).await;
		});

		// Store the task handle
		*inner.processor_task.lock().await = Some(task_handle);

		Self(inner)
	}

	/// Sets the last index to the given value.
	#[instrument(name = "transaction::watermark::begin", level = "trace", skip(self), fields(version = version.0))]
	pub fn begin(&self, version: CommitVersion) {
		// Update last_index to the maximum
		self.last_index.fetch_max(version.0, Ordering::SeqCst);

		let _ = self.tx.send(Mark {
			version: version.0,
			waiter: None,
			done: false,
		});
	}

	/// Sets a single index as done.
	#[instrument(name = "transaction::watermark::done", level = "trace", skip(self), fields(index = index.0))]
	pub fn done(&self, index: CommitVersion) {
		let _ = self.tx.send(Mark {
			version: index.0,
			waiter: None,
			done: true,
		});
	}

	/// Returns the maximum index that has the property that all indices
	/// less than or equal to it are done.
	pub fn done_until(&self) -> CommitVersion {
		CommitVersion(self.done_until.load(Ordering::SeqCst))
	}

	/// Waits until the given index is marked as done with a default
	/// timeout.
	pub async fn wait_for_mark(&self, index: u64) {
		self.wait_for_mark_timeout(CommitVersion(index), Duration::from_secs(30)).await;
	}

	/// Waits until the given index is marked as done with a specified
	/// timeout.
	pub async fn wait_for_mark_timeout(&self, index: CommitVersion, timeout: Duration) -> bool {
		if self.done_until.load(Ordering::SeqCst) >= index.0 {
			return true;
		}

		let (wait_tx, wait_rx) = oneshot::channel();

		if self.tx
			.send(Mark {
				version: index.0,
				waiter: Some(wait_tx),
				done: false,
			})
			.is_err()
		{
			// Channel closed
			return false;
		}

		// Wait with timeout
		match tokio::time::timeout(timeout, wait_rx).await {
			Ok(Ok(_)) => true,
			Ok(Err(_)) => {
				// Channel closed without signal
				false
			}
			Err(_) => {
				// Timeout occurred
				false
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::{sync::atomic::AtomicUsize, time::Instant};

	use tokio::time::sleep;

	use super::*;

	#[tokio::test]
	async fn test_basic() {
		init_and_close(|_| async {}).await;
	}

	#[tokio::test]
	async fn test_begin_done() {
		init_and_close(|watermark| async move {
			watermark.begin(CommitVersion(1));
			watermark.begin(CommitVersion(2));
			watermark.begin(CommitVersion(3));

			watermark.done(CommitVersion(1));
			watermark.done(CommitVersion(2));
			watermark.done(CommitVersion(3));
		})
		.await;
	}

	#[tokio::test]
	async fn test_wait_for_mark() {
		init_and_close(|watermark| async move {
			watermark.begin(CommitVersion(1));
			watermark.begin(CommitVersion(2));
			watermark.begin(CommitVersion(3));

			watermark.done(CommitVersion(2));
			watermark.done(CommitVersion(3));

			assert_eq!(watermark.done_until(), 0);

			watermark.done(CommitVersion(1));
			watermark.wait_for_mark(1).await;
			watermark.wait_for_mark(3).await;
			assert_eq!(watermark.done_until(), 3);
		})
		.await;
	}

	#[tokio::test]
	async fn test_done_until() {
		init_and_close(|watermark| async move {
			watermark.done_until.store(1, Ordering::SeqCst);
			assert_eq!(watermark.done_until(), 1);
		})
		.await;
	}

	#[tokio::test]
	async fn test_high_concurrency() {
		let closer = Closer::new(1);
		let watermark = Arc::new(WaterMark::new("concurrent".into(), closer.clone()).await);

		const NUM_TASKS: usize = 50;
		const OPS_PER_TASK: usize = 100;

		let mut handles = vec![];

		// Spawn tasks that perform concurrent begin/done operations
		for task_id in 0..NUM_TASKS {
			let wm = watermark.clone();
			let handle = tokio::spawn(async move {
				for i in 0..OPS_PER_TASK {
					let version = CommitVersion((task_id * OPS_PER_TASK + i) as u64 + 1);
					wm.begin(version);
					wm.done(version);
				}
			});
			handles.push(handle);
		}

		for handle in handles {
			handle.await.expect("Task panicked");
		}

		sleep(Duration::from_millis(100)).await;

		// Verify the watermark progressed
		let final_done = watermark.done_until();
		assert!(final_done.0 > 0, "Watermark should have progressed");

		closer.signal_and_wait().await;
	}

	#[tokio::test]
	async fn test_concurrent_wait_for_mark() {
		let closer = Closer::new(1);
		let watermark = Arc::new(WaterMark::new("wait_concurrent".into(), closer.clone()).await);
		let success_count = Arc::new(AtomicUsize::new(0));

		// Start some versions
		for i in 1..=10 {
			watermark.begin(CommitVersion(i));
		}

		let mut handles = vec![];

		// Spawn tasks that wait for marks
		for version in 1..=10 {
			let wm = watermark.clone();
			let counter = success_count.clone();
			let handle = tokio::spawn(async move {
				// Use timeout to avoid hanging if something goes wrong
				if wm.wait_for_mark_timeout(CommitVersion(version), Duration::from_secs(5)).await {
					counter.fetch_add(1, Ordering::Relaxed);
				}
			});
			handles.push(handle);
		}

		// Give tasks time to start waiting
		sleep(Duration::from_millis(50)).await;

		// Complete the versions
		for i in 1..=10 {
			watermark.done(CommitVersion(i));
		}

		for handle in handles {
			handle.await.expect("Task panicked");
		}

		// All waits should have succeeded
		assert_eq!(success_count.load(Ordering::Relaxed), 10);

		closer.signal_and_wait().await;
	}

	#[tokio::test]
	async fn test_old_version_rejection() {
		init_and_close(|watermark| async move {
			// Advance done_until significantly
			for i in 1..=100 {
				watermark.begin(CommitVersion(i));
				watermark.done(CommitVersion(i));
			}

			// Wait for processing
			sleep(Duration::from_millis(50)).await;

			let done_until = watermark.done_until();
			assert!(done_until.0 >= 50, "Should have processed many versions");

			// Try to wait for a very old version (should return immediately)
			let very_old = done_until.0.saturating_sub(super::super::OLD_VERSION_THRESHOLD + 10);
			let start = Instant::now();
			watermark.wait_for_mark(very_old).await;
			let elapsed = start.elapsed();

			// Should return almost immediately (< 10ms)
			assert!(elapsed.as_millis() < 10, "Old version wait should return immediately");
		})
		.await;
	}

	#[tokio::test]
	async fn test_timeout_behavior() {
		init_and_close(|watermark| async move {
			// Begin but don't complete a version
			watermark.begin(CommitVersion(1));

			// Wait with short timeout
			let start = Instant::now();
			let result =
				watermark.wait_for_mark_timeout(CommitVersion(1), Duration::from_millis(100)).await;
			let elapsed = start.elapsed();

			// Should timeout and return false
			assert!(!result, "Should timeout waiting for uncompleted version");
			assert!(
				elapsed.as_millis() >= 100 && elapsed.as_millis() < 200,
				"Should respect timeout duration"
			);
		})
		.await;
	}

	async fn init_and_close<F, Fut>(f: F)
	where
		F: FnOnce(Arc<WaterMark>) -> Fut,
		Fut: Future<Output = ()>,
	{
		let closer = Closer::new(1);

		let watermark = Arc::new(WaterMark::new("watermark".into(), closer.clone()).await);
		f(watermark).await;

		sleep(Duration::from_millis(10)).await;
		closer.signal_and_wait().await;
	}
}
