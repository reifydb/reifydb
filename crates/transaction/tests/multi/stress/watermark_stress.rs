// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
	time::{Duration, Instant},
};

use reifydb_core::CommitVersion;
use reifydb_transaction::multi::watermark::{Closer, MAX_PENDING, MAX_WAITERS, WaterMark};
use tokio::time::sleep;

/// Test watermark with many pending versions to trigger cleanup
#[tokio::test]
async fn test_watermark_pending_cleanup() {
	let closer = Closer::new(1);
	let watermark = WaterMark::new("stress_pending".into(), closer.clone()).await;

	// Create many versions without completing them initially
	// This should trigger MAX_PENDING cleanup logic
	const NUM_VERSIONS: u64 = MAX_PENDING as u64 + 500;

	// Begin many versions
	for version in 1..=NUM_VERSIONS {
		watermark.begin(CommitVersion(version));

		// Complete some versions to allow progress
		if version % 3 == 0 {
			watermark.done(CommitVersion(version));
		}
	}

	// Give processing task time to handle cleanup
	sleep(Duration::from_millis(500)).await;

	// Now complete remaining versions
	for version in 1..=NUM_VERSIONS {
		if version % 3 != 0 {
			watermark.done(CommitVersion(version));
		}
	}

	// Give processing task time to catch up
	sleep(Duration::from_millis(500)).await;

	// Verify the system handled the load without panic
	let final_done = watermark.done_until();
	assert!(final_done > CommitVersion(0), "Watermark should have progressed despite high load");

	closer.signal_and_wait().await;
}

/// Test watermark with many concurrent waiters to trigger cleanup
#[tokio::test]
async fn test_watermark_waiters_cleanup() {
	let closer = Closer::new(1);
	let watermark = Arc::new(WaterMark::new("stress_waiters".into(), closer.clone()).await);

	const NUM_WAITERS: usize = MAX_WAITERS + 5000;

	for i in 1..=NUM_WAITERS as u64 {
		watermark.begin(CommitVersion(i));
	}

	let mut handles = vec![];
	let timeout_count = Arc::new(AtomicUsize::new(0));

	// Spawn many tasks that wait for versions
	for version in 1..=NUM_WAITERS as u64 {
		let wm = watermark.clone();
		let counter = timeout_count.clone();

		let handle = tokio::spawn(async move {
			if !wm.wait_for_mark_timeout(CommitVersion(version), Duration::from_secs(2)).await {
				counter.fetch_add(1, Ordering::Relaxed);
			}
		});
		handles.push(handle);
	}

	// Give tasks time to register as waiters
	sleep(Duration::from_millis(100)).await;

	// Now complete all versions
	for i in 1..=NUM_WAITERS as u64 {
		watermark.done(CommitVersion(i));
	}

	for handle in handles {
		handle.await.expect("Task panicked");
	}

	// Most waiters should have succeeded (some timeouts are acceptable)
	let timeouts = timeout_count.load(Ordering::Relaxed);
	assert!(timeouts < NUM_WAITERS / 10, "Too many timeouts: {}/{}", timeouts, NUM_WAITERS);

	closer.signal_and_wait().await;
}

/// Test channel saturation with rapid begin/done operations
#[tokio::test]
async fn test_watermark_channel_saturation() {
	let closer = Closer::new(1);
	let watermark = Arc::new(WaterMark::new("channel_stress".into(), closer.clone()).await);

	const NUM_TASKS: usize = 50;
	const OPS_PER_TASK: usize = 1000;

	let mut handles = vec![];

	// Spawn tasks that send sequential begin/done messages
	for task_id in 0..NUM_TASKS {
		let wm = watermark.clone();

		let handle = tokio::spawn(async move {
			// Create sequential, non-overlapping version ranges to
			// avoid ordering issues
			let base_version = (task_id * OPS_PER_TASK) as u64 + 1;

			for i in 0..OPS_PER_TASK {
				let version = base_version + i as u64;

				// Rapid fire begin/done to stress the channel
				wm.begin(CommitVersion(version));
				wm.done(CommitVersion(version));

				// Occasionally wait, but with shorter timeout
				// to avoid indefinite hangs
				if i % 100 == 0 {
					wm.wait_for_mark_timeout(CommitVersion(version), Duration::from_millis(100))
						.await;
				}
			}
		});
		handles.push(handle);
	}

	for handle in handles {
		handle.await.expect("Task panicked");
	}

	// Give processing task time to drain the channel
	sleep(Duration::from_millis(500)).await;

	// Verify progress was made
	let final_done = watermark.done_until();
	assert!(final_done > CommitVersion(0), "Should have processed versions despite channel pressure");

	closer.signal_and_wait().await;
}

/// Test resilience when processing ancient versions
#[tokio::test]
async fn test_watermark_old_version_stress() {
	let closer = Closer::new(1);
	let watermark = Arc::new(WaterMark::new("old_version_stress".into(), closer.clone()).await);

	// Advance the watermark significantly
	for i in 1..=1000 {
		watermark.begin(CommitVersion(i));
		watermark.done(CommitVersion(i));
	}

	// Wait for processing
	sleep(Duration::from_millis(100)).await;

	let done_until = watermark.done_until();
	assert!(done_until >= CommitVersion(900), "Should have processed most versions");

	// Now try operations with very old versions
	let mut handles = vec![];

	for i in 0..100 {
		let wm = watermark.clone();
		let old_version = i + 1; // Versions 1-100, all very old

		let handle = tokio::spawn(async move {
			// These should all be handled efficiently
			let start = Instant::now();
			wm.wait_for_mark(old_version).await;
			let elapsed = start.elapsed();

			// Should return very quickly for old versions
			assert!(elapsed.as_millis() < 50, "Old version should be handled quickly");
		});
		handles.push(handle);
	}

	for handle in handles {
		handle.await.expect("Task panicked");
	}

	closer.signal_and_wait().await;
}
