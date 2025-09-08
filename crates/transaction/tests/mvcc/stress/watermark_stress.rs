// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
	thread,
	time::{Duration, Instant},
};

use reifydb_transaction::mvcc::watermark::{
	Closer, MAX_PENDING, MAX_WAITERS, WaterMark,
};

/// Test watermark with many pending versions to trigger cleanup
#[test]
fn test_watermark_pending_cleanup() {
	let closer = Closer::new(1);
	let watermark = WaterMark::new("stress_pending".into(), closer.clone());

	// Create many versions without completing them initially
	// This should trigger MAX_PENDING cleanup logic
	const NUM_VERSIONS: u64 = MAX_PENDING as u64 + 500;

	// Begin many versions
	for version in 1..=NUM_VERSIONS {
		watermark.begin(version);

		// Complete some versions to allow progress
		if version % 3 == 0 {
			watermark.done(version);
		}
	}

	// Give processing thread time to handle cleanup
	thread::sleep(Duration::from_millis(500));

	// Now complete remaining versions
	for version in 1..=NUM_VERSIONS {
		if version % 3 != 0 {
			watermark.done(version);
		}
	}

	// Give processing thread time to catch up
	thread::sleep(Duration::from_millis(500));

	// Verify the system handled the load without panic
	let final_done = watermark.done_until();
	assert!(
		final_done > 0,
		"Watermark should have progressed despite high load"
	);

	closer.signal_and_wait();
}

/// Test watermark with many concurrent waiters to trigger cleanup
#[test]
fn test_watermark_waiters_cleanup() {
	let closer = Closer::new(1);
	let watermark = Arc::new(WaterMark::new(
		"stress_waiters".into(),
		closer.clone(),
	));

	const NUM_WAITERS: usize = MAX_WAITERS + 5000;

	for i in 1..=NUM_WAITERS as u64 {
		watermark.begin(i);
	}

	let mut handles = vec![];
	let timeout_count = Arc::new(AtomicUsize::new(0));

	// Spawn many threads that wait for versions
	for version in 1..=NUM_WAITERS as u64 {
		let wm = watermark.clone();
		let counter = timeout_count.clone();

		let handle = thread::spawn(move || {
			if !wm.wait_for_mark_timeout(
				version,
				Duration::from_secs(2),
			) {
				counter.fetch_add(1, Ordering::Relaxed);
			}
		});
		handles.push(handle);
	}

	// Give threads time to register as waiters
	thread::sleep(Duration::from_millis(100));

	// Now complete all versions
	for i in 1..=NUM_WAITERS as u64 {
		watermark.done(i);
	}

	for handle in handles {
		handle.join().expect("Thread panicked");
	}

	// Most waiters should have succeeded (some timeouts are acceptable)
	let timeouts = timeout_count.load(Ordering::Relaxed);
	assert!(
		timeouts < NUM_WAITERS / 10,
		"Too many timeouts: {}/{}",
		timeouts,
		NUM_WAITERS
	);

	closer.signal_and_wait();
}

/// Test channel saturation with rapid begin/done operations
#[test]
fn test_watermark_channel_saturation() {
	let closer = Closer::new(1);
	let watermark = Arc::new(WaterMark::new(
		"channel_stress".into(),
		closer.clone(),
	));

	const NUM_THREADS: usize = 50;
	const OPS_PER_THREAD: usize = 1000;

	let mut handles = vec![];

	// Spawn threads that send sequential begin/done messages
	for thread_id in 0..NUM_THREADS {
		let wm = watermark.clone();

		let handle = thread::spawn(move || {
			// Create sequential, non-overlapping version ranges to
			// avoid ordering issues
			let base_version =
				(thread_id * OPS_PER_THREAD) as u64 + 1;

			for i in 0..OPS_PER_THREAD {
				let version = base_version + i as u64;

				// Rapid fire begin/done to stress the channel
				wm.begin(version);
				wm.done(version);

				// Occasionally wait, but with shorter timeout
				// to avoid indefinite hangs
				if i % 100 == 0 {
					wm.wait_for_mark_timeout(
						version,
						Duration::from_millis(100),
					);
				}
			}
		});
		handles.push(handle);
	}

	for handle in handles {
		handle.join().expect("Thread panicked");
	}

	// Give processing thread time to drain the channel
	thread::sleep(Duration::from_millis(500));

	// Verify progress was made
	let final_done = watermark.done_until();
	assert!(
		final_done > 0,
		"Should have processed versions despite channel pressure"
	);

	closer.signal_and_wait();
}

/// Test resilience when processing ancient versions
#[test]
fn test_watermark_old_version_stress() {
	let closer = Closer::new(1);
	let watermark = Arc::new(WaterMark::new(
		"old_version_stress".into(),
		closer.clone(),
	));

	// Advance the watermark significantly
	for i in 1..=1000 {
		watermark.begin(i);
		watermark.done(i);
	}

	// Wait for processing
	thread::sleep(Duration::from_millis(100));

	let done_until = watermark.done_until();
	assert!(done_until >= 900, "Should have processed most versions");

	// Now try operations with very old versions
	let mut handles = vec![];

	for i in 0..100 {
		let wm = watermark.clone();
		let old_version = i + 1; // Versions 1-100, all very old

		let handle = thread::spawn(move || {
			// These should all be handled efficiently
			let start = Instant::now();
			wm.wait_for_mark(old_version);
			let elapsed = start.elapsed();

			// Should return very quickly for old versions
			assert!(
				elapsed.as_millis() < 50,
				"Old version should be handled quickly"
			);
		});
		handles.push(handle);
	}

	for handle in handles {
		handle.join().expect("Thread panicked");
	}

	closer.signal_and_wait();
}
