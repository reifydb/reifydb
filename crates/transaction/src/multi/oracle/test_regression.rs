// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Regression tests for the oracle's watermark race condition.

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use reifydb_core::{CommitVersion, EncodedKey};
use tokio::time::{Duration, sleep};

use super::{CreateCommitResult, Oracle, testing::*};
use crate::multi::{conflict::ConflictManager, transaction::version::VersionProvider};

// Mock version provider for testing
#[derive(Debug, Clone)]
struct MockVersionProvider {
	current: Arc<AtomicU64>,
}

impl MockVersionProvider {
	fn new(start: impl Into<CommitVersion>) -> Self {
		Self {
			current: Arc::new(AtomicU64::new(start.into().0)),
		}
	}
}

#[async_trait]
impl VersionProvider for MockVersionProvider {
	async fn next(&self) -> crate::Result<CommitVersion> {
		Ok(CommitVersion(self.current.fetch_add(1, Ordering::Relaxed) + 1))
	}

	async fn current(&self) -> crate::Result<CommitVersion> {
		Ok(CommitVersion(self.current.load(Ordering::Relaxed)))
	}
}

fn create_test_key(s: &str) -> EncodedKey {
	EncodedKey::new(s.as_bytes().to_vec())
}

/// Regression test for watermark race condition using the test hook.
///
/// This test uses yield_hook() to force task interleaving between version
/// allocation and begin(). With the fix applied (begin inside lock), the
/// hook runs inside the lock so other tasks are blocked. With the fix
/// reverted, the hook would allow interleaving, causing the race.
///
/// # How this catches the bug
///
/// When `begin(version)` is inside `version_lock`:
/// - Task A: acquires lock, gets version N, runs hook (yields), calls begin(N), releases lock
/// - Task B: blocked on lock until A releases
/// - Versions registered in order: begin(N) before begin(N+1)
/// - Watermark advances correctly
///
/// When `begin(version)` is outside `version_lock` (BUG):
/// - Task A: acquires lock, gets version N, releases lock, runs hook (yields)
/// - Task B: acquires lock, gets version N+1, releases lock, calls begin(N+1), done(N+1)
/// - Task A: finally calls begin(N)
/// - Watermark sees done(N+1) before begin(N), skips version N
/// - Test fails: done_until < max_version
#[tokio::test]
async fn test_watermark_race_with_yield_hook() {
	const NUM_CONCURRENT: usize = 50;
	const ITERATIONS: usize = 5;

	for iteration in 0..ITERATIONS {
		let clock = MockVersionProvider::new(0);
		let oracle = Arc::new(Oracle::<_>::new(clock).await);

		// Install hook that yields between version allocation and begin()
		let _guard = set_oracle_test_hook(yield_hook()).await;

		let mut handles = vec![];

		for i in 0..NUM_CONCURRENT {
			let oracle_clone = oracle.clone();
			let key = create_test_key(&format!("hook_key_{}_{}", iteration, i));

			let handle = tokio::spawn(async move {
				let mut conflicts = ConflictManager::new();
				conflicts.mark_write(&key);

				let mut done_read = false;
				let result = oracle_clone
					.new_commit(&mut done_read, CommitVersion(1), conflicts)
					.await
					.unwrap();

				match result {
					CreateCommitResult::Success(version) => {
						// Variable delay before done_commit to stress watermark
						if i % 3 == 0 {
							sleep(Duration::from_micros(50)).await;
						}
						oracle_clone.done_commit(version);
						Some(version)
					}
					CreateCommitResult::Conflict(_) => None,
				}
			});
			handles.push(handle);
		}

		let mut max_version = CommitVersion(0);
		let mut success_count = 0;
		for handle in handles {
			if let Some(v) = handle.await.unwrap() {
				max_version = max_version.max(v);
				success_count += 1;
			}
		}

		assert_eq!(success_count, NUM_CONCURRENT, "All commits should succeed with unique keys");

		// Give watermark time to process all done() calls
		sleep(Duration::from_millis(100)).await;

		// KEY ASSERTION: Watermark must reach max_version
		// If any version was skipped due to race, watermark gets stuck
		let done_until = oracle.command.done_until();
		assert_eq!(
			done_until, max_version,
			"Iteration {}: Watermark race detected! done_until={} but max_version={}. \
			 The hook exposed out-of-order version registration.",
			iteration, done_until.0, max_version.0
		);
	}
}

/// Test with barrier synchronization to maximize contention.
///
/// Uses a barrier to ensure all tasks reach the commit point simultaneously,
/// then the yield hook forces interleaving.
#[tokio::test]
async fn test_watermark_race_with_barrier_and_hook() {
	use tokio::sync::Barrier;

	const NUM_CONCURRENT: usize = 20;

	let clock = MockVersionProvider::new(0);
	let oracle = Arc::new(Oracle::<_>::new(clock).await);
	let barrier = Arc::new(Barrier::new(NUM_CONCURRENT));

	// Install yield hook
	let _guard = set_oracle_test_hook(yield_hook()).await;

	let mut handles = vec![];

	for i in 0..NUM_CONCURRENT {
		let oracle_clone = oracle.clone();
		let barrier_clone = barrier.clone();
		let key = create_test_key(&format!("barrier_key_{}", i));

		let handle = tokio::spawn(async move {
			// Synchronize all tasks
			barrier_clone.wait().await;

			let mut conflicts = ConflictManager::new();
			conflicts.mark_write(&key);

			let mut done_read = false;
			let result =
				oracle_clone.new_commit(&mut done_read, CommitVersion(1), conflicts).await.unwrap();

			if let CreateCommitResult::Success(version) = result {
				oracle_clone.done_commit(version);
				version
			} else {
				CommitVersion(0)
			}
		});
		handles.push(handle);
	}

	let mut versions: Vec<u64> = vec![];
	for handle in handles {
		let v = handle.await.unwrap();
		if v.0 > 0 {
			versions.push(v.0);
		}
	}

	// Give watermark time to process
	sleep(Duration::from_millis(100)).await;

	// Verify all versions are contiguous (no gaps)
	versions.sort();
	for i in 1..versions.len() {
		assert_eq!(
			versions[i],
			versions[i - 1] + 1,
			"Version gap: {} -> {}. With the fix, versions should be contiguous.",
			versions[i - 1],
			versions[i]
		);
	}

	// Verify watermark reached the highest version
	let done_until = oracle.command.done_until();
	let max_version = *versions.last().unwrap_or(&0);
	assert_eq!(done_until.0, max_version, "Watermark should be at highest version");
}
