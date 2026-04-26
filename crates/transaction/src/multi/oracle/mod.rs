// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{BTreeMap, HashSet},
	sync::Arc,
};

use cleanup::cleanup_old_windows;
use reifydb_core::{
	common::CommitVersion,
	encoded::key::EncodedKey,
	interface::catalog::config::{ConfigKey, GetConfig},
	util::bloom::BloomFilter,
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::{clock::Clock, rng::Rng},
	sync::rwlock::RwLock,
};
use reifydb_type::Result;
use tracing::{Span, field, instrument};

use crate::multi::{conflict::ConflictManager, transaction::version::VersionProvider, watermark::watermark::WaterMark};

pub mod cleanup;

/// Time window containing committed transactions
pub(crate) struct CommittedWindow {
	/// All transactions committed in this window
	transactions: Vec<CommittedTxn>,
	/// Set of all keys modified in this window for quick filtering
	modified_keys: HashSet<EncodedKey>,
	/// Bloom filter for fast negative checks
	bloom: BloomFilter,
	/// Maximum version in this window
	max_version: CommitVersion,
	/// Per-window lock for fine-grained synchronization
	lock: RwLock<()>,
}

impl CommittedWindow {
	fn new(min_version: CommitVersion) -> Self {
		Self {
			transactions: Vec::with_capacity(200),
			modified_keys: HashSet::with_capacity(500),
			bloom: BloomFilter::new(500),
			max_version: min_version,
			lock: RwLock::new(()),
		}
	}

	fn add_transaction(&mut self, txn: CommittedTxn) {
		self.max_version = self.max_version.max(txn.version);

		// Add all conflict keys to our modified keys set and bloom
		// filter
		if let Some(ref conflicts) = txn.conflict_manager {
			for key in conflicts.get_write_keys() {
				self.modified_keys.insert(key.clone());
				self.bloom.add(&key);
			}
		}

		self.transactions.push(txn);
	}

	fn might_have_key(&self, key: &EncodedKey) -> bool {
		// Quick check with bloom filter first
		if !self.bloom.might_contain(key) {
			return false;
		}
		// If bloom says maybe, check the actual set
		self.modified_keys.contains(key)
	}

	pub(super) fn max_version(&self) -> CommitVersion {
		self.max_version
	}
}

/// Oracle implementation with time-window based conflict detection
pub(crate) struct OracleInner {
	/// Time windows containing committed transactions, keyed by window
	/// start version. Each window carries its own `modified_keys` set and
	/// bloom filter, which together act as the conflict-lookup index.
	/// (We previously kept a denormalized `key_to_windows` index here, but
	/// it grew unbounded with every distinct write key - dropped in favour
	/// of bloom-filter scans across the bounded `time_windows`.)
	pub time_windows: BTreeMap<CommitVersion, CommittedWindow>,

	/// Highest commit version present in any evicted window.
	/// Any transaction with read-start version < this must be aborted.
	pub evicted_up_through: CommitVersion,
}

#[derive(Debug)]
pub(crate) struct CommittedTxn {
	version: CommitVersion,
	conflict_manager: Option<ConflictManager>,
}

pub(crate) enum CreateCommitResult {
	Success(CommitVersion),
	Conflict(ConflictManager),
	TooOld,
}

/// Oracle with time-window based conflict detection
pub(crate) struct Oracle<L>
where
	L: VersionProvider,
{
	pub(crate) clock: L,
	pub(crate) inner: RwLock<OracleInner>,
	pub(crate) query: WaterMark,
	pub(crate) command: WaterMark,
	shutdown_signal: Arc<RwLock<bool>>,
	actor_system: ActorSystem,
	metrics_clock: Clock,
	rng: Rng,
	config: Arc<dyn GetConfig>,
}

impl<L> Oracle<L>
where
	L: VersionProvider,
{
	pub fn new(
		clock: L,
		actor_system: ActorSystem,
		metrics_clock: Clock,
		rng: Rng,
		config: Arc<dyn GetConfig>,
	) -> Self {
		let shutdown_signal = Arc::new(RwLock::new(false));

		Self {
			clock,
			inner: RwLock::new(OracleInner {
				time_windows: BTreeMap::new(),
				evicted_up_through: CommitVersion(0),
			}),
			query: WaterMark::new("txn-mark-query".into(), &actor_system),
			command: WaterMark::new("txn-mark-cmd".into(), &actor_system),
			shutdown_signal,
			actor_system,
			metrics_clock,
			rng,
			config,
		}
	}

	/// Return the shared configuration so callers can wire it to the catalog.
	pub fn config(&self) -> Arc<dyn GetConfig> {
		self.config.clone()
	}

	/// Get the actor system
	pub fn actor_system(&self) -> ActorSystem {
		self.actor_system.clone()
	}

	/// Get the metrics clock
	pub fn metrics_clock(&self) -> &Clock {
		&self.metrics_clock
	}

	/// Get the RNG
	pub fn rng(&self) -> &Rng {
		&self.rng
	}

	/// Efficient conflict detection using time windows and key indexing
	#[instrument(name = "transaction::oracle::new_commit", level = "debug", skip(self, done_read, conflicts), fields(
		%version,
		read_keys = field::Empty,
		write_keys = field::Empty,
		relevant_windows = field::Empty,
		windows_checked = field::Empty,
		txns_checked = field::Empty,
		inner_read_lock_us = field::Empty,
		find_windows_us = field::Empty,
		conflict_check_us = field::Empty,
		clock_next_us = field::Empty,
		inner_write_lock_us = field::Empty,
		add_txn_us = field::Empty,
		cleanup_us = field::Empty,
		has_conflict = field::Empty
	))]
	pub(crate) fn new_commit(
		&self,
		done_read: &mut bool,
		version: CommitVersion,
		conflicts: ConflictManager,
	) -> Result<CreateCommitResult> {
		// First, perform conflict detection with read lock for better
		// concurrency
		let lock_start = self.metrics_clock.instant();
		let inner = self.inner.read();
		Span::current().record("inner_read_lock_us", lock_start.elapsed().as_micros() as u64);

		if version < inner.evicted_up_through {
			return Ok(CreateCommitResult::TooOld);
		}

		// Get keys involved in this transaction for efficient filtering
		// Use references to avoid cloning
		let read_keys = conflicts.get_read_keys();
		let write_keys = conflicts.get_write_keys();
		Span::current().record("read_keys", read_keys.len());
		Span::current().record("write_keys", write_keys.len());
		let has_keys = !read_keys.is_empty() || !write_keys.is_empty();

		// Only check conflicts in windows that contain relevant keys.
		// We scan the (bounded) `time_windows` and use each window's bloom
		// filter to cheaply skip windows that can't possibly hold any of
		// our keys. Range operations bypass the bloom filter and need a
		// full window scan. The number of retained windows is bounded by
		// `OracleWaterMark`, so this is O(retained_windows * keys * bloom).
		let find_start = self.metrics_clock.instant();
		let relevant_windows: Vec<CommitVersion> = if conflicts.has_range_operations() {
			// Range operations can't be bloom-filtered; check all retained windows.
			inner.time_windows.keys().copied().collect()
		} else if !has_keys {
			// No specific keys and no range ops -> nothing can conflict.
			Vec::new()
		} else {
			inner.time_windows
				.iter()
				.filter(|(_, win)| {
					read_keys.iter().chain(write_keys.iter()).any(|k| win.might_have_key(k))
				})
				.map(|(v, _)| *v)
				.collect()
		};
		Span::current().record("find_windows_us", find_start.elapsed().as_micros() as u64);
		Span::current().record("relevant_windows", relevant_windows.len());

		// Check for conflicts only in relevant windows
		let conflict_start = self.metrics_clock.instant();
		let mut windows_checked = 0u64;
		let mut txns_checked = 0u64;
		for window_version in &relevant_windows {
			if let Some(window) = inner.time_windows.get(window_version) {
				windows_checked += 1;
				// OPTIMIZATION: Early skip if all transactions in window are older
				// than our read version - no need to acquire lock
				if window.max_version <= version {
					continue;
				}

				// Quick bloom filter check first to potentially
				// skip this window. But only if we don't
				// have range operations (which can't be bloom filtered)
				if !conflicts.has_range_operations() {
					// We need to check both:
					// 1. If any of our writes conflict with window's writes (write-write conflict)
					// 2. If any of our reads overlap with window's writes (read-write conflict)
					let needs_detailed_check = read_keys
						.iter()
						.chain(write_keys.iter())
						.any(|key| window.might_have_key(key));

					if !needs_detailed_check {
						continue;
					}
				}

				// Acquire read lock on the window for conflict
				// checking
				let _window_lock = window.lock.read();

				// Check conflicts with transactions in this
				// window
				for committed_txn in &window.transactions {
					txns_checked += 1;
					// Skip transactions that committed
					// before we started reading
					if committed_txn.version <= version {
						continue;
					}

					if let Some(old_conflicts) = &committed_txn.conflict_manager
						&& conflicts.has_conflict(old_conflicts)
					{
						Span::current().record(
							"conflict_check_us",
							conflict_start.elapsed().as_micros() as u64,
						);
						Span::current().record("windows_checked", windows_checked);
						Span::current().record("txns_checked", txns_checked);
						Span::current().record("has_conflict", true);
						return Ok(CreateCommitResult::Conflict(conflicts));
					}
				}
			}
		}
		Span::current().record("conflict_check_us", conflict_start.elapsed().as_micros() as u64);
		Span::current().record("windows_checked", windows_checked);
		Span::current().record("txns_checked", txns_checked);

		// Release read lock and acquire write lock for commit
		drop(inner);

		// No conflicts found, proceed with commit
		if !*done_read {
			self.query.done(version);
			*done_read = true;
		}

		// Get commit version - lock-free with gap-tolerant watermark
		let commit_version = {
			let clock = self.clock.clone();

			let clock_start = self.metrics_clock.instant();
			let version = clock.next()?;
			Span::current().record("clock_next_us", clock_start.elapsed().as_micros() as u64);

			// Register with watermark - can arrive out of order
			// The gap-tolerant watermark processor handles this correctly
			self.command.begin(version);

			version
		};

		// Add this transaction to the appropriate window with write lock
		let needs_cleanup = {
			let write_lock_start = self.metrics_clock.instant();
			let mut inner = self.inner.write();
			Span::current().record("inner_write_lock_us", write_lock_start.elapsed().as_micros() as u64);

			let add_start = self.metrics_clock.instant();
			let window_size = self.config.get_config_uint8(ConfigKey::OracleWindowSize);
			inner.add_committed_transaction(commit_version, conflicts, window_size);
			Span::current().record("add_txn_us", add_start.elapsed().as_micros() as u64);
			// Check if cleanup is needed
			let water_mark = self.config.get_config_uint8(ConfigKey::OracleWaterMark) as usize;
			inner.time_windows.len() > water_mark
		};

		if needs_cleanup {
			let cleanup_start = self.metrics_clock.instant();
			let safe_evict_below = self.query.done_until();
			let mut inner = self.inner.write();
			let inner = &mut *inner;
			cleanup_old_windows(&mut inner.time_windows, &mut inner.evicted_up_through, safe_evict_below);
			Span::current().record("cleanup_us", cleanup_start.elapsed().as_micros() as u64);
		}

		// DO NOT call done() here - watermark should only advance AFTER storage write completes
		// done_commit() will be called after MultiVersionCommit::commit() finishes

		Ok(CreateCommitResult::Success(commit_version))
	}

	/// Clear the conflict detection window and mark the oracle as ready.
	/// Called after bootstrap completes - bootstrap transactions committed
	/// sequentially before any concurrent access and should not participate
	/// in conflict detection.
	pub(crate) fn bootstrapping_completed(&self) {
		let mut inner = self.inner.write();
		inner.time_windows.clear();
	}

	pub(crate) fn version(&self) -> Result<CommitVersion> {
		self.clock.current()
	}

	pub fn stop(&mut self) {
		// Signal shutdown - use blocking_write since this is called from Drop
		{
			let mut shutdown = self.shutdown_signal.write();
			*shutdown = true;
		}

		// Clear accumulated window data to free memory before shutdown
		{
			let mut inner = self.inner.write();
			inner.time_windows.clear();
		}

		self.actor_system.shutdown();
	}

	/// Mark a query as done
	pub(crate) fn done_query(&self, version: CommitVersion) {
		self.query.done(version);
	}

	/// Mark a commit as done
	pub(crate) fn done_commit(&self, version: CommitVersion) {
		self.command.done(version);
	}

	/// Advance the version provider for replica replication.
	pub(crate) fn advance_version_for_replica(&self, version: CommitVersion) {
		self.clock.advance_to(version);
	}

	/// Allocate the next commit version without registering the transaction
	/// in the conflict-detection time-windows. Still rejects with `TooOld`
	/// if the caller's read-version is already past `evicted_up_through`,
	/// preserving the staleness invariant.
	///
	/// See `Engine::bulk_insert_unchecked` for the safety contract.
	pub(crate) fn advance_unchecked(
		&self,
		done_read: &mut bool,
		version: CommitVersion,
	) -> Result<CreateCommitResult> {
		// Hold the read lock across clock.next() so the TooOld check and
		// version allocation observe the same evicted_up_through. The value
		// is monotonic and we don't register a window, so the race would
		// be benign even without the lock - but holding it removes that
		// analysis from the hot path of any future reader. Unlike new_commit,
		// advance_unchecked never reacquires the write lock, so holding the
		// read lock across clock.next() is cheap.
		let inner = self.inner.read();
		if version < inner.evicted_up_through {
			return Ok(CreateCommitResult::TooOld);
		}

		if !*done_read {
			self.query.done(version);
			*done_read = true;
		}

		let commit_version = self.clock.next()?;
		self.command.begin(commit_version);
		drop(inner);

		Ok(CreateCommitResult::Success(commit_version))
	}
}

impl OracleInner {
	/// Add a committed transaction to the appropriate time window
	fn add_committed_transaction(&mut self, version: CommitVersion, conflicts: ConflictManager, window_size: u64) {
		// Determine which window this transaction belongs to
		let window_start = CommitVersion((version.0 / window_size) * window_size);

		// Get or create the window
		let window =
			self.time_windows.entry(window_start).or_insert_with(|| CommittedWindow::new(window_start));

		// Add transaction to window. The window's own `modified_keys` set
		// and bloom filter are updated by `add_transaction`, which is now
		// the sole index used by conflict-detection lookups.
		let txn = CommittedTxn {
			version,
			conflict_manager: Some(conflicts),
		};

		window.add_transaction(txn);
	}
}

impl<L> Drop for Oracle<L>
where
	L: VersionProvider,
{
	fn drop(&mut self) {
		self.stop();
	}
}

#[cfg(test)]
pub mod tests {
	use std::{
		mem::discriminant,
		sync::{
			Arc, Barrier,
			atomic::{AtomicU64, Ordering},
		},
		thread,
		thread::sleep,
		time::Duration,
	};

	use reifydb_core::encoded::key::EncodedKeyRange;
	use reifydb_runtime::{context::clock::MockClock, pool::Pools};
	use reifydb_type::value::Value;

	use super::*;
	use crate::multi::transaction::version::VersionProvider;

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

	impl VersionProvider for MockVersionProvider {
		fn next(&self) -> Result<CommitVersion> {
			Ok(CommitVersion(self.current.fetch_add(1, Ordering::Relaxed) + 1))
		}

		fn current(&self) -> Result<CommitVersion> {
			Ok(CommitVersion(self.current.load(Ordering::Relaxed)))
		}

		fn advance_to(&self, version: CommitVersion) {
			self.current.fetch_max(version.0, Ordering::Relaxed);
		}
	}

	fn create_test_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes().to_vec())
	}

	fn create_test_oracle(start: impl Into<CommitVersion>) -> Oracle<MockVersionProvider> {
		let clock = MockVersionProvider::new(start);
		let actor_system = ActorSystem::new(Pools::default(), Clock::Real);

		struct DummyConfig;
		impl GetConfig for DummyConfig {
			fn get_config(&self, key: ConfigKey) -> Value {
				key.default_value()
			}
			fn get_config_at(&self, key: ConfigKey, _version: CommitVersion) -> Value {
				key.default_value()
			}
		}
		let config = Arc::new(DummyConfig);

		Oracle::new(clock, actor_system, Clock::Mock(MockClock::from_millis(1000)), Rng::seeded(42), config)
	}

	#[test]
	fn test_oracle_basic_creation() {
		let oracle = create_test_oracle(0);

		// Oracle should be created successfully
		assert_eq!(oracle.version().unwrap(), 0);
	}

	#[test]
	fn test_window_creation_and_indexing() {
		let oracle = create_test_oracle(0);

		// Create a conflict manager with some keys
		let mut conflicts = ConflictManager::new();
		let key1 = create_test_key("key1");
		let key2 = create_test_key("key2");
		conflicts.mark_write(&key1);
		conflicts.mark_write(&key2);

		// Simulate committing a transaction
		let mut done_read = false;
		let result = oracle.new_commit(&mut done_read, CommitVersion(1), conflicts).unwrap();

		match result {
			CreateCommitResult::Success(version) => {
				assert!(version.0 >= 1); // Should get a new version

				// Check that the keys ended up in the window's modified_keys index
				let inner = oracle.inner.read();
				assert!(inner.time_windows.len() > 0);
				let any_window_has_key1 =
					inner.time_windows.values().any(|w| w.modified_keys.contains(&key1));
				let any_window_has_key2 =
					inner.time_windows.values().any(|w| w.modified_keys.contains(&key2));
				assert!(any_window_has_key1);
				assert!(any_window_has_key2);
			}
			CreateCommitResult::Conflict(_) => panic!("Unexpected conflict for first transaction"),
			CreateCommitResult::TooOld => panic!("Unexpected TooOld for first transaction"),
		}
	}

	#[test]
	fn test_conflict_detection_between_transactions() {
		let oracle = create_test_oracle(1);

		let shared_key = create_test_key("shared_key");

		// First transaction: reads and writes shared_key, starts
		// reading at version 1
		let mut conflicts1 = ConflictManager::new();
		conflicts1.mark_read(&shared_key);
		conflicts1.mark_write(&shared_key);

		let mut done_read1 = false;
		let result1 = oracle.new_commit(&mut done_read1, CommitVersion(1), conflicts1).unwrap();
		let _commit_v1 = match result1 {
			CreateCommitResult::Success(v) => v, // This should be version 2
			_ => panic!("First transaction should succeed"),
		};

		// Second transaction: reads shared_key and writes to it (should
		// conflict) Started reading at version 1 (before txn1
		// committed)
		let mut conflicts2 = ConflictManager::new();
		conflicts2.mark_read(&shared_key);
		conflicts2.mark_write(&shared_key);

		let mut done_read2 = false;
		// txn2 also started reading at version 1, but txn1 committed at
		// version 2 So txn2 should see the conflict
		let result2 = oracle.new_commit(&mut done_read2, CommitVersion(1), conflicts2).unwrap();

		// Should detect conflict because txn2 read shared_key which
		// txn1 wrote to
		assert!(matches!(result2, CreateCommitResult::Conflict(_)));
	}

	#[test]
	fn test_no_conflict_different_keys() {
		let oracle = create_test_oracle(0);

		let key1 = create_test_key("key1");
		let key2 = create_test_key("key2");

		// First transaction: reads and writes key1
		let mut conflicts1 = ConflictManager::new();
		conflicts1.mark_read(&key1);
		conflicts1.mark_write(&key1);

		let mut done_read1 = false;
		let result1 = oracle.new_commit(&mut done_read1, CommitVersion(1), conflicts1).unwrap();
		assert!(matches!(result1, CreateCommitResult::Success(_)));

		// Second transaction: reads and writes key2 (different key, no
		// conflict)
		let mut conflicts2 = ConflictManager::new();
		conflicts2.mark_read(&key2);
		conflicts2.mark_write(&key2);

		let mut done_read2 = false;
		let result2 = oracle.new_commit(&mut done_read2, CommitVersion(1), conflicts2).unwrap();

		// Should succeed because different keys
		assert!(matches!(result2, CreateCommitResult::Success(_)));
	}

	#[test]
	fn test_key_indexing_multiple_windows() {
		let oracle = create_test_oracle(0);

		let key1 = create_test_key("key1");
		let key2 = create_test_key("key2");

		// Add transactions to different windows by using different
		// version ranges
		for i in 0..3 {
			let mut conflicts = ConflictManager::new();
			if i % 2 == 0 {
				conflicts.mark_write(&key1);
			} else {
				conflicts.mark_write(&key2);
			}

			let mut done_read = false;
			let version_start = CommitVersion(i as u64 * 500 + 1);
			let result = oracle.new_commit(&mut done_read, version_start, conflicts).unwrap();
			assert!(matches!(result, CreateCommitResult::Success(_)));
		}

		// Check key indexing across multiple windows via the per-window
		// `modified_keys` set (the source of truth).
		let inner = oracle.inner.read();

		let key1_window_count = inner.time_windows.values().filter(|w| w.modified_keys.contains(&key1)).count();
		assert!(key1_window_count >= 1);

		let key2_window_count = inner.time_windows.values().filter(|w| w.modified_keys.contains(&key2)).count();
		assert!(key2_window_count >= 1);
	}

	#[test]
	fn test_version_filtering_in_conflict_detection() {
		let oracle = create_test_oracle(2);

		let shared_key = create_test_key("shared_key");

		// First transaction at version 5
		let mut conflicts1 = ConflictManager::new();
		conflicts1.mark_write(&shared_key);

		let mut done_read1 = false;
		let result1 = oracle.new_commit(&mut done_read1, CommitVersion(5), conflicts1).unwrap();
		let commit_v1 = match result1 {
			CreateCommitResult::Success(v) => v,
			_ => panic!("First transaction should succeed"),
		};

		// Second transaction that started BEFORE the first committed
		// (version 3) Should NOT conflict because txn1 committed
		// after txn2 started reading
		let mut conflicts2 = ConflictManager::new();
		conflicts2.mark_read(&shared_key);
		conflicts2.mark_write(&shared_key);

		let mut done_read2 = false;
		let result2 = oracle.new_commit(&mut done_read2, CommitVersion(3), conflicts2).unwrap();
		assert!(matches!(result2, CreateCommitResult::Success(_)));

		// Third transaction that started BEFORE the first committed
		// Should conflict because txn1 wrote to shared_key after txn3
		// started reading
		let mut conflicts3 = ConflictManager::new();
		conflicts3.mark_read(&shared_key);
		conflicts3.mark_write(&shared_key);

		let mut done_read3 = false;
		let read_version = CommitVersion(commit_v1.0 - 1); // Started reading before txn1 committed
		let result3 = oracle.new_commit(&mut done_read3, read_version, conflicts3).unwrap();
		assert!(matches!(result3, CreateCommitResult::Conflict(_)));
	}

	#[test]
	fn test_range_operations_fallback() {
		let oracle = create_test_oracle(1);

		let key1 = create_test_key("key1");

		// First transaction: writes to a specific key
		let mut conflicts1 = ConflictManager::new();
		conflicts1.mark_write(&key1);

		let mut done_read1 = false;
		let result1 = oracle.new_commit(&mut done_read1, CommitVersion(1), conflicts1).unwrap();
		assert!(matches!(result1, CreateCommitResult::Success(_)));

		// Second transaction: does a range operation (which can't be
		// indexed by specific keys)
		let mut conflicts2 = ConflictManager::new();
		// Simulate a range read that doesn't return specific keys
		let range = EncodedKeyRange::parse("a..z");
		conflicts2.mark_range(range);
		conflicts2.mark_write(&create_test_key("other_key"));

		// This should use the fallback mechanism to check all windows
		let mut done_read2 = false;
		let result2 = oracle.new_commit(&mut done_read2, CommitVersion(1), conflicts2).unwrap();

		// Should detect conflict due to the range overlap with key1
		assert!(matches!(result2, CreateCommitResult::Conflict(_)));
	}

	/// Regression: range-only reads must scan windows whose `window_start`
	/// is less than `read_version` but whose contents include transactions
	/// with `commit_version > read_version`. The `!has_keys` branch in
	/// `new_commit` (oracle/mod.rs:225-228) limits its scan to
	/// `time_windows.range(version..).take(5)`, which skips such windows.
	#[test]
	fn test_range_only_read_finds_conflict_in_older_window() {
		// Default OracleWindowSize = 500. Start clock at 749 so T1 commits at 750
		// and lands in the window keyed by window_start = 500.
		let oracle = create_test_oracle(749);

		let key_k = create_test_key("k");

		// T1: write to "k". Read version irrelevant; commit version will be 750.
		let mut conflicts1 = ConflictManager::new();
		conflicts1.mark_write(&key_k);
		let mut done_read1 = false;
		let r1 = oracle.new_commit(&mut done_read1, CommitVersion(1), conflicts1).unwrap();
		let commit_v1 = match r1 {
			CreateCommitResult::Success(v) => v,
			_ => panic!("T1 should commit"),
		};
		assert_eq!(commit_v1, CommitVersion(750));

		// Sanity: T1 lives in the window keyed at 500, so any read_version in
		// (500, 750) exercises the bug. If defaults change, this assertion
		// fires before the silent-pass below masks a real regression.
		{
			let inner = oracle.inner.read();
			assert!(
				inner.time_windows.contains_key(&CommitVersion(500)),
				"expected T1's window_start to be 500 (default OracleWindowSize=500); \
				 test assumptions invalidated"
			);
		}

		// T2: range-only read covering "k". No read_keys, no write_keys, so
		// has_keys = false. read_version = 510 is inside window [500, 1000)
		// but greater than the window's start. T1's commit version (750) is
		// greater than T2's read version, so T2 must see the conflict.
		let mut conflicts2 = ConflictManager::new();
		conflicts2.mark_range(EncodedKeyRange::parse("a..z"));
		let mut done_read2 = false;
		let r2 = oracle.new_commit(&mut done_read2, CommitVersion(510), conflicts2).unwrap();

		assert!(
			matches!(r2, CreateCommitResult::Conflict(_)),
			"T2's range read of 'k' must conflict with T1's write at version 750 > 510, \
			 but the !has_keys branch in oracle/mod.rs:225 skips windows whose \
			 window_start < read_version"
		);
	}

	/// Regression: a transaction that has both specific keys AND a range op
	/// must scan every retained window, not just the windows the bloom filter
	/// matched on its specific keys. Otherwise a range conflict in a window
	/// whose bloom does not contain any of our specific keys is silently missed.
	#[test]
	fn test_range_op_with_keys_scans_all_windows_not_just_bloom_matches() {
		// Default OracleWindowSize = 500. Place T_b at v=50 (window @ 0)
		// and T_a at v=750 (window @ 500) so the two are in different windows.
		let oracle = create_test_oracle(49);

		let key_alpha = create_test_key("alpha");
		let key_beta = create_test_key("beta");

		// T_b: writes "beta". Lands in window @ 0. Bloom for window 0
		// contains "beta" but not "alpha".
		let mut conflicts_b = ConflictManager::new();
		conflicts_b.mark_write(&key_beta);
		let mut done_read_b = false;
		let r_b = oracle.new_commit(&mut done_read_b, CommitVersion(1), conflicts_b).unwrap();
		let commit_v_b = match r_b {
			CreateCommitResult::Success(v) => v,
			_ => panic!("T_b should commit"),
		};
		assert_eq!(commit_v_b, CommitVersion(50));

		// Skip the clock forward so T_a lands in window @ 500.
		oracle.advance_version_for_replica(CommitVersion(749));

		// T_a: writes "alpha". Lands in window @ 500. Bloom for window 500
		// contains "alpha" but not "beta".
		let mut conflicts_a = ConflictManager::new();
		conflicts_a.mark_write(&key_alpha);
		let mut done_read_a = false;
		let r_a = oracle.new_commit(&mut done_read_a, CommitVersion(1), conflicts_a).unwrap();
		let commit_v_a = match r_a {
			CreateCommitResult::Success(v) => v,
			_ => panic!("T_a should commit"),
		};
		assert_eq!(commit_v_a, CommitVersion(750));

		// Sanity: both windows exist as expected. If OracleWindowSize defaults
		// change, this fires before the conflict assertion silently masks a regression.
		{
			let inner = oracle.inner.read();
			assert!(
				inner.time_windows.contains_key(&CommitVersion(0)),
				"expected T_b's window_start to be 0 (default OracleWindowSize=500); \
				 test assumptions invalidated"
			);
			assert!(
				inner.time_windows.contains_key(&CommitVersion(500)),
				"expected T_a's window_start to be 500 (default OracleWindowSize=500); \
				 test assumptions invalidated"
			);
		}

		// T3: writes "beta" (so has_keys=true; bloom-only path matches window @ 0
		// only) AND reads range "a..z" (which overlaps "alpha" in window @ 500).
		// read_version=100 sits between T_b (v=50) and T_a (v=750), so:
		//   - In window @ 0, max_version=50 <= 100, the early-skip fires before the per-txn loop.
		//   - Window @ 500 is the only place where the conflict is visible (T_a's "alpha" is in T3's range and
		//     v=750 > 100).
		// New code: has_range_operations() => scan all windows => finds conflict.
		// Old code: bloom-only relevant_windows=[0] when has_keys=true => never visits
		// window @ 500 => silently misses the range conflict.
		let mut conflicts_3 = ConflictManager::new();
		conflicts_3.mark_write(&key_beta);
		conflicts_3.mark_range(EncodedKeyRange::parse("a..z"));
		let mut done_read_3 = false;
		let r_3 = oracle.new_commit(&mut done_read_3, CommitVersion(100), conflicts_3).unwrap();

		assert!(
			matches!(r_3, CreateCommitResult::Conflict(_)),
			"T3's range 'a..z' overlaps T_a's write of 'alpha' (v=750 > 100), \
			 but T3's specific write key 'beta' only bloom-matches window @ 0. \
			 Range ops must force a scan of all retained windows, including window @ 500."
		);
	}

	#[test]
	fn test_empty_conflict_manager() {
		let oracle = create_test_oracle(0);

		// Transaction with no conflicts (read-only)
		let conflicts = ConflictManager::new(); // Empty conflict manager

		let mut done_read = false;
		let result = oracle.new_commit(&mut done_read, CommitVersion(1), conflicts).unwrap();

		// Should succeed; no write keys means the window's modified_keys
		// stays empty.
		match result {
			CreateCommitResult::Success(_) => {
				let inner = oracle.inner.read();
				let total_modified: usize =
					inner.time_windows.values().map(|w| w.modified_keys.len()).sum();
				assert_eq!(total_modified, 0);
			}
			CreateCommitResult::Conflict(_) => {
				panic!("Empty conflict manager should not cause conflicts")
			}
			CreateCommitResult::TooOld => panic!("Unexpected TooOld for empty conflict manager"),
		}
	}

	#[test]
	fn test_write_write_conflict() {
		let oracle = create_test_oracle(1);

		let shared_key = create_test_key("shared_key");

		// First transaction: writes to shared_key (no read)
		let mut conflicts1 = ConflictManager::new();
		conflicts1.mark_write(&shared_key);

		let mut done_read1 = false;
		let result1 = oracle.new_commit(&mut done_read1, CommitVersion(1), conflicts1).unwrap();
		assert!(matches!(result1, CreateCommitResult::Success(_)));

		// Second transaction: also writes to shared_key (write-write
		// conflict)
		let mut conflicts2 = ConflictManager::new();
		conflicts2.mark_write(&shared_key);

		let mut done_read2 = false;
		let result2 = oracle.new_commit(&mut done_read2, CommitVersion(1), conflicts2).unwrap();

		// Should detect conflict because both transactions write to the
		// same key
		assert!(matches!(result2, CreateCommitResult::Conflict(_)));
	}

	#[test]
	fn test_read_write_conflict() {
		let oracle = create_test_oracle(1);

		let shared_key = create_test_key("shared_key");

		// First transaction: writes to shared_key
		let mut conflicts1 = ConflictManager::new();
		conflicts1.mark_write(&shared_key);

		let mut done_read1 = false;
		let result1 = oracle.new_commit(&mut done_read1, CommitVersion(1), conflicts1).unwrap();
		assert!(matches!(result1, CreateCommitResult::Success(_)));

		// Second transaction: reads from shared_key (read-write
		// conflict)
		let mut conflicts2 = ConflictManager::new();
		conflicts2.mark_read(&shared_key);

		let mut done_read2 = false;
		let result2 = oracle.new_commit(&mut done_read2, CommitVersion(1), conflicts2).unwrap();

		// Should detect conflict because txn2 read from key that txn1
		// wrote to
		assert!(matches!(result2, CreateCommitResult::Conflict(_)));
	}

	#[test]
	fn test_sequential_transactions_no_conflict() {
		let oracle = create_test_oracle(0);

		let shared_key = create_test_key("shared_key");

		// First transaction
		let mut conflicts1 = ConflictManager::new();
		conflicts1.mark_read(&shared_key);
		conflicts1.mark_write(&shared_key);

		let mut done_read1 = false;
		let result1 = oracle.new_commit(&mut done_read1, CommitVersion(1), conflicts1).unwrap();
		let commit_v1 = match result1 {
			CreateCommitResult::Success(v) => v,
			_ => panic!("First transaction should succeed"),
		};

		// Second transaction starts AFTER first transaction committed
		let mut conflicts2 = ConflictManager::new();
		conflicts2.mark_read(&shared_key);
		conflicts2.mark_write(&shared_key);

		let mut done_read2 = false;
		let read_version = CommitVersion(commit_v1.0 + 1); // Started after first committed
		let result2 = oracle.new_commit(&mut done_read2, read_version, conflicts2).unwrap();

		// Should NOT conflict because they don't overlap in time
		assert!(matches!(result2, CreateCommitResult::Success(_)));
	}

	#[test]
	fn test_comptokenize_multi_key_scenario() {
		let oracle = create_test_oracle(1);

		let key_a = create_test_key("key_a");
		let key_b = create_test_key("key_b");
		let key_c = create_test_key("key_c");

		// Transaction 1: reads A, writes B
		let mut conflicts1 = ConflictManager::new();
		conflicts1.mark_read(&key_a);
		conflicts1.mark_write(&key_b);

		let mut done_read1 = false;
		let result1 = oracle.new_commit(&mut done_read1, CommitVersion(1), conflicts1).unwrap();
		assert!(matches!(result1, CreateCommitResult::Success(_)));

		// Transaction 2: reads B, writes C (should conflict because
		// txn1 wrote B)
		let mut conflicts2 = ConflictManager::new();
		conflicts2.mark_read(&key_b);
		conflicts2.mark_write(&key_c);

		let mut done_read2 = false;
		let result2 = oracle.new_commit(&mut done_read2, CommitVersion(1), conflicts2).unwrap();
		assert!(matches!(result2, CreateCommitResult::Conflict(_)));

		// Transaction 3: reads C, writes A (should not conflict)
		let mut conflicts3 = ConflictManager::new();
		conflicts3.mark_read(&key_c);
		conflicts3.mark_write(&key_a);

		let mut done_read3 = false;
		let result3 = oracle.new_commit(&mut done_read3, CommitVersion(1), conflicts3).unwrap();
		assert!(matches!(result3, CreateCommitResult::Success(_)));
	}

	/// Regression test for watermark ordering race condition.
	///
	/// This test verifies that concurrent commits don't cause the watermark
	/// to skip versions. The fix ensures `begin(version)` is called inside
	/// the version_lock, guaranteeing versions are registered in order.
	#[test]
	fn test_concurrent_commits_dont_skip_watermark_versions() {
		const NUM_CONCURRENT: usize = 100;
		const ITERATIONS: usize = 10;

		for iteration in 0..ITERATIONS {
			// Create fresh oracle for each iteration to avoid conflicts
			let oracle = Arc::new(create_test_oracle(0));
			let mut handles = vec![];

			for i in 0..NUM_CONCURRENT {
				let oracle_clone = oracle.clone();
				// Use unique keys per iteration to avoid conflicts
				let key = create_test_key(&format!("key_{}_{}", iteration, i));

				let handle = thread::spawn(move || {
					let mut conflicts = ConflictManager::new();
					conflicts.mark_write(&key);

					let mut done_read = false;
					let result = oracle_clone
						.new_commit(&mut done_read, CommitVersion(1), conflicts)
						.unwrap();

					match result {
						CreateCommitResult::Success(version) => {
							// Simulate storage write with variable delay
							if i % 3 == 0 {
								sleep(Duration::from_micros(100));
							}
							// Mark commit as done
							oracle_clone.done_commit(version);
							Some(version)
						}
						CreateCommitResult::Conflict(_) => None,
						CreateCommitResult::TooOld => None,
					}
				});
				handles.push(handle);
			}

			// Wait for all commits
			let mut max_version = CommitVersion(0);
			let mut success_count = 0;
			for handle in handles {
				if let Some(v) = handle.join().unwrap() {
					max_version = max_version.max(v);
					success_count += 1;
				}
			}

			// All should succeed since keys are unique
			assert_eq!(
				success_count, NUM_CONCURRENT,
				"Expected {} successful commits, got {}",
				NUM_CONCURRENT, success_count
			);

			// Give watermark processor time to catch up
			sleep(Duration::from_millis(100));

			// KEY ASSERTION: The watermark should have advanced to max_version
			// If any version was skipped due to the race condition, done_until
			// would be less than max_version (stuck at the skipped version - 1)
			let done_until = oracle.command.done_until();
			assert_eq!(
				done_until, max_version,
				"Watermark race condition detected! done_until={} but max_version={}. \
				 Some version was skipped.",
				done_until.0, max_version.0
			);
		}
	}

	/// Test that verifies versions are registered with watermark in order
	#[test]
	fn test_version_begin_ordering() {
		let oracle = Arc::new(create_test_oracle(0));
		let barrier = Arc::new(Barrier::new(10));

		let mut handles = vec![];

		// Spawn 10 concurrent commits that all start at the same time
		for i in 0..10 {
			let oracle_clone = oracle.clone();
			let barrier_clone = barrier.clone();
			let key = create_test_key(&format!("order_key_{}", i));

			let handle = thread::spawn(move || {
				// Wait for all tasks to be ready
				barrier_clone.wait();

				let mut conflicts = ConflictManager::new();
				conflicts.mark_write(&key);

				let mut done_read = false;
				let result =
					oracle_clone.new_commit(&mut done_read, CommitVersion(1), conflicts).unwrap();

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
			let v = handle.join().unwrap();
			if v.0 > 0 {
				versions.push(v.0);
			}
		}

		// Give watermark time to process
		sleep(Duration::from_millis(50));

		// All versions should be contiguous (no gaps)
		versions.sort();
		for i in 1..versions.len() {
			assert_eq!(
				versions[i],
				versions[i - 1] + 1,
				"Version gap detected: {} -> {}. Versions should be contiguous.",
				versions[i - 1],
				versions[i]
			);
		}

		// Watermark should be at the highest version
		let done_until = oracle.command.done_until();
		assert_eq!(
			done_until.0,
			*versions.last().unwrap_or(&0),
			"Watermark should be at highest committed version"
		);
	}

	#[test]
	fn test_disabled_then_new_commit_skips_conflict_registration() {
		// Start the clock at 1 so T1's commit version is 2; T2 then reads
		// at version 1 (strictly before T1) and the per-window check
		// `committed_txn.version <= read_version` will not short-circuit.
		let oracle = create_test_oracle(1);
		let key = create_test_key("shared");

		// T1: disable() + rollback() must restore a usable manager.
		// Subsequent mark_write must be recorded (the fix), not silently
		// dropped (the bug).
		let mut cm1 = ConflictManager::new();
		cm1.disable();
		cm1.rollback();
		cm1.mark_write(&key);
		assert!(
			cm1.get_write_keys().contains(&key),
			"rollback must reset ConflictManager.disabled; otherwise the reused \
			 manager would silently drop mark_write and the oracle would register \
			 an empty window for this transaction"
		);

		let mut done_read1 = false;
		let v1 = match oracle.new_commit(&mut done_read1, CommitVersion(1), cm1).unwrap() {
			CreateCommitResult::Success(v) => v,
			other => panic!("T1 should commit, got variant {:?}", discriminant(&other)),
		};
		assert!(v1.0 >= 2, "T1's commit version should be at least 2, got {}", v1.0);

		// T2: reads at v=1 (strictly before T1's commit) and writes the same
		// key. SSI must report a conflict against T1's write of `shared`.
		// With the bug, T1's window had empty modified_keys and T2 would
		// have been silently allowed through.
		let mut cm2 = ConflictManager::new();
		cm2.mark_read(&key);
		cm2.mark_write(&key);
		let mut done_read2 = false;
		let r2 = oracle.new_commit(&mut done_read2, CommitVersion(1), cm2).unwrap();

		assert!(
			matches!(r2, CreateCommitResult::Conflict(_)),
			"T2's read+write of `shared` (read_version=1) must conflict with T1's \
			 write at v={}",
			v1.0
		);
	}
}
