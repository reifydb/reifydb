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

pub(crate) struct CommittedWindow {
	transactions: Vec<CommittedTxn>,
	modified_keys: HashSet<EncodedKey>,
	bloom: BloomFilter,
	max_version: CommitVersion,
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

		if let Some(ref conflicts) = txn.conflict_manager {
			for key in conflicts.get_write_keys() {
				self.modified_keys.insert(key.clone());
				self.bloom.add(&key);
			}
		}

		self.transactions.push(txn);
	}

	fn might_have_key(&self, key: &EncodedKey) -> bool {
		if !self.bloom.might_contain(key) {
			return false;
		}
		self.modified_keys.contains(key)
	}

	pub(super) fn max_version(&self) -> CommitVersion {
		self.max_version
	}
}

pub(crate) struct OracleState {
	pub time_windows: BTreeMap<CommitVersion, CommittedWindow>,

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

pub(crate) struct Oracle<L>
where
	L: VersionProvider,
{
	pub(crate) clock: L,
	pub(crate) inner: RwLock<OracleState>,
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
			inner: RwLock::new(OracleState {
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

	pub fn config(&self) -> Arc<dyn GetConfig> {
		self.config.clone()
	}

	pub fn actor_system(&self) -> ActorSystem {
		self.actor_system.clone()
	}

	pub fn metrics_clock(&self) -> &Clock {
		&self.metrics_clock
	}

	pub fn rng(&self) -> &Rng {
		&self.rng
	}

	#[instrument(name = "transaction::oracle::new_commit", level = "debug", skip(self, conflicts), fields(
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
		version: CommitVersion,
		conflicts: ConflictManager,
	) -> Result<CreateCommitResult> {
		let lock_start = self.metrics_clock.instant();
		let inner = self.inner.read();
		Span::current().record("inner_read_lock_us", lock_start.elapsed().as_micros() as u64);

		if let Some(early) = self.check_too_old(&inner, version) {
			return Ok(early);
		}

		if self.detect_conflicts(&inner, version, &conflicts) {
			return Ok(CreateCommitResult::Conflict(conflicts));
		}

		drop(inner);

		let commit_version = self.allocate_commit_version()?;
		let needs_cleanup = self.register_committed(commit_version, conflicts);
		if needs_cleanup {
			self.cleanup_old_windows();
		}

		Ok(CreateCommitResult::Success(commit_version))
	}

	#[inline]
	fn check_too_old(&self, inner: &OracleState, version: CommitVersion) -> Option<CreateCommitResult> {
		if version < inner.evicted_up_through {
			Some(CreateCommitResult::TooOld)
		} else {
			None
		}
	}

	fn detect_conflicts(&self, inner: &OracleState, version: CommitVersion, conflicts: &ConflictManager) -> bool {
		let read_keys = conflicts.get_read_keys();
		let write_keys = conflicts.get_write_keys();
		Span::current().record("read_keys", read_keys.len());
		Span::current().record("write_keys", write_keys.len());
		let has_keys = !read_keys.is_empty() || !write_keys.is_empty();

		let find_start = self.metrics_clock.instant();
		let relevant_windows: Vec<CommitVersion> = if conflicts.has_range_operations() {
			inner.time_windows.keys().copied().collect()
		} else if !has_keys {
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

		let conflict_start = self.metrics_clock.instant();
		let mut windows_checked = 0u64;
		let mut txns_checked = 0u64;
		for window_version in &relevant_windows {
			if let Some(window) = inner.time_windows.get(window_version) {
				windows_checked += 1;
				if window.max_version <= version {
					continue;
				}

				if !conflicts.has_range_operations() {
					let needs_detailed_check = read_keys
						.iter()
						.chain(write_keys.iter())
						.any(|key| window.might_have_key(key));

					if !needs_detailed_check {
						continue;
					}
				}

				let _window_lock = window.lock.read();

				for committed_txn in &window.transactions {
					txns_checked += 1;
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
						return true;
					}
				}
			}
		}
		Span::current().record("conflict_check_us", conflict_start.elapsed().as_micros() as u64);
		Span::current().record("windows_checked", windows_checked);
		Span::current().record("txns_checked", txns_checked);
		false
	}

	#[inline]
	fn allocate_commit_version(&self) -> Result<CommitVersion> {
		let clock = self.clock.clone();
		let clock_start = self.metrics_clock.instant();
		let commit_version = clock.next()?;
		Span::current().record("clock_next_us", clock_start.elapsed().as_micros() as u64);

		self.command.register_in_flight(commit_version);
		Ok(commit_version)
	}

	#[inline]
	fn register_committed(&self, commit_version: CommitVersion, conflicts: ConflictManager) -> bool {
		let write_lock_start = self.metrics_clock.instant();
		let mut inner = self.inner.write();
		Span::current().record("inner_write_lock_us", write_lock_start.elapsed().as_micros() as u64);

		let add_start = self.metrics_clock.instant();
		let window_size = self.config.get_config_uint8(ConfigKey::OracleWindowSize);
		inner.add_committed_transaction(commit_version, conflicts, window_size);
		Span::current().record("add_txn_us", add_start.elapsed().as_micros() as u64);

		let water_mark = self.config.get_config_uint8(ConfigKey::OracleWaterMark) as usize;
		inner.time_windows.len() > water_mark
	}

	#[inline]
	fn cleanup_old_windows(&self) {
		let cleanup_start = self.metrics_clock.instant();
		let safe_evict_below = self.query.done_until();
		let mut inner = self.inner.write();
		let inner = &mut *inner;
		cleanup_old_windows(&mut inner.time_windows, &mut inner.evicted_up_through, safe_evict_below);
		Span::current().record("cleanup_us", cleanup_start.elapsed().as_micros() as u64);
	}

	pub(crate) fn bootstrapping_completed(&self) {
		let mut inner = self.inner.write();
		inner.time_windows.clear();
	}

	pub(crate) fn version(&self) -> Result<CommitVersion> {
		self.clock.current()
	}

	pub fn stop(&mut self) {
		{
			let mut shutdown = self.shutdown_signal.write();
			*shutdown = true;
		}
		{
			let mut inner = self.inner.write();
			inner.time_windows.clear();
		}
		self.actor_system.shutdown();
	}

	pub(crate) fn done_query(&self, version: CommitVersion) {
		self.query.mark_finished(version);
	}

	pub(crate) fn done_commit(&self, version: CommitVersion) {
		self.command.mark_finished(version);
	}

	pub(crate) fn advance_version_for_replica(&self, version: CommitVersion) {
		self.clock.advance_to(version);
	}

	pub(crate) fn advance_unchecked(&self, version: CommitVersion) -> Result<CreateCommitResult> {
		let inner = self.inner.read();
		if version < inner.evicted_up_through {
			return Ok(CreateCommitResult::TooOld);
		}

		let commit_version = self.clock.next()?;
		self.command.register_in_flight(commit_version);
		drop(inner);

		Ok(CreateCommitResult::Success(commit_version))
	}
}

impl OracleState {
	fn add_committed_transaction(&mut self, version: CommitVersion, conflicts: ConflictManager, window_size: u64) {
		let window_start = CommitVersion((version.0 / window_size) * window_size);

		let window =
			self.time_windows.entry(window_start).or_insert_with(|| CommittedWindow::new(window_start));

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
mod tests {
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
	fn test_window_creation_and_indexing() {
		let oracle = create_test_oracle(0);

		// Create a conflict manager with some keys
		let mut conflicts = ConflictManager::new();
		let key1 = create_test_key("key1");
		let key2 = create_test_key("key2");
		conflicts.mark_write(&key1);
		conflicts.mark_write(&key2);

		// Simulate committing a transaction
		let result = oracle.new_commit(CommitVersion(1), conflicts).unwrap();

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

			let version_start = CommitVersion(i as u64 * 500 + 1);
			let result = oracle.new_commit(version_start, conflicts).unwrap();
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
	fn test_range_operations_fallback() {
		let oracle = create_test_oracle(1);

		let key1 = create_test_key("key1");

		// First transaction: writes to a specific key
		let mut conflicts1 = ConflictManager::new();
		conflicts1.mark_write(&key1);

		let result1 = oracle.new_commit(CommitVersion(1), conflicts1).unwrap();
		assert!(matches!(result1, CreateCommitResult::Success(_)));

		// Second transaction: does a range operation (which can't be
		// indexed by specific keys)
		let mut conflicts2 = ConflictManager::new();
		// Simulate a range read that doesn't return specific keys
		let range = EncodedKeyRange::parse("a..z");
		conflicts2.mark_range(range);
		conflicts2.mark_write(&create_test_key("other_key"));

		// This should use the fallback mechanism to check all windows
		let result2 = oracle.new_commit(CommitVersion(1), conflicts2).unwrap();

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
		let r1 = oracle.new_commit(CommitVersion(1), conflicts1).unwrap();
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
		let r2 = oracle.new_commit(CommitVersion(510), conflicts2).unwrap();

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
		let r_b = oracle.new_commit(CommitVersion(1), conflicts_b).unwrap();
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
		let r_a = oracle.new_commit(CommitVersion(1), conflicts_a).unwrap();
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
		let r_3 = oracle.new_commit(CommitVersion(100), conflicts_3).unwrap();

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

		let result = oracle.new_commit(CommitVersion(1), conflicts).unwrap();

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

		let result1 = oracle.new_commit(CommitVersion(1), conflicts1).unwrap();
		assert!(matches!(result1, CreateCommitResult::Success(_)));

		// Second transaction: also writes to shared_key (write-write
		// conflict)
		let mut conflicts2 = ConflictManager::new();
		conflicts2.mark_write(&shared_key);

		let result2 = oracle.new_commit(CommitVersion(1), conflicts2).unwrap();

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

		let result1 = oracle.new_commit(CommitVersion(1), conflicts1).unwrap();
		assert!(matches!(result1, CreateCommitResult::Success(_)));

		// Second transaction: reads from shared_key (read-write
		// conflict)
		let mut conflicts2 = ConflictManager::new();
		conflicts2.mark_read(&shared_key);

		let result2 = oracle.new_commit(CommitVersion(1), conflicts2).unwrap();

		// Should detect conflict because txn2 read from key that txn1
		// wrote to
		assert!(matches!(result2, CreateCommitResult::Conflict(_)));
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

					let result = oracle_clone.new_commit(CommitVersion(1), conflicts).unwrap();

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

				let result = oracle_clone.new_commit(CommitVersion(1), conflicts).unwrap();

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

		// T1: set_disabled() + rollback() must restore a usable manager.
		// Subsequent mark_write must be recorded (the fix), not silently
		// dropped (the bug).
		let mut cm1 = ConflictManager::new();
		cm1.set_disabled();
		cm1.rollback();
		cm1.mark_write(&key);
		assert!(
			cm1.get_write_keys().contains(&key),
			"rollback must reset ConflictMode to Tracking; otherwise the reused \
			 manager would silently drop mark_write and the oracle would register \
			 an empty window for this transaction"
		);

		let v1 = match oracle.new_commit(CommitVersion(1), cm1).unwrap() {
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
		let r2 = oracle.new_commit(CommitVersion(1), cm2).unwrap();

		assert!(
			matches!(r2, CreateCommitResult::Conflict(_)),
			"T2's read+write of `shared` (read_version=1) must conflict with T1's \
			 write at v={}",
			v1.0
		);
	}
}
