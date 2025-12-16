// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	collections::{BTreeMap, BTreeSet, HashMap, HashSet},
	sync::Arc,
};

use parking_lot::{Mutex, RwLock};
use reifydb_core::{CommitVersion, EncodedKey, util::bloom::BloomFilter};
use tracing::{debug_span, instrument};

use crate::multi::{
	conflict::ConflictManager,
	transaction::version::VersionProvider,
	watermark::{Closer, WaterMark},
};

/// Configuration for the efficient oracle
const DEFAULT_WINDOW_SIZE: u64 = 1000;
const MAX_WINDOWS: usize = 50;
const CLEANUP_THRESHOLD: usize = 40;
pub const MAX_COMMITTED_TXNS: usize = MAX_WINDOWS * 200;

/// Time window containing committed transactions
pub(super) struct CommittedWindow {
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

	/// Get the modified keys for cleanup purposes
	pub(super) fn get_modified_keys(&self) -> &HashSet<EncodedKey> {
		&self.modified_keys
	}
}

/// Oracle implementation with time-window based conflict detection
pub(super) struct OracleInner<L>
where
	L: VersionProvider,
{
	pub clock: L,
	pub last_cleanup: CommitVersion,

	/// Time windows containing committed transactions, keyed by window
	/// start version
	pub time_windows: BTreeMap<CommitVersion, CommittedWindow>,

	/// Index: key -> set of window versions that modified this key
	pub key_to_windows: HashMap<EncodedKey, BTreeSet<CommitVersion>>,

	/// Current window size for new windows
	pub window_size: u64,
}

#[derive(Debug)]
pub(super) struct CommittedTxn {
	version: CommitVersion,
	conflict_manager: Option<ConflictManager>,
}

pub(super) enum CreateCommitResult {
	Success(CommitVersion),
	Conflict(ConflictManager),
}

/// Oracle with time-window based conflict detection
pub(super) struct Oracle<L>
where
	L: VersionProvider,
{
	// Using RwLock allows multiple concurrent readers during conflict detection
	pub(super) inner: RwLock<OracleInner<L>>,

	/// Version provider lock for serializing version generation only
	pub(super) version_lock: Mutex<()>,

	/// Used by DB
	pub(super) query: WaterMark,
	/// Used to block new transaction, so all previous commits are visible
	/// to a new query.
	pub(super) command: WaterMark,

	/// Shutdown signal for cleanup thread
	shutdown_signal: Arc<RwLock<bool>>,

	/// closer is used to stop watermarks.
	closer: Closer,
}

impl<L> Oracle<L>
where
	L: VersionProvider,
{
	/// Create a new oracle with efficient conflict detection
	pub fn new(clock: L) -> Self {
		let closer = Closer::new(2);
		let shutdown_signal = Arc::new(RwLock::new(false));

		Self {
			inner: RwLock::new(OracleInner {
				clock,
				last_cleanup: CommitVersion(0),
				time_windows: BTreeMap::new(),
				key_to_windows: HashMap::with_capacity(10000),
				window_size: DEFAULT_WINDOW_SIZE,
			}),
			version_lock: Mutex::new(()),
			query: WaterMark::new("txn-mark-query".into(), closer.clone()),
			command: WaterMark::new("txn-mark-cmd".into(), closer.clone()),
			shutdown_signal,
			closer,
		}
	}

	/// Efficient conflict detection using time windows and key indexing
	#[instrument(level = "debug", skip(self, done_read, conflicts), fields(%version))]
	pub(super) fn new_commit(
		&self,
		done_read: &mut bool,
		version: CommitVersion,
		conflicts: ConflictManager,
	) -> crate::Result<CreateCommitResult> {
		// First, perform conflict detection with read lock for better
		// concurrency
		let inner = self.inner.read();

		// Get keys involved in this transaction for efficient filtering
		// Use references to avoid cloning
		let read_keys = conflicts.get_read_keys();
		let write_keys = conflicts.get_write_keys();
		let has_keys = !read_keys.is_empty() || !write_keys.is_empty();

		// Only check conflicts in windows that contain relevant keys
		let relevant_windows: Vec<CommitVersion> = {
			let _span = debug_span!(
				"find_relevant_windows",
				read_key_count = read_keys.len(),
				conflict_key_count = write_keys.len(),
				has_range_ops = conflicts.has_range_operations()
			)
			.entered();

			if !has_keys {
				// If no specific keys, we need to check recent windows
				// for range/all operations
				inner.time_windows.range(version..).take(5).map(|(&v, _)| v).collect()
			} else {
				let mut windows = HashSet::new();

				// Iterate over combined keys once instead of twice
				for key in read_keys.iter().chain(write_keys.iter()) {
					if let Some(window_versions) = inner.key_to_windows.get(key) {
						windows.extend(window_versions.iter().copied());
					}
				}

				// If no windows found via key index, only fall back to full
				// window scan if there are range operations. For point
				// operations on new keys, no conflicts are possible since
				// no other transaction could have written to those keys.
				if windows.is_empty() {
					if conflicts.has_range_operations() {
						// Range operations require checking all windows
						inner.time_windows.keys().copied().collect()
					} else {
						// New keys with no range ops = no possible conflicts
						Vec::new()
					}
				} else {
					windows.into_iter().collect()
				}
			}
		};

		// Check for conflicts only in relevant windows
		{
			let _span = debug_span!("check_windows", window_count = relevant_windows.len()).entered();

			for window_version in &relevant_windows {
				if let Some(window) = inner.time_windows.get(window_version) {
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
						// 1. If any of our writes conflict with window's writes (write-write
						//    conflict)
						// 2. If any of our reads overlap with window's writes (read-write
						//    conflict)
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
						// Skip transactions that committed
						// before we started reading
						if committed_txn.version <= version {
							continue;
						}

						if let Some(old_conflicts) = &committed_txn.conflict_manager {
							if conflicts.has_conflict(old_conflicts) {
								return Ok(CreateCommitResult::Conflict(conflicts));
							}
						}
					}
				}
			}
		}

		// Release read lock and acquire write lock for commit
		drop(inner);

		// No conflicts found, proceed with commit
		if !*done_read {
			self.query.done(version);
			*done_read = true;
		}

		// Get commit version with minimal locking
		let commit_version = {
			let _span = debug_span!("allocate_version").entered();
			let _version_guard = self.version_lock.lock();
			let inner = self.inner.read();
			inner.clock.next()?
		};

		// Add this transaction to the appropriate window with write lock
		let needs_cleanup = {
			let _span = debug_span!("add_to_index").entered();
			let mut inner = self.inner.write();
			inner.add_committed_transaction(commit_version, conflicts);

			// Check if cleanup is needed
			inner.time_windows.len() > CLEANUP_THRESHOLD
		};

		if needs_cleanup {
			let _span = debug_span!("cleanup_windows").entered();
			let mut inner = self.inner.write();
			let inner = &mut *inner;
			super::oracle_cleanup::cleanup_old_windows(&mut inner.time_windows, &mut inner.key_to_windows);
		}

		self.command.done(commit_version);

		Ok(CreateCommitResult::Success(commit_version))
	}

	pub(super) fn version(&self) -> crate::Result<CommitVersion> {
		self.inner.read().clock.current()
	}

	pub(super) fn discard_at_or_below(&self) -> CommitVersion {
		self.command.done_until()
	}

	pub fn stop(&mut self) {
		// Signal shutdown
		{
			let mut shutdown = self.shutdown_signal.write();
			*shutdown = true;
		}

		self.closer.signal_and_wait();
	}

	/// Mark a query as done (for compatibility with existing API)
	pub(super) fn done_query(&self, version: CommitVersion) {
		self.query.done(version);
	}

	/// Mark a commit as done (for compatibility with existing API)  
	pub(super) fn done_commit(&self, version: CommitVersion) {
		self.command.done(version);
	}
}

impl<L> OracleInner<L>
where
	L: VersionProvider,
{
	/// Add a committed transaction to the appropriate time window
	fn add_committed_transaction(&mut self, version: CommitVersion, conflicts: ConflictManager) {
		// Determine which window this transaction belongs to
		let window_start = CommitVersion((version.0 / self.window_size) * self.window_size);

		// Get or create the window
		let window =
			self.time_windows.entry(window_start).or_insert_with(|| CommittedWindow::new(window_start));

		// Update key index for all conflict keys
		let write_keys = conflicts.get_write_keys();
		for key in write_keys {
			self.key_to_windows.entry(key.clone()).or_insert_with(BTreeSet::new).insert(window_start);
		}

		// Add transaction to window
		let txn = CommittedTxn {
			version,
			conflict_manager: Some(conflicts),
		};

		window.add_transaction(txn);
		self.last_cleanup = self.last_cleanup.max(version);
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
	use std::sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	};

	use super::*;
	use crate::multi::transaction::version::VersionProvider;

	// Mock version provider for testing
	#[derive(Debug)]
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
		fn next(&self) -> crate::Result<CommitVersion> {
			Ok(CommitVersion(self.current.fetch_add(1, Ordering::Relaxed) + 1))
		}

		fn current(&self) -> crate::Result<CommitVersion> {
			Ok(CommitVersion(self.current.load(Ordering::Relaxed)))
		}
	}

	fn create_test_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes().to_vec())
	}

	#[test]
	fn test_oracle_basic_creation() {
		let clock = MockVersionProvider::new(0);
		let oracle = Oracle::<_>::new(clock);

		// Oracle should be created successfully
		assert_eq!(oracle.version().unwrap(), 0);
	}

	#[test]
	fn test_window_creation_and_indexing() {
		let clock = MockVersionProvider::new(0);
		let oracle = Oracle::<_>::new(clock);

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

				// Check that keys were indexed
				let inner = oracle.inner.read();
				assert!(inner.key_to_windows.contains_key(&key1));
				assert!(inner.key_to_windows.contains_key(&key2));

				// Check that window was created
				assert!(inner.time_windows.len() > 0);
			}
			CreateCommitResult::Conflict(_) => panic!("Unexpected conflict for first transaction"),
		}
	}

	#[test]
	fn test_conflict_detection_between_transactions() {
		let clock = MockVersionProvider::new(1);
		let oracle = Oracle::<_>::new(clock);

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
		let clock = MockVersionProvider::new(0);
		let oracle = Oracle::<_>::new(clock);

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
		let clock = MockVersionProvider::new(0);
		let oracle = Oracle::<_>::new(clock);

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
			let version_start = CommitVersion(i as u64 * DEFAULT_WINDOW_SIZE + 1);
			let result = oracle.new_commit(&mut done_read, version_start, conflicts).unwrap();
			assert!(matches!(result, CreateCommitResult::Success(_)));
		}

		// Check key indexing across multiple windows
		let inner = oracle.inner.read();

		// key1 should be in windows 0 and 2000 (i=0,2)
		let key1_windows = inner.key_to_windows.get(&key1).unwrap();
		assert!(key1_windows.len() >= 1);

		// key2 should be in window 1000 (i=1)
		let key2_windows = inner.key_to_windows.get(&key2).unwrap();
		assert!(key2_windows.len() >= 1);
	}

	#[test]
	fn test_version_filtering_in_conflict_detection() {
		let clock = MockVersionProvider::new(2);
		let oracle = Oracle::<_>::new(clock);

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
		let clock = MockVersionProvider::new(1);
		let oracle = Oracle::<_>::new(clock);

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
		use reifydb_core::EncodedKeyRange;
		let range = EncodedKeyRange::parse("a..z");
		conflicts2.mark_range(range);
		conflicts2.mark_write(&create_test_key("other_key"));

		// This should use the fallback mechanism to check all windows
		let mut done_read2 = false;
		let result2 = oracle.new_commit(&mut done_read2, CommitVersion(1), conflicts2).unwrap();

		// Should detect conflict due to the range overlap with key1
		assert!(matches!(result2, CreateCommitResult::Conflict(_)));
	}

	#[test]
	fn test_window_cleanup_mechanism() {
		let clock = MockVersionProvider::new(0);
		let oracle = Oracle::<_>::new(clock);

		// Add many transactions to trigger cleanup
		let mut keys = Vec::new();
		for i in 0..(CLEANUP_THRESHOLD + 10) {
			let key = create_test_key(&format!("key{}", i));
			keys.push(key.clone());

			let mut conflicts = ConflictManager::new();
			conflicts.mark_write(&key);

			let mut done_read = false;
			let version_start = CommitVersion(i as u64 * DEFAULT_WINDOW_SIZE + 1);
			let result = oracle.new_commit(&mut done_read, version_start, conflicts).unwrap();
			assert!(matches!(result, CreateCommitResult::Success(_)));
		}

		// Check that cleanup occurred
		let inner = oracle.inner.read();
		assert!(inner.time_windows.len() <= MAX_WINDOWS);

		// Verify that key index was also cleaned up
		for (i, key) in keys.iter().enumerate() {
			if i < (CLEANUP_THRESHOLD + 10 - MAX_WINDOWS) {
				// Old keys should be removed from index
				assert!(!inner.key_to_windows.contains_key(key));
			} else {
				// Recent keys should still be present
				assert!(inner.key_to_windows.contains_key(key));
			}
		}
	}

	#[test]
	fn test_empty_conflict_manager() {
		let clock = MockVersionProvider::new(0);
		let oracle = Oracle::<_>::new(clock);

		// Transaction with no conflicts (read-only)
		let conflicts = ConflictManager::new(); // Empty conflict manager

		let mut done_read = false;
		let result = oracle.new_commit(&mut done_read, CommitVersion(1), conflicts).unwrap();

		// Should succeed but not create any key index entries
		match result {
			CreateCommitResult::Success(_) => {
				let inner = oracle.inner.read();
				assert!(inner.key_to_windows.is_empty());
			}
			CreateCommitResult::Conflict(_) => {
				panic!("Empty conflict manager should not cause conflicts")
			}
		}
	}

	#[test]
	fn test_write_write_conflict() {
		let clock = MockVersionProvider::new(1);
		let oracle = Oracle::<_>::new(clock);

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
		let clock = MockVersionProvider::new(1);
		let oracle = Oracle::<_>::new(clock);

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
		let clock = MockVersionProvider::new(0);
		let oracle = Oracle::<_>::new(clock);

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
		let clock = MockVersionProvider::new(1);
		let oracle = Oracle::<_>::new(clock);

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
}
