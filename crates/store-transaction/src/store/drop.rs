// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Drop operation implementation for cleaning up versioned entries.
//!
//! The Drop operation completely erases versioned entries from storage without
//! writing tombstones or generating CDC events. It's used for internal cleanup
//! operations like maintaining single-version semantics for flow node state.

use std::ops::Bound;

use reifydb_core::CommitVersion;
use reifydb_type::CowVec;

use super::version::{encode_versioned_key, extract_version, key_version_range};
use crate::tier::{EntryKind, RangeCursor, TierStorage};

/// Information about an entry to be dropped.
#[derive(Debug, Clone)]
pub struct DropEntry {
	/// The versioned key to delete
	pub versioned_key: CowVec<u8>,
	/// Size of the value being dropped (0 if tombstone)
	pub value_bytes: u64,
}

/// Find versioned keys to drop based on constraints.
///
/// # Arguments
/// - `storage`: The storage backend to scan
/// - `table`: The table containing the keys
/// - `key`: The logical key (without version suffix)
/// - `up_to_version`: If Some(v), candidate versions where version < v
/// - `keep_last_versions`: If Some(n), protect n most recent versions from being dropped
pub(crate) fn find_keys_to_drop<S: TierStorage>(
	storage: &S,
	table: EntryKind,
	key: &[u8],
	up_to_version: Option<CommitVersion>,
	keep_last_versions: Option<usize>,
	pending_version: Option<CommitVersion>,
) -> crate::Result<Vec<DropEntry>> {
	let (start, end) = key_version_range(key);

	// Collect all versioned keys for this logical key, including value sizes
	let mut versioned_entries: Vec<(CowVec<u8>, CommitVersion, u64)> = Vec::new();

	let mut cursor = RangeCursor::new();
	loop {
		let batch = storage.range_next(
			table,
			&mut cursor,
			Bound::Included(start.as_slice()),
			Bound::Included(end.as_slice()),
			1024,
		)?;

		for entry in batch.entries {
			if let Some(entry_version) = extract_version(&entry.key) {
				let value_bytes = entry.value.as_ref().map(|v| v.len() as u64).unwrap_or(0);
				versioned_entries.push((entry.key, entry_version, value_bytes));
			}
		}

		if cursor.exhausted {
			break;
		}
	}

	// Include pending version if provided (version being written in current batch)
	// This prevents a race where Drop scans storage before Set is written
	if let Some(pending_ver) = pending_version {
		// Add a placeholder entry for the pending version
		// value_bytes=0 is fine since this entry will never be dropped (it's the newest)
		let pending_key = CowVec::new(encode_versioned_key(key, pending_ver));
		versioned_entries.push((pending_key, pending_ver, 0));
	}

	// Sort by version descending (most recent first) for keep_last_versions logic
	versioned_entries.sort_by(|a, b| b.1.cmp(&a.1));

	// Determine which entries to drop
	let mut entries_to_drop = Vec::new();

	for (idx, (versioned_key, entry_version, value_bytes)) in versioned_entries.into_iter().enumerate() {
		// Use AND logic for combined constraints:
		// - keep_last_versions protects the N most recent versions
		// - up_to_version only drops versions < threshold IF not protected
		let should_drop = match (up_to_version, keep_last_versions) {
			// Both None: drop everything
			(None, None) => true,
			// Only version constraint: drop if version < threshold
			(Some(threshold), None) => entry_version < threshold,
			// Only keep constraint: drop if beyond keep count
			(None, Some(keep_count)) => idx >= keep_count,
			// Both constraints (AND): drop only if BOTH say drop
			// This ensures keep_last_versions always protects N versions
			(Some(threshold), Some(keep_count)) => entry_version < threshold && idx >= keep_count,
		};

		if should_drop {
			// Never drop the pending version (it's being written in this batch)
			if Some(entry_version) == pending_version {
				continue;
			}

			entries_to_drop.push(DropEntry {
				versioned_key,
				value_bytes,
			});
		}
	}

	Ok(entries_to_drop)
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use reifydb_core::runtime::ComputePool;

	use super::{
		super::version::{encode_versioned_key, extract_key},
		*,
	};
	use crate::hot::HotStorage;

	fn test_compute_pool() -> ComputePool {
		ComputePool::new(2, 8)
	}

	/// Create versioned test entries for a key
	fn setup_versioned_entries(storage: &HotStorage, table: EntryKind, key: &[u8], versions: &[u64]) {
		let entries: Vec<_> = versions
			.iter()
			.map(|v| {
				let versioned_key = CowVec::new(encode_versioned_key(key, CommitVersion(*v)));
				(versioned_key, Some(CowVec::new(vec![*v as u8]))) // value = version byte
			})
			.collect();

		storage.set(HashMap::from([(table, entries)])).unwrap();
	}

	/// Extract version numbers from the drop entries
	fn extract_dropped_versions(entries: &[DropEntry]) -> Vec<u64> {
		entries.iter().filter_map(|e| extract_version(&e.versioned_key).map(|v| v.0)).collect()
	}

	#[test]
	fn test_drop_all_versions() {
		let storage = HotStorage::memory(test_compute_pool());
		let table = EntryKind::Multi;
		let key = b"test_key";

		setup_versioned_entries(&storage, table, key, &[1, 5, 10, 20, 100]);

		let to_drop = find_keys_to_drop(&storage, table, key, None, None, None).unwrap();

		assert_eq!(to_drop.len(), 5);
		let versions = extract_dropped_versions(&to_drop);
		assert!(versions.contains(&1));
		assert!(versions.contains(&5));
		assert!(versions.contains(&10));
		assert!(versions.contains(&20));
		assert!(versions.contains(&100));
	}

	#[test]
	fn test_drop_up_to_version() {
		let storage = HotStorage::memory(test_compute_pool());
		let table = EntryKind::Multi;
		let key = b"test_key";

		// Versions: 1, 5, 10, 20, 100
		setup_versioned_entries(&storage, table, key, &[1, 5, 10, 20, 100]);

		// Drop versions < 10 (should drop 1, 5)
		let to_drop = find_keys_to_drop(&storage, table, key, Some(CommitVersion(10)), None, None).unwrap();

		let versions = extract_dropped_versions(&to_drop);
		assert_eq!(versions.len(), 2);
		assert!(versions.contains(&1));
		assert!(versions.contains(&5));
		assert!(!versions.contains(&10));
		assert!(!versions.contains(&20));
		assert!(!versions.contains(&100));
	}

	#[test]
	fn test_drop_up_to_version_boundary() {
		// Test exact boundary - version == threshold should NOT be dropped
		let storage = HotStorage::memory(test_compute_pool());
		let table = EntryKind::Multi;
		let key = b"test_key";

		setup_versioned_entries(&storage, table, key, &[9, 10, 11]);

		let to_drop = find_keys_to_drop(&storage, table, key, Some(CommitVersion(10)), None, None).unwrap();

		let versions = extract_dropped_versions(&to_drop);
		assert_eq!(versions.len(), 1);
		assert!(versions.contains(&9)); // Only 9 < 10
	}

	#[test]
	fn test_keep_last_n_versions() {
		let storage = HotStorage::memory(test_compute_pool());
		let table = EntryKind::Multi;
		let key = b"test_key";

		// Versions: 1, 5, 10, 20, 100 (sorted descending: 100, 20, 10, 5, 1)
		setup_versioned_entries(&storage, table, key, &[1, 5, 10, 20, 100]);

		// Keep 2 most recent (100, 20), drop others (10, 5, 1)
		let to_drop = find_keys_to_drop(&storage, table, key, None, Some(2), None).unwrap();

		let versions = extract_dropped_versions(&to_drop);
		assert_eq!(versions.len(), 3);
		assert!(versions.contains(&1));
		assert!(versions.contains(&5));
		assert!(versions.contains(&10));
		assert!(!versions.contains(&20));
		assert!(!versions.contains(&100));
	}

	#[test]
	fn test_keep_more_than_exists() {
		// Keep 10 but only 3 exist - should drop nothing
		let storage = HotStorage::memory(test_compute_pool());
		let table = EntryKind::Multi;
		let key = b"test_key";

		setup_versioned_entries(&storage, table, key, &[1, 5, 10]);

		let to_drop = find_keys_to_drop(&storage, table, key, None, Some(10), None).unwrap();

		assert!(to_drop.is_empty());
	}

	#[test]
	fn test_keep_zero_versions() {
		// Keep 0 = drop all
		let storage = HotStorage::memory(test_compute_pool());
		let table = EntryKind::Multi;
		let key = b"test_key";

		setup_versioned_entries(&storage, table, key, &[1, 5, 10]);

		let to_drop = find_keys_to_drop(&storage, table, key, None, Some(0), None).unwrap();

		assert_eq!(to_drop.len(), 3);
	}

	#[test]
	fn test_keep_one_version() {
		let storage = HotStorage::memory(test_compute_pool());
		let table = EntryKind::Multi;
		let key = b"test_key";

		setup_versioned_entries(&storage, table, key, &[1, 5, 10, 20, 100]);

		// Keep only most recent (100)
		let to_drop = find_keys_to_drop(&storage, table, key, None, Some(1), None).unwrap();

		let versions = extract_dropped_versions(&to_drop);
		assert_eq!(versions.len(), 4);
		assert!(!versions.contains(&100)); // Most recent kept
	}

	#[test]
	fn test_combined_constraints_keep_protects() {
		let storage = HotStorage::memory(test_compute_pool());
		let table = EntryKind::Multi;
		let key = b"test_key";

		// Versions: 1, 5, 10, 20, 100 (sorted desc: 100, 20, 10, 5, 1)
		setup_versioned_entries(&storage, table, key, &[1, 5, 10, 20, 100]);

		// up_to_version=15 would drop: 1, 5, 10 (all < 15)
		// keep_last_versions=3 protects: 100, 20, 10 (indices 0, 1, 2)
		// Combined (AND logic): drop only if (version < 15) AND (idx >= 3)
		// - 100: idx=0, 100 >= 15 → KEEP
		// - 20: idx=1, 20 >= 15 → KEEP
		// - 10: idx=2, 10 < 15 BUT idx < 3 → KEEP (protected!)
		// - 5: idx=3, 5 < 15 AND idx >= 3 → DROP
		// - 1: idx=4, 1 < 15 AND idx >= 3 → DROP
		let to_drop = find_keys_to_drop(&storage, table, key, Some(CommitVersion(15)), Some(3), None).unwrap();

		let versions = extract_dropped_versions(&to_drop);
		assert_eq!(versions.len(), 2); // Only 1 and 5 dropped
		assert!(versions.contains(&1));
		assert!(versions.contains(&5));
		assert!(!versions.contains(&10)); // Protected by keep_last=3
		assert!(!versions.contains(&20));
		assert!(!versions.contains(&100));
	}

	#[test]
	fn test_combined_constraints_version_restricts() {
		// Test case where up_to_version is more restrictive than keep_last
		let storage = HotStorage::memory(test_compute_pool());
		let table = EntryKind::Multi;
		let key = b"test_key";

		// Versions: 1, 5, 10, 20, 100 (sorted desc: 100, 20, 10, 5, 1)
		setup_versioned_entries(&storage, table, key, &[1, 5, 10, 20, 100]);

		// up_to_version=3 would drop: only 1 (1 < 3)
		// keep_last_versions=2 protects: 100, 20 (indices 0, 1)
		// Combined (AND logic): drop only if (version < 3) AND (idx >= 2)
		// - 100: idx=0 → KEEP (protected)
		// - 20: idx=1 → KEEP (protected)
		// - 10: idx=2, 10 >= 3 → KEEP (version constraint not met)
		// - 5: idx=3, 5 >= 3 → KEEP (version constraint not met)
		// - 1: idx=4, 1 < 3 AND idx >= 2 → DROP
		let to_drop = find_keys_to_drop(&storage, table, key, Some(CommitVersion(3)), Some(2), None).unwrap();

		let versions = extract_dropped_versions(&to_drop);
		assert_eq!(versions.len(), 1); // Only 1 dropped
		assert!(versions.contains(&1));
	}

	#[test]
	fn test_combined_constraints_both_aggressive() {
		// Both constraints are aggressive
		let storage = HotStorage::memory(test_compute_pool());
		let table = EntryKind::Multi;
		let key = b"test_key";

		// Versions: 1, 5, 10, 20, 100 (sorted desc: 100, 20, 10, 5, 1)
		setup_versioned_entries(&storage, table, key, &[1, 5, 10, 20, 100]);

		// up_to_version=50 would drop: 1, 5, 10, 20 (all < 50)
		// keep_last_versions=1 protects: only 100 (index 0)
		// Combined (AND logic): drop only if (version < 50) AND (idx >= 1)
		// - 100: idx=0 → KEEP (protected)
		// - 20: idx=1, 20 < 50 AND idx >= 1 → DROP
		// - 10: idx=2, 10 < 50 AND idx >= 1 → DROP
		// - 5: idx=3, 5 < 50 AND idx >= 1 → DROP
		// - 1: idx=4, 1 < 50 AND idx >= 1 → DROP
		let to_drop = find_keys_to_drop(&storage, table, key, Some(CommitVersion(50)), Some(1), None).unwrap();

		let versions = extract_dropped_versions(&to_drop);
		assert_eq!(versions.len(), 4); // All except 100
		assert!(versions.contains(&1));
		assert!(versions.contains(&5));
		assert!(versions.contains(&10));
		assert!(versions.contains(&20));
		assert!(!versions.contains(&100)); // Protected
	}

	// ==================== Edge cases ====================

	#[test]
	fn test_empty_storage() {
		let storage = HotStorage::memory(test_compute_pool());
		let table = EntryKind::Multi;
		let key = b"nonexistent";

		let to_drop = find_keys_to_drop(&storage, table, key, None, None, None).unwrap();
		assert!(to_drop.is_empty());
	}

	#[test]
	fn test_single_version_drop_all() {
		let storage = HotStorage::memory(test_compute_pool());
		let table = EntryKind::Multi;
		let key = b"test_key";

		setup_versioned_entries(&storage, table, key, &[42]);

		// Drop all
		let to_drop = find_keys_to_drop(&storage, table, key, None, None, None).unwrap();
		assert_eq!(to_drop.len(), 1);
	}

	#[test]
	fn test_single_version_keep_one() {
		let storage = HotStorage::memory(test_compute_pool());
		let table = EntryKind::Multi;
		let key = b"test_key";

		setup_versioned_entries(&storage, table, key, &[42]);

		// Keep 1 - should drop nothing
		let to_drop = find_keys_to_drop(&storage, table, key, None, Some(1), None).unwrap();
		assert!(to_drop.is_empty());
	}

	#[test]
	fn test_different_keys_isolated() {
		let storage = HotStorage::memory(test_compute_pool());
		let table = EntryKind::Multi;

		setup_versioned_entries(&storage, table, b"key_a", &[1, 2, 3]);
		setup_versioned_entries(&storage, table, b"key_b", &[10, 20, 30]);

		// Drop all versions of key_a
		let to_drop = find_keys_to_drop(&storage, table, b"key_a", None, None, None).unwrap();

		assert_eq!(to_drop.len(), 3);
		// Verify all dropped keys are for key_a, not key_b
		for entry in &to_drop {
			let original = extract_key(&entry.versioned_key).unwrap();
			assert_eq!(original, b"key_a");
		}
	}

	#[test]
	fn test_up_to_version_zero() {
		// up_to_version=0 means drop nothing (no versions < 0)
		let storage = HotStorage::memory(test_compute_pool());
		let table = EntryKind::Multi;
		let key = b"test_key";

		setup_versioned_entries(&storage, table, key, &[1, 5, 10]);

		let to_drop = find_keys_to_drop(&storage, table, key, Some(CommitVersion(0)), None, None).unwrap();

		assert!(to_drop.is_empty());
	}

	#[test]
	fn test_up_to_version_max() {
		// up_to_version=MAX means drop all (all versions < MAX)
		let storage = HotStorage::memory(test_compute_pool());
		let table = EntryKind::Multi;
		let key = b"test_key";

		setup_versioned_entries(&storage, table, key, &[1, 5, u64::MAX - 1]);

		let to_drop =
			find_keys_to_drop(&storage, table, key, Some(CommitVersion(u64::MAX)), None, None).unwrap();

		assert_eq!(to_drop.len(), 3);
	}
}
