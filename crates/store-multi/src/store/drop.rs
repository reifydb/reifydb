// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Drop operation implementation for cleaning up versioned entries.
//!
//! The Drop operation completely erases versioned entries from storage without
//! writing tombstones or generating CDC events. It's used for internal cleanup
//! operations like maintaining single-version semantics for flow node state.

use reifydb_core::{common::CommitVersion, interface::store::EntryKind};
use reifydb_type::util::cowvec::CowVec;

use crate::{Result, tier::TierStorage};

/// Information about an entry to be dropped.
#[derive(Debug, Clone)]
pub struct DropEntry {
	/// The logical key to delete
	pub key: CowVec<u8>,
	/// The specific version to delete
	pub version: CommitVersion,
	/// The size of the value being dropped (for metrics tracking)
	pub value_bytes: u64,
}

/// Find historical versioned keys to drop.
///
/// Always keeps the most recent version and drops everything else.
///
/// # Arguments
/// - `storage`: The storage backend to scan
/// - `table`: The table containing the keys
/// - `key`: The logical key (without version suffix)
/// - `pending_version`: Version being written in the same batch (to avoid race)
pub(crate) fn find_keys_to_drop<S: TierStorage>(
	storage: &S,
	table: EntryKind,
	key: &[u8],
	pending_version: Option<CommitVersion>,
) -> Result<Vec<DropEntry>> {
	// Get all versions of this key directly (bypasses MVCC resolution)
	let all_versions = storage.get_all_versions(table, key)?;

	// Collect all versions with their value sizes
	let mut versioned_entries: Vec<(CommitVersion, u64)> = all_versions
		.into_iter()
		.map(|(version, value)| {
			let value_bytes = value.as_ref().map(|v| v.len() as u64).unwrap_or(0);
			(version, value_bytes)
		})
		.collect();

	// Include pending version if provided (version being written in current batch)
	// This prevents a race where Drop scans storage before Set is written
	if let Some(pending_ver) = pending_version {
		// Check if pending version already exists (avoid duplicates)
		if !versioned_entries.iter().any(|(v, _)| *v == pending_ver) {
			// Add a placeholder entry for the pending version
			// value_bytes=0 is fine since this entry will never be dropped (it's the newest)
			versioned_entries.push((pending_ver, 0));
		}
	}

	// Sort by version descending (most recent first)
	versioned_entries.sort_by(|a, b| b.0.cmp(&a.0));

	// Determine which entries to drop: Keep only index 0 (the latest), drop all others
	let mut entries_to_drop = Vec::new();
	let key_cow = CowVec::new(key.to_vec());

	for (idx, (entry_version, value_bytes)) in versioned_entries.into_iter().enumerate() {
		// Aggressive cleanup: Drop everything except the most recent version
		let should_drop = idx > 0;

		if should_drop {
			// Never drop the pending version (it's being written in this batch)
			if Some(entry_version) == pending_version {
				continue;
			}

			entries_to_drop.push(DropEntry {
				key: key_cow.clone(),
				version: entry_version,
				value_bytes,
			});
		}
	}

	Ok(entries_to_drop)
}

#[cfg(test)]
pub mod tests {
	use std::collections::HashMap;

	use super::*;
	use crate::hot::storage::HotStorage;

	/// Create versioned test entries for a key
	fn setup_versioned_entries(storage: &HotStorage, table: EntryKind, key: &[u8], versions: &[u64]) {
		for v in versions {
			let entries = vec![(CowVec::new(key.to_vec()), Some(CowVec::new(vec![*v as u8])))];
			storage.set(CommitVersion(*v), HashMap::from([(table, entries)])).unwrap();
		}
	}

	/// Extract version numbers from the drop entries
	fn extract_dropped_versions(entries: &[DropEntry]) -> Vec<u64> {
		entries.iter().map(|e| e.version.0).collect()
	}

	#[test]
	fn test_drop_historical_versions() {
		let storage = HotStorage::memory();
		let table = EntryKind::Multi;
		let key = b"test_key";

		// Versions: 1, 5, 10, 20, 100
		setup_versioned_entries(&storage, table, key, &[1, 5, 10, 20, 100]);

		// Should drop all except 100
		let to_drop = find_keys_to_drop(&storage, table, key, None).unwrap();

		assert_eq!(to_drop.len(), 4);
		let versions = extract_dropped_versions(&to_drop);
		assert!(versions.contains(&1));
		assert!(versions.contains(&5));
		assert!(versions.contains(&10));
		assert!(versions.contains(&20));
		assert!(!versions.contains(&100));
	}

	#[test]
	fn test_keep_latest_with_pending() {
		let storage = HotStorage::memory();
		let table = EntryKind::Multi;
		let key = b"test_key";

		// Existing: 1, 5, 10. Pending: 20.
		setup_versioned_entries(&storage, table, key, &[1, 5, 10]);

		// Should keep 20 (pending) and drop 1, 5, 10
		let to_drop = find_keys_to_drop(&storage, table, key, Some(CommitVersion(20))).unwrap();

		assert_eq!(to_drop.len(), 3);
		let versions = extract_dropped_versions(&to_drop);
		assert!(versions.contains(&1));
		assert!(versions.contains(&5));
		assert!(versions.contains(&10));
		assert!(!versions.contains(&20));
	}

	#[test]
	fn test_single_version_no_drop() {
		let storage = HotStorage::memory();
		let table = EntryKind::Multi;
		let key = b"test_key";

		setup_versioned_entries(&storage, table, key, &[42]);

		// Only one version exists, should drop nothing
		let to_drop = find_keys_to_drop(&storage, table, key, None).unwrap();
		assert!(to_drop.is_empty());
	}

	#[test]
	fn test_empty_storage() {
		let storage = HotStorage::memory();
		let table = EntryKind::Multi;
		let key = b"nonexistent";

		let to_drop = find_keys_to_drop(&storage, table, key, None).unwrap();
		assert!(to_drop.is_empty());
	}
}
