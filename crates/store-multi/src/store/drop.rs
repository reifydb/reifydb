// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, encoded::key::EncodedKey, interface::store::EntryKind};

use crate::{Result, tier::TierStorage};

#[derive(Debug, Clone)]
pub struct DropEntry {
	pub key: EncodedKey,

	pub version: CommitVersion,

	pub value_bytes: u64,
}

pub(crate) fn find_keys_to_drop<S: TierStorage>(
	storage: &S,
	table: EntryKind,
	key: &[u8],
	pending_version: Option<CommitVersion>,
) -> Result<Vec<DropEntry>> {
	let all_versions = storage.get_all_versions(table, key)?;

	let mut versioned_entries: Vec<(CommitVersion, u64)> = all_versions
		.into_iter()
		.map(|(version, value)| {
			let value_bytes = value.as_ref().map(|v| v.len() as u64).unwrap_or(0);
			(version, value_bytes)
		})
		.collect();

	if let Some(pending_ver) = pending_version
		&& !versioned_entries.iter().any(|(v, _)| *v == pending_ver)
	{
		versioned_entries.push((pending_ver, 0));
	}

	versioned_entries.sort_by(|a, b| b.0.cmp(&a.0));

	let mut entries_to_drop = Vec::with_capacity(versioned_entries.len().saturating_sub(1));
	let drop_key = EncodedKey::new(key.to_vec());

	for (idx, (entry_version, value_bytes)) in versioned_entries.into_iter().enumerate() {
		let should_drop = idx > 0;

		if should_drop {
			if Some(entry_version) == pending_version {
				continue;
			}

			entries_to_drop.push(DropEntry {
				key: drop_key.clone(),
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

	use reifydb_value::util::cowvec::CowVec;

	use super::*;
	use crate::tier::commit::buffer::MultiCommitBufferTier;

	/// Create versioned test entries for a key
	fn setup_versioned_entries(storage: &MultiCommitBufferTier, table: EntryKind, key: &[u8], versions: &[u64]) {
		for v in versions {
			let entries = vec![(EncodedKey::new(key.to_vec()), Some(CowVec::new(vec![*v as u8])))];
			storage.set(CommitVersion(*v), HashMap::from([(table, entries)])).unwrap();
		}
	}

	/// Extract version numbers from the drop entries
	fn extract_dropped_versions(entries: &[DropEntry]) -> Vec<u64> {
		entries.iter().map(|e| e.version.0).collect()
	}

	#[test]
	fn test_drop_historical_versions() {
		let storage = MultiCommitBufferTier::memory();
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
		let storage = MultiCommitBufferTier::memory();
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
		let storage = MultiCommitBufferTier::memory();
		let table = EntryKind::Multi;
		let key = b"test_key";

		setup_versioned_entries(&storage, table, key, &[42]);

		// Only one version exists, should drop nothing
		let to_drop = find_keys_to_drop(&storage, table, key, None).unwrap();
		assert!(to_drop.is_empty());
	}

	#[test]
	fn test_empty_storage() {
		let storage = MultiCommitBufferTier::memory();
		let table = EntryKind::Multi;
		let key = b"nonexistent";

		let to_drop = find_keys_to_drop(&storage, table, key, None).unwrap();
		assert!(to_drop.is_empty());
	}
}
