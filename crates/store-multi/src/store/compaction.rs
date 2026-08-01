// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, interface::store::EntryKind};

use crate::{Result, tier::TierStorage};

pub(crate) fn find_superseded_versions<S: TierStorage>(
	storage: &S,
	table: EntryKind,
	key: &[u8],
	pending_version: Option<CommitVersion>,
) -> Result<Vec<CommitVersion>> {
	let mut versions: Vec<CommitVersion> =
		storage.get_all_versions(table, key)?.into_iter().map(|(version, _)| version).collect();

	if let Some(pending_ver) = pending_version
		&& !versions.contains(&pending_ver)
	{
		versions.push(pending_ver);
	}

	versions.sort_by(|a, b| b.cmp(a));

	let mut superseded = Vec::with_capacity(versions.len().saturating_sub(1));

	for (idx, entry_version) in versions.into_iter().enumerate() {
		let should_drop = idx > 0;

		if should_drop {
			if Some(entry_version) == pending_version {
				continue;
			}

			superseded.push(entry_version);
		}
	}

	Ok(superseded)
}

#[cfg(test)]
pub mod tests {
	use std::collections::HashMap;

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_value::util::cowvec::CowVec;

	use super::*;
	use crate::tier::commit::buffer::MultiCommitBufferTier;

	fn setup_versioned_entries(storage: &MultiCommitBufferTier, table: EntryKind, key: &[u8], versions: &[u64]) {
		for v in versions {
			let entries = vec![(EncodedKey::new(key), Some(CowVec::new(vec![*v as u8])))];
			storage.set(CommitVersion(*v), HashMap::from([(table, entries)])).unwrap();
		}
	}

	fn extract_dropped_versions(versions: &[CommitVersion]) -> Vec<u64> {
		versions.iter().map(|version| version.0).collect()
	}

	#[test]
	fn test_drop_historical_versions() {
		let storage = MultiCommitBufferTier::memory();
		let table = EntryKind::Multi;
		let key = b"test_key";

		setup_versioned_entries(&storage, table, key, &[1, 5, 10, 20, 100]);

		// Only the newest version survives; everything it supersedes is droppable.
		let to_drop = find_superseded_versions(&storage, table, key, None).unwrap();

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

		setup_versioned_entries(&storage, table, key, &[1, 5, 10]);

		// A pending version supersedes the stored ones, so it must not itself be scheduled for drop.
		let to_drop = find_superseded_versions(&storage, table, key, Some(CommitVersion(20))).unwrap();

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

		let to_drop = find_superseded_versions(&storage, table, key, None).unwrap();
		assert!(to_drop.is_empty());
	}

	#[test]
	fn test_empty_storage() {
		let storage = MultiCommitBufferTier::memory();
		let table = EntryKind::Multi;
		let key = b"nonexistent";

		let to_drop = find_superseded_versions(&storage, table, key, None).unwrap();
		assert!(to_drop.is_empty());
	}
}
