// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Bound};

use reifydb_core::{common::CommitVersion, encoded::key::EncodedKey, interface::store::EntryKind};
use reifydb_type::{Result, util::cowvec::CowVec};

use super::memory::storage::MemoryPrimitiveStorage;
use crate::tier::{HistoricalCursor, RangeBatch, RangeCursor, TierBackend, TierBatch, TierStorage, VersionedGetResult};

#[derive(Clone)]
#[repr(u8)]
pub enum MultiBufferTier {
	Memory(MemoryPrimitiveStorage) = 0,
}

impl MultiBufferTier {
	pub fn memory() -> Self {
		Self::Memory(MemoryPrimitiveStorage::new())
	}
}

impl MultiBufferTier {
	pub fn maintenance(&self) {
		match self {
			Self::Memory(_) => {}
		}
	}

	pub fn count_current(&self, table: EntryKind) -> Result<u64> {
		match self {
			Self::Memory(s) => s.count_current(table),
		}
	}

	pub fn count_historical(&self, table: EntryKind) -> Result<u64> {
		match self {
			Self::Memory(s) => s.count_historical(table),
		}
	}

	pub fn list_all_entry_kinds(&self) -> Result<Vec<EntryKind>> {
		match self {
			Self::Memory(s) => s.list_all_entry_kinds(),
		}
	}
}

impl TierStorage for MultiBufferTier {
	#[inline]
	fn get(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<VersionedGetResult> {
		match self {
			Self::Memory(s) => s.get(table, key, version),
		}
	}

	#[inline]
	fn contains(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<bool> {
		match self {
			Self::Memory(s) => s.contains(table, key, version),
		}
	}

	#[inline]
	fn set(&self, version: CommitVersion, batches: TierBatch) -> Result<()> {
		match self {
			Self::Memory(s) => s.set(version, batches),
		}
	}

	#[inline]
	fn range_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		version: CommitVersion,
		batch_size: usize,
	) -> Result<RangeBatch> {
		match self {
			Self::Memory(s) => s.range_next(table, cursor, start, end, version, batch_size),
		}
	}

	#[inline]
	fn range_rev_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		version: CommitVersion,
		batch_size: usize,
	) -> Result<RangeBatch> {
		match self {
			Self::Memory(s) => s.range_rev_next(table, cursor, start, end, version, batch_size),
		}
	}

	#[inline]
	fn ensure_table(&self, table: EntryKind) -> Result<()> {
		match self {
			Self::Memory(s) => s.ensure_table(table),
		}
	}

	#[inline]
	fn clear_table(&self, table: EntryKind) -> Result<()> {
		match self {
			Self::Memory(s) => s.clear_table(table),
		}
	}

	#[inline]
	fn drop(&self, batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>>) -> Result<()> {
		match self {
			Self::Memory(s) => s.drop(batches),
		}
	}

	#[inline]
	fn get_all_versions(&self, table: EntryKind, key: &[u8]) -> Result<Vec<(CommitVersion, Option<CowVec<u8>>)>> {
		match self {
			Self::Memory(s) => s.get_all_versions(table, key),
		}
	}

	#[inline]
	fn scan_historical_below(
		&self,
		table: EntryKind,
		cutoff: CommitVersion,
		cursor: &mut HistoricalCursor,
		batch_size: usize,
	) -> Result<Vec<(EncodedKey, CommitVersion)>> {
		match self {
			Self::Memory(s) => s.scan_historical_below(table, cutoff, cursor, batch_size),
		}
	}
}

impl TierBackend for MultiBufferTier {}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_memory_backend() {
		let storage = MultiBufferTier::memory();

		let key = EncodedKey::new(b"key".to_vec());
		let version = CommitVersion(1);

		storage.set(
			version,
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"value".to_vec())))])]),
		)
		.unwrap();
		assert_eq!(
			storage.get(EntryKind::Multi, &key, version).unwrap().value().as_deref(),
			Some(b"value".as_slice())
		);
	}

	#[test]
	fn test_range_next_memory() {
		let storage = MultiBufferTier::memory();

		let version = CommitVersion(1);
		storage.set(
			version,
			HashMap::from([(
				EntryKind::Multi,
				vec![
					(EncodedKey::new(b"a".to_vec()), Some(CowVec::new(b"1".to_vec()))),
					(EncodedKey::new(b"b".to_vec()), Some(CowVec::new(b"2".to_vec()))),
					(EncodedKey::new(b"c".to_vec()), Some(CowVec::new(b"3".to_vec()))),
				],
			)]),
		)
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 100)
			.unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.exhausted);
	}
}
