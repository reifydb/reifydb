// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Bound};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	interface::store::EntryKind,
	metrics::{collect::MetricsCollector, sample::MetricsSample},
};
use reifydb_value::{Result, byte_size::ByteSize, count::Count, util::cowvec::CowVec};

use crate::{
	MultiVersionScope,
	tier::{
		HistoricalCursor, RangeBatch, RangeCursor, TierBackend, TierBatch, TierStorage, VersionedGetResult,
		commit::memory::storage::{EvictedVersion, MemoryRowStorage},
	},
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MultiCommitMetrics {
	pub current_bytes: ByteSize,
	pub historical_bytes: ByteSize,
	pub table_count: Count,
	pub current_entries: Count,
}

#[derive(Clone)]
#[repr(u8)]
pub enum MultiCommitBufferTier {
	Memory(MemoryRowStorage) = 0,
}

impl MultiCommitBufferTier {
	pub fn memory() -> Self {
		Self::Memory(MemoryRowStorage::new())
	}
}

impl MultiCommitBufferTier {
	pub fn maintenance(&self) {
		match self {
			Self::Memory(_) => {}
		}
	}

	pub fn estimated_current_count(&self, table: EntryKind) -> Result<u64> {
		match self {
			Self::Memory(s) => s.estimated_current_count(table),
		}
	}

	pub fn list_all_entry_kinds(&self) -> Result<Vec<EntryKind>> {
		match self {
			Self::Memory(s) => s.list_all_entry_kinds(),
		}
	}

	pub fn list_entry_kinds_by_oldest_pending(&self) -> Result<Vec<EntryKind>> {
		match self {
			Self::Memory(s) => s.list_entry_kinds_by_oldest_pending(),
		}
	}

	pub fn oldest_pending_for(&self, kind: EntryKind) -> Option<CommitVersion> {
		match self {
			Self::Memory(s) => s.oldest_pending_for(kind),
		}
	}

	pub fn oldest_pending_version(&self) -> Option<CommitVersion> {
		match self {
			Self::Memory(s) => s.oldest_pending_version(),
		}
	}

	pub fn current_resident_bytes(&self) -> ByteSize {
		match self {
			Self::Memory(s) => s.current_resident_bytes(),
		}
	}

	pub fn historical_resident_bytes(&self) -> ByteSize {
		match self {
			Self::Memory(s) => s.historical_resident_bytes(),
		}
	}

	pub fn metrics(&self) -> MultiCommitMetrics {
		let kinds = self.list_all_entry_kinds().unwrap_or_default();
		let current_entries: u64 =
			kinds.iter().map(|kind| self.estimated_current_count(*kind).unwrap_or(0)).sum();
		MultiCommitMetrics {
			current_bytes: self.current_resident_bytes(),
			historical_bytes: self.historical_resident_bytes(),
			table_count: Count::new(kinds.len() as u64),
			current_entries: Count::new(current_entries),
		}
	}

	#[inline]
	pub fn compact(
		&self,
		batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>>,
	) -> Result<Vec<EvictedVersion>> {
		match self {
			Self::Memory(s) => s.compact(batches),
		}
	}

	#[inline]
	pub fn get_all_versions(
		&self,
		table: EntryKind,
		key: &[u8],
	) -> Result<Vec<(CommitVersion, Option<CowVec<u8>>)>> {
		match self {
			Self::Memory(s) => s.get_all_versions(table, key),
		}
	}

	#[inline]
	pub fn scan_historical_below(
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

impl MetricsCollector for MultiCommitBufferTier {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		out.push(MetricsSample::heap("commit_buffer", "current_bytes", self.current_resident_bytes()));
		out.push(MetricsSample::heap("commit_buffer", "historical_bytes", self.historical_resident_bytes()));
		let kinds = self.list_all_entry_kinds().unwrap_or_default();
		out.push(MetricsSample::count("commit_buffer", "table_count", kinds.len() as u64));
		let current_entries: u64 =
			kinds.iter().map(|kind| self.estimated_current_count(*kind).unwrap_or(0)).sum();
		out.push(MetricsSample::count("commit_buffer", "current_entries", current_entries));
	}
}

impl TierStorage for MultiCommitBufferTier {
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
		scope: MultiVersionScope,
		batch_size: usize,
	) -> Result<RangeBatch> {
		match self {
			Self::Memory(s) => s.range_next(table, cursor, start, end, scope, batch_size),
		}
	}

	#[inline]
	fn range_rev_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> Result<RangeBatch> {
		match self {
			Self::Memory(s) => s.range_rev_next(table, cursor, start, end, scope, batch_size),
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
}

impl TierBackend for MultiCommitBufferTier {}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_memory_backend() {
		let storage = MultiCommitBufferTier::memory();

		let key = EncodedKey::new(b"key");
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
		let storage = MultiCommitBufferTier::memory();

		let version = CommitVersion(1);
		storage.set(
			version,
			HashMap::from([(
				EntryKind::Multi,
				vec![
					(EncodedKey::new(b"a"), Some(CowVec::new(b"1".to_vec()))),
					(EncodedKey::new(b"b"), Some(CowVec::new(b"2".to_vec()))),
					(EncodedKey::new(b"c"), Some(CowVec::new(b"3".to_vec()))),
				],
			)]),
		)
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: version,
				},
				100,
			)
			.unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.is_exhausted());
	}
}
