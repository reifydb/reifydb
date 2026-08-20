// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::metrics::{collect::MetricsCollector, sample::MetricsSample};
use reifydb_value::{Result, byte_size::ByteSize, count::Count, util::cowvec::CowVec};

use super::memory::storage::MemoryRowStorage;
use crate::tier::{RangeBatch, RangeCursor, TierBackend, TierStorage};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SingleCommitMetrics {
	pub resident_entries: Count,
	pub resident_bytes: ByteSize,
}

#[derive(Clone)]
#[repr(u8)]
pub enum SingleCommitBufferTier {
	Memory(MemoryRowStorage) = 0,
}

impl SingleCommitBufferTier {
	pub fn memory() -> Self {
		Self::Memory(MemoryRowStorage::new())
	}

	pub fn memory_usage(&self) -> (usize, usize) {
		match self {
			Self::Memory(s) => s.memory_usage(),
		}
	}

	pub fn metrics(&self) -> SingleCommitMetrics {
		let (entries, bytes) = self.memory_usage();
		SingleCommitMetrics {
			resident_entries: Count::new(entries as u64),
			resident_bytes: ByteSize::from_bytes(bytes as u64),
		}
	}
}

impl MetricsCollector for SingleCommitBufferTier {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		let (entries, bytes) = self.memory_usage();
		out.push(MetricsSample::count("store_single::buffer", "resident_entries", entries as u64));
		out.push(MetricsSample::heap(
			"store_single::buffer",
			"resident_bytes",
			ByteSize::from_bytes(bytes as u64),
		));
	}
}

impl TierStorage for SingleCommitBufferTier {
	#[inline]
	fn get(&self, key: &[u8]) -> Result<Option<CowVec<u8>>> {
		match self {
			Self::Memory(s) => s.get(key),
		}
	}

	#[inline]
	fn contains(&self, key: &[u8]) -> Result<bool> {
		match self {
			Self::Memory(s) => s.contains(key),
		}
	}

	#[inline]
	fn get_with_tombstone(&self, key: &[u8]) -> Result<Option<Option<CowVec<u8>>>> {
		match self {
			Self::Memory(s) => s.get_with_tombstone(key),
		}
	}

	#[inline]
	fn set(&self, entries: Vec<(EncodedKey, Option<CowVec<u8>>)>) -> Result<()> {
		match self {
			Self::Memory(s) => s.set(entries),
		}
	}

	#[inline]
	fn range_next(
		&self,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<RangeBatch> {
		match self {
			Self::Memory(s) => s.range_next(cursor, start, end, batch_size),
		}
	}

	#[inline]
	fn range_rev_next(
		&self,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<RangeBatch> {
		match self {
			Self::Memory(s) => s.range_rev_next(cursor, start, end, batch_size),
		}
	}

	#[inline]
	fn ensure_table(&self) -> Result<()> {
		match self {
			Self::Memory(s) => s.ensure_table(),
		}
	}

	#[inline]
	fn clear_table(&self) -> Result<()> {
		match self {
			Self::Memory(s) => s.clear_table(),
		}
	}
}

impl TierBackend for SingleCommitBufferTier {}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_memory_backend() {
		let storage = SingleCommitBufferTier::memory();

		storage.set(vec![(EncodedKey::new(b"key"), Some(CowVec::new(b"value".to_vec())))]).unwrap();
		assert_eq!(storage.get(b"key").unwrap().as_deref(), Some(b"value".as_slice()));
	}

	#[test]
	fn test_range_next_memory() {
		let storage = SingleCommitBufferTier::memory();

		storage.set(vec![
			(EncodedKey::new(b"a"), Some(CowVec::new(b"1".to_vec()))),
			(EncodedKey::new(b"b"), Some(CowVec::new(b"2".to_vec()))),
			(EncodedKey::new(b"c"), Some(CowVec::new(b"3".to_vec()))),
		])
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage.range_next(&mut cursor, Bound::Unbounded, Bound::Unbounded, 100).unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.exhausted);
	}
}
