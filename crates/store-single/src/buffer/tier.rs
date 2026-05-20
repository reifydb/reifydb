// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_core::encoded::key::EncodedKey;
use reifydb_type::{Result, util::cowvec::CowVec};

use super::memory::storage::MemoryPrimitiveStorage;
use crate::tier::{RangeBatch, RangeCursor, TierBackend, TierStorage};

#[derive(Clone)]
#[repr(u8)]
pub enum SingleBufferTier {
	Memory(MemoryPrimitiveStorage) = 0,
}

impl SingleBufferTier {
	pub fn memory() -> Self {
		Self::Memory(MemoryPrimitiveStorage::new())
	}
}

impl TierStorage for SingleBufferTier {
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

impl TierBackend for SingleBufferTier {}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_memory_backend() {
		let storage = SingleBufferTier::memory();

		storage.set(vec![(EncodedKey::new(b"key".to_vec()), Some(CowVec::new(b"value".to_vec())))]).unwrap();
		assert_eq!(storage.get(b"key").unwrap().as_deref(), Some(b"value".as_slice()));
	}

	#[test]
	fn test_range_next_memory() {
		let storage = SingleBufferTier::memory();

		storage.set(vec![
			(EncodedKey::new(b"a".to_vec()), Some(CowVec::new(b"1".to_vec()))),
			(EncodedKey::new(b"b".to_vec()), Some(CowVec::new(b"2".to_vec()))),
			(EncodedKey::new(b"c".to_vec()), Some(CowVec::new(b"3".to_vec()))),
		])
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage.range_next(&mut cursor, Bound::Unbounded, Bound::Unbounded, 100).unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.exhausted);
	}
}
