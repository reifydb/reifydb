// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! CDC Storage abstraction.
//!
//! This module provides a trait for CDC storage backends and an in-memory implementation.
//! CDC storage is independent of MVCC versioned storage - it uses simple BE u64 keys
//! (CommitVersion) and stores fully resolved values.

mod memory;

pub use memory::MemoryCdcStorage;

use std::collections::Bound;

use reifydb_core::{CommitVersion, interface::{Cdc, CdcBatch}};

use crate::error::CdcError;

/// Result type for CDC storage operations.
pub type CdcStorageResult<T> = Result<T, CdcError>;

/// Trait for CDC storage backends.
///
/// CDC storage stores fully resolved change data capture entries keyed by CommitVersion.
/// Unlike MVCC storage, CDC entries are immutable and use simple version keys.
///
/// Implementations must be thread-safe and cloneable to support concurrent access
/// from multiple consumers and the CDC generation pipeline.
pub trait CdcStorage: Send + Sync + Clone + 'static {
	/// Write a CDC entry (fully resolved values).
	///
	/// The entry is keyed by its version. If an entry already exists at this version,
	/// it will be overwritten (this should only happen during recovery/replay).
	fn write(&self, cdc: &Cdc) -> CdcStorageResult<()>;

	/// Read a CDC entry by version.
	///
	/// Returns `None` if no entry exists at the given version.
	fn read(&self, version: CommitVersion) -> CdcStorageResult<Option<Cdc>>;

	/// Read CDC entries in a version range.
	///
	/// Returns entries in ascending version order up to `batch_size` entries.
	/// The `CdcBatch.has_more` flag indicates if more entries exist beyond the batch.
	fn read_range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> CdcStorageResult<CdcBatch>;

	/// Count CDC changes at a specific version.
	///
	/// Returns 0 if no entry exists at the given version.
	fn count(&self, version: CommitVersion) -> CdcStorageResult<usize>;

	/// Get the minimum (oldest) CDC version in storage.
	///
	/// Returns `None` if storage is empty.
	fn min_version(&self) -> CdcStorageResult<Option<CommitVersion>>;

	/// Get the maximum (newest) CDC version in storage.
	///
	/// Returns `None` if storage is empty.
	fn max_version(&self) -> CdcStorageResult<Option<CommitVersion>>;

	/// Check if a CDC entry exists at the given version.
	fn exists(&self, version: CommitVersion) -> CdcStorageResult<bool> {
		Ok(self.read(version)?.is_some())
	}

	/// Convenience method with default batch size.
	fn range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
	) -> CdcStorageResult<CdcBatch> {
		self.read_range(start, end, 1024)
	}

	/// Scan all CDC entries with the given batch size.
	fn scan(&self, batch_size: u64) -> CdcStorageResult<CdcBatch> {
		self.read_range(Bound::Unbounded, Bound::Unbounded, batch_size)
	}
}

/// Blanket implementation for CdcStore compatibility with existing traits.
impl<T: CdcStorage> CdcStorage for std::sync::Arc<T> {
	fn write(&self, cdc: &Cdc) -> CdcStorageResult<()> {
		(**self).write(cdc)
	}

	fn read(&self, version: CommitVersion) -> CdcStorageResult<Option<Cdc>> {
		(**self).read(version)
	}

	fn read_range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> CdcStorageResult<CdcBatch> {
		(**self).read_range(start, end, batch_size)
	}

	fn count(&self, version: CommitVersion) -> CdcStorageResult<usize> {
		(**self).count(version)
	}

	fn min_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		(**self).min_version()
	}

	fn max_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		(**self).max_version()
	}
}

/// CDC storage abstraction enum.
///
/// Provides a unified interface over different CDC storage backends.
/// Currently supports in-memory storage, with room for future backends.
#[derive(Clone)]
pub enum CdcStore {
	/// In-memory CDC storage backed by a BTreeMap.
	Memory(MemoryCdcStorage),
}

impl CdcStore {
	/// Create an in-memory CDC store.
	pub fn memory() -> Self {
		Self::Memory(MemoryCdcStorage::new())
	}

	/// Write a CDC entry.
	pub fn write(&self, cdc: &Cdc) -> CdcStorageResult<()> {
		match self {
			Self::Memory(s) => s.write(cdc),
		}
	}

	/// Read a CDC entry by version.
	pub fn read(&self, version: CommitVersion) -> CdcStorageResult<Option<Cdc>> {
		match self {
			Self::Memory(s) => s.read(version),
		}
	}

	/// Read CDC entries in a version range.
	pub fn read_range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> CdcStorageResult<CdcBatch> {
		match self {
			Self::Memory(s) => s.read_range(start, end, batch_size),
		}
	}

	/// Count CDC changes at a specific version.
	pub fn count(&self, version: CommitVersion) -> CdcStorageResult<usize> {
		match self {
			Self::Memory(s) => s.count(version),
		}
	}

	/// Get the minimum (oldest) CDC version in storage.
	pub fn min_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		match self {
			Self::Memory(s) => s.min_version(),
		}
	}

	/// Get the maximum (newest) CDC version in storage.
	pub fn max_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		match self {
			Self::Memory(s) => s.max_version(),
		}
	}
}

impl CdcStorage for CdcStore {
	fn write(&self, cdc: &Cdc) -> CdcStorageResult<()> {
		CdcStore::write(self, cdc)
	}

	fn read(&self, version: CommitVersion) -> CdcStorageResult<Option<Cdc>> {
		CdcStore::read(self, version)
	}

	fn read_range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> CdcStorageResult<CdcBatch> {
		CdcStore::read_range(self, start, end, batch_size)
	}

	fn count(&self, version: CommitVersion) -> CdcStorageResult<usize> {
		CdcStore::count(self, version)
	}

	fn min_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		CdcStore::min_version(self)
	}

	fn max_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		CdcStore::max_version(self)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use reifydb_core::{EncodedKey, interface::{CdcChange, CdcSequencedChange}, value::encoded::EncodedValues};

	fn create_test_cdc(version: u64, num_changes: usize) -> Cdc {
		let changes: Vec<CdcSequencedChange> = (0..num_changes)
			.map(|i| CdcSequencedChange {
				sequence: i as u16 + 1,
				change: CdcChange::Insert {
					key: EncodedKey::new(vec![i as u8]),
					post: EncodedValues(reifydb_core::CowVec::new(vec![])),
				},
			})
			.collect();

		Cdc::new(CommitVersion(version), 12345, changes)
	}

	#[test]
	fn test_memory_storage_write_read() {
		let storage = MemoryCdcStorage::new();
		let cdc = create_test_cdc(1, 3);

		storage.write(&cdc).unwrap();

		let read_cdc = storage.read(CommitVersion(1)).unwrap();
		assert!(read_cdc.is_some());
		let read_cdc = read_cdc.unwrap();
		assert_eq!(read_cdc.version, CommitVersion(1));
		assert_eq!(read_cdc.changes.len(), 3);
	}

	#[test]
	fn test_memory_storage_read_nonexistent() {
		let storage = MemoryCdcStorage::new();
		let result = storage.read(CommitVersion(999)).unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_memory_storage_range() {
		let storage = MemoryCdcStorage::new();

		for v in 1..=10 {
			storage.write(&create_test_cdc(v, 1)).unwrap();
		}

		// Read range [3, 7]
		let batch = storage
			.read_range(
				Bound::Included(CommitVersion(3)),
				Bound::Included(CommitVersion(7)),
				100,
			)
			.unwrap();

		assert_eq!(batch.items.len(), 5);
		assert!(!batch.has_more);
		assert_eq!(batch.items[0].version, CommitVersion(3));
		assert_eq!(batch.items[4].version, CommitVersion(7));
	}

	#[test]
	fn test_memory_storage_range_batch_size() {
		let storage = MemoryCdcStorage::new();

		for v in 1..=10 {
			storage.write(&create_test_cdc(v, 1)).unwrap();
		}

		// Read with batch size 3
		let batch = storage
			.read_range(Bound::Unbounded, Bound::Unbounded, 3)
			.unwrap();

		assert_eq!(batch.items.len(), 3);
		assert!(batch.has_more);
	}

	#[test]
	fn test_memory_storage_count() {
		let storage = MemoryCdcStorage::new();
		let cdc = create_test_cdc(1, 5);
		storage.write(&cdc).unwrap();

		assert_eq!(storage.count(CommitVersion(1)).unwrap(), 5);
		assert_eq!(storage.count(CommitVersion(2)).unwrap(), 0);
	}

	#[test]
	fn test_memory_storage_min_max_version() {
		let storage = MemoryCdcStorage::new();

		assert!(storage.min_version().unwrap().is_none());
		assert!(storage.max_version().unwrap().is_none());

		storage.write(&create_test_cdc(5, 1)).unwrap();
		storage.write(&create_test_cdc(3, 1)).unwrap();
		storage.write(&create_test_cdc(8, 1)).unwrap();

		assert_eq!(storage.min_version().unwrap(), Some(CommitVersion(3)));
		assert_eq!(storage.max_version().unwrap(), Some(CommitVersion(8)));
	}
}
