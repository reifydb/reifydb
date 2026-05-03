// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Pluggable backing store for the CDC log. The in-memory implementation is the testing default; SQLite is the
//! durable default for production deployments. Both implement the same trait surface so the producer and consumer
//! sides are agnostic to which is configured.

pub mod memory;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub mod sqlite;

use std::{collections::Bound, sync};

use memory::MemoryCdcStorage;
use reifydb_core::{
	common::CommitVersion,
	encoded::key::EncodedKey,
	interface::cdc::{Cdc, CdcBatch},
};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::SqliteConfig;
use reifydb_type::value::datetime::DateTime;

use crate::error::CdcError;

pub type CdcStorageResult<T> = Result<T, CdcError>;

enum ScanContinuation {
	Done(CommitVersion),
	Continue(Bound<CommitVersion>),
}

#[inline]
fn scan_batch_for_cutoff(items: &[Cdc], cutoff: DateTime) -> Option<CommitVersion> {
	for cdc in items {
		if cdc.timestamp >= cutoff {
			return Some(cdc.version);
		}
	}
	None
}

#[inline]
fn next_start_after_batch(batch: &CdcBatch, max: CommitVersion) -> ScanContinuation {
	if !batch.has_more {
		return ScanContinuation::Done(CommitVersion(max.0.saturating_add(1)));
	}
	let last = batch.items.last().unwrap().version;
	ScanContinuation::Continue(Bound::Excluded(last))
}

#[inline]
pub(crate) fn normalize_range_inclusive(
	start: Bound<CommitVersion>,
	end: Bound<CommitVersion>,
) -> Option<(CommitVersion, CommitVersion)> {
	let lo_inc = match start {
		Bound::Included(v) => v,
		Bound::Excluded(v) => CommitVersion(v.0.saturating_add(1)),
		Bound::Unbounded => CommitVersion(0),
	};
	let hi_inc = match end {
		Bound::Included(v) => v,
		Bound::Excluded(v) => CommitVersion(v.0.saturating_sub(1)),
		Bound::Unbounded => CommitVersion(u64::MAX),
	};
	if lo_inc > hi_inc {
		None
	} else {
		Some((lo_inc, hi_inc))
	}
}

#[derive(Debug, Clone)]
pub struct DroppedCdcEntry {
	pub key: EncodedKey,
	pub value_bytes: u64,
}

#[derive(Debug, Clone, Default)]
pub struct DropBeforeResult {
	pub count: usize,
	pub entries: Vec<DroppedCdcEntry>,
}

pub trait CdcStorage: Send + Sync + Clone + 'static {
	fn write(&self, cdc: &Cdc) -> CdcStorageResult<()>;

	fn read(&self, version: CommitVersion) -> CdcStorageResult<Option<Cdc>>;

	fn read_range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> CdcStorageResult<CdcBatch>;

	fn count(&self, version: CommitVersion) -> CdcStorageResult<usize>;

	fn min_version(&self) -> CdcStorageResult<Option<CommitVersion>>;

	fn max_version(&self) -> CdcStorageResult<Option<CommitVersion>>;

	fn exists(&self, version: CommitVersion) -> CdcStorageResult<bool> {
		Ok(self.read(version)?.is_some())
	}

	fn drop_before(&self, version: CommitVersion) -> CdcStorageResult<DropBeforeResult>;

	fn find_ttl_cutoff(&self, cutoff: DateTime) -> CdcStorageResult<Option<CommitVersion>> {
		let Some(min) = self.min_version()? else {
			return Ok(None);
		};
		let Some(max) = self.max_version()? else {
			return Ok(None);
		};

		let mut next_start = Bound::Included(min);
		loop {
			let batch = self.read_range(next_start, Bound::Unbounded, 256)?;
			if batch.items.is_empty() {
				return Ok(Some(CommitVersion(max.0.saturating_add(1))));
			}
			if let Some(version) = scan_batch_for_cutoff(&batch.items, cutoff) {
				return Ok(Some(version));
			}
			match next_start_after_batch(&batch, max) {
				ScanContinuation::Done(v) => return Ok(Some(v)),
				ScanContinuation::Continue(start) => next_start = start,
			}
		}
	}

	fn range(&self, start: Bound<CommitVersion>, end: Bound<CommitVersion>) -> CdcStorageResult<CdcBatch> {
		self.read_range(start, end, 1024)
	}

	fn scan(&self, batch_size: u64) -> CdcStorageResult<CdcBatch> {
		self.read_range(Bound::Unbounded, Bound::Unbounded, batch_size)
	}
}

impl<T: CdcStorage> CdcStorage for sync::Arc<T> {
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

	fn drop_before(&self, version: CommitVersion) -> CdcStorageResult<DropBeforeResult> {
		(**self).drop_before(version)
	}

	fn find_ttl_cutoff(&self, cutoff: DateTime) -> CdcStorageResult<Option<CommitVersion>> {
		(**self).find_ttl_cutoff(cutoff)
	}
}

#[derive(Clone)]
pub enum CdcStore {
	Memory(MemoryCdcStorage),

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	Sqlite(sqlite::storage::SqliteCdcStorage),
}

impl CdcStore {
	pub fn memory() -> Self {
		Self::Memory(MemoryCdcStorage::new())
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(config: SqliteConfig) -> Self {
		Self::Sqlite(sqlite::storage::SqliteCdcStorage::new(config))
	}

	pub fn write(&self, cdc: &Cdc) -> CdcStorageResult<()> {
		match self {
			Self::Memory(s) => s.write(cdc),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.write(cdc),
		}
	}

	pub fn read(&self, version: CommitVersion) -> CdcStorageResult<Option<Cdc>> {
		match self {
			Self::Memory(s) => s.read(version),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.read(version),
		}
	}

	pub fn read_range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> CdcStorageResult<CdcBatch> {
		match self {
			Self::Memory(s) => s.read_range(start, end, batch_size),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.read_range(start, end, batch_size),
		}
	}

	pub fn count(&self, version: CommitVersion) -> CdcStorageResult<usize> {
		match self {
			Self::Memory(s) => s.count(version),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.count(version),
		}
	}

	pub fn min_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		match self {
			Self::Memory(s) => s.min_version(),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.min_version(),
		}
	}

	pub fn max_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		match self {
			Self::Memory(s) => s.max_version(),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.max_version(),
		}
	}

	pub fn delete_before(&self, version: CommitVersion) -> CdcStorageResult<DropBeforeResult> {
		match self {
			Self::Memory(s) => s.drop_before(version),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.drop_before(version),
		}
	}

	pub fn find_ttl_cutoff(&self, cutoff: DateTime) -> CdcStorageResult<Option<CommitVersion>> {
		match self {
			Self::Memory(s) => s.find_ttl_cutoff(cutoff),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.find_ttl_cutoff(cutoff),
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

	fn drop_before(&self, version: CommitVersion) -> CdcStorageResult<DropBeforeResult> {
		CdcStore::delete_before(self, version)
	}

	fn find_ttl_cutoff(&self, cutoff: DateTime) -> CdcStorageResult<Option<CommitVersion>> {
		CdcStore::find_ttl_cutoff(self, cutoff)
	}
}
