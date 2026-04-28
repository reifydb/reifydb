// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! CDC Storage abstraction.
//!
//! This module provides a trait for CDC storage backends and an in-memory implementation.
//! CDC storage is independent of MVCC versioned storage - it uses simple BE u64 keys
//! (CommitVersion) and stores fully resolved values.

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

/// Result type for CDC storage operations.
pub type CdcStorageResult<T> = Result<T, CdcError>;

enum ScanContinuation {
	Done(CommitVersion),
	Continue(Bound<CommitVersion>),
}

/// Walk a non-empty batch looking for the first entry with `timestamp >= cutoff`.
/// Returns `Some(version)` on hit; `None` if every entry is still older than
/// the cutoff (the caller should fetch the next batch or terminate).
#[inline]
fn scan_batch_for_cutoff(items: &[Cdc], cutoff: DateTime) -> Option<CommitVersion> {
	for cdc in items {
		if cdc.timestamp >= cutoff {
			return Some(cdc.version);
		}
	}
	None
}

/// Decide what to do after a batch with no cutoff hit:
/// - `Done(max + 1)` if the batch was the last one (no `has_more`).
/// - `Continue(Excluded(last_version))` to fetch the next batch.
#[inline]
fn next_start_after_batch(batch: &CdcBatch, max: CommitVersion) -> ScanContinuation {
	if !batch.has_more {
		return ScanContinuation::Done(CommitVersion(max.0.saturating_add(1)));
	}
	let last = batch.items.last().unwrap().version;
	ScanContinuation::Continue(Bound::Excluded(last))
}

/// Normalize a half-open range request from the trait API into an inclusive
/// `[lo, hi]` pair. Returns `None` if the range is empty (lo > hi after the
/// Excluded/Unbounded substitutions are applied).
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

/// Information about a dropped CDC entry for stats tracking.
#[derive(Debug, Clone)]
pub struct DroppedCdcEntry {
	pub key: EncodedKey,
	pub value_bytes: u64,
}

/// Result of a drop_before operation.
#[derive(Debug, Clone, Default)]
pub struct DropBeforeResult {
	pub count: usize,
	pub entries: Vec<DroppedCdcEntry>,
}

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

	/// Delete all CDC entries with version strictly less than the given version.
	/// Returns the count and entry information for stats tracking.
	fn drop_before(&self, version: CommitVersion) -> CdcStorageResult<DropBeforeResult>;

	/// Find the smallest CDC version V such that `cdc[V].timestamp >= cutoff`.
	///
	/// Returns `Some(V)` if such an entry exists. If every stored entry has
	/// `timestamp < cutoff`, returns `Some(max_version + 1)` so callers can pass it
	/// straight to `drop_before` to evict everything older than the cutoff.
	/// Returns `None` if storage is empty.
	///
	/// Default impl scans `read_range` from the smallest version upward in batches of
	/// 256, stopping at the first entry whose timestamp is `>= cutoff`. Backends with
	/// an indexed timestamp column should override.
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

	/// Convenience method with default batch size.
	fn range(&self, start: Bound<CommitVersion>, end: Bound<CommitVersion>) -> CdcStorageResult<CdcBatch> {
		self.read_range(start, end, 1024)
	}

	/// Scan all CDC entries with the given batch size.
	fn scan(&self, batch_size: u64) -> CdcStorageResult<CdcBatch> {
		self.read_range(Bound::Unbounded, Bound::Unbounded, batch_size)
	}
}

/// Blanket implementation for CdcStore compatibility with existing traits.
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

/// CDC storage abstraction enum.
///
/// Provides a unified interface over different CDC storage backends.
#[derive(Clone)]
pub enum CdcStore {
	/// In-memory CDC storage backed by a BTreeMap.
	Memory(MemoryCdcStorage),
	/// SQLite-backed CDC storage for persistent durability.
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	Sqlite(sqlite::storage::SqliteCdcStorage),
}

impl CdcStore {
	/// Create an in-memory CDC store.
	pub fn memory() -> Self {
		Self::Memory(MemoryCdcStorage::new())
	}

	/// Create a SQLite-backed CDC store with the given configuration.
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(config: SqliteConfig) -> Self {
		Self::Sqlite(sqlite::storage::SqliteCdcStorage::new(config))
	}

	/// Write a CDC entry.
	pub fn write(&self, cdc: &Cdc) -> CdcStorageResult<()> {
		match self {
			Self::Memory(s) => s.write(cdc),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.write(cdc),
		}
	}

	/// Read a CDC entry by version.
	pub fn read(&self, version: CommitVersion) -> CdcStorageResult<Option<Cdc>> {
		match self {
			Self::Memory(s) => s.read(version),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.read(version),
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
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.read_range(start, end, batch_size),
		}
	}

	/// Count CDC changes at a specific version.
	pub fn count(&self, version: CommitVersion) -> CdcStorageResult<usize> {
		match self {
			Self::Memory(s) => s.count(version),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.count(version),
		}
	}

	/// Get the minimum (oldest) CDC version in storage.
	pub fn min_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		match self {
			Self::Memory(s) => s.min_version(),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.min_version(),
		}
	}

	/// Get the maximum (newest) CDC version in storage.
	pub fn max_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		match self {
			Self::Memory(s) => s.max_version(),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.max_version(),
		}
	}

	/// Delete all CDC entries with version strictly less than the given version.
	pub fn delete_before(&self, version: CommitVersion) -> CdcStorageResult<DropBeforeResult> {
		match self {
			Self::Memory(s) => s.drop_before(version),
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			Self::Sqlite(s) => s.drop_before(version),
		}
	}

	/// Find the smallest CDC version V such that `cdc[V].timestamp >= cutoff`.
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
