// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::Bound;

use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, CdcBatch},
};
use reifydb_value::value::datetime::DateTime;

use super::{CdcStorage, CdcStorageResult, DropBeforeResult, normalize_range_inclusive, recent_cache::RecentCdcCache};

#[derive(Clone)]
pub struct CachedCdcStorage<S: CdcStorage> {
	inner: S,
	cache: RecentCdcCache,
}

impl<S: CdcStorage> CachedCdcStorage<S> {
	pub fn new(inner: S, capacity: usize) -> Self {
		Self {
			inner,
			cache: RecentCdcCache::new(capacity),
		}
	}

	pub fn inner(&self) -> &S {
		&self.inner
	}
}

impl<S: CdcStorage> CdcStorage for CachedCdcStorage<S> {
	fn write(&self, cdc: &Cdc) -> CdcStorageResult<()> {
		self.inner.write(cdc)?;
		self.cache.insert(cdc);
		Ok(())
	}

	fn read(&self, version: CommitVersion) -> CdcStorageResult<Option<Cdc>> {
		if let Some(cdc) = self.cache.get(version) {
			return Ok(Some((*cdc).clone()));
		}
		self.inner.read(version)
	}

	fn read_range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> CdcStorageResult<CdcBatch> {
		if let Some((lo_inc, hi_inc)) = normalize_range_inclusive(start, end)
			&& let Some((items, has_more)) = self.cache.try_serve_range(lo_inc, hi_inc, batch_size as usize)
		{
			return Ok(CdcBatch {
				items,
				has_more,
			});
		}
		self.inner.read_range(start, end, batch_size)
	}

	fn count(&self, version: CommitVersion) -> CdcStorageResult<usize> {
		self.inner.count(version)
	}

	fn min_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		self.inner.min_version()
	}

	fn max_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		self.inner.max_version()
	}

	fn drop_before(&self, version: CommitVersion, limit: usize) -> CdcStorageResult<DropBeforeResult> {
		self.inner.drop_before(version, limit)
	}

	fn vacuum(&self) -> CdcStorageResult<()> {
		self.inner.vacuum()
	}

	fn find_ttl_cutoff(&self, cutoff: DateTime) -> CdcStorageResult<Option<CommitVersion>> {
		self.inner.find_ttl_cutoff(cutoff)
	}
}

#[cfg(test)]
mod tests {
	use std::collections::Bound;

	use reifydb_core::{common::CommitVersion, interface::cdc::Cdc};
	use reifydb_value::value::datetime::DateTime;

	use super::*;
	use crate::storage::memory::MemoryCdcStorage;

	fn cv(n: u64) -> CommitVersion {
		CommitVersion(n)
	}

	fn cdc(version: u64) -> Cdc {
		Cdc::new(cv(version), DateTime::default(), Vec::new(), Vec::new())
	}

	#[test]
	fn write_is_persisted_to_inner_and_served_from_cache() {
		let cached = CachedCdcStorage::new(MemoryCdcStorage::new(), 16);
		cached.write(&cdc(1)).unwrap();
		// inner has it durably
		assert!(cached.inner().read(cv(1)).unwrap().is_some());
		// and the cache serves the read
		assert_eq!(cached.read(cv(1)).unwrap().unwrap().version, cv(1));
	}

	#[test]
	fn read_range_served_from_cache_when_covered() {
		let cached = CachedCdcStorage::new(MemoryCdcStorage::new(), 16);
		for v in 1..=5 {
			cached.write(&cdc(v)).unwrap();
		}
		let batch = cached.read_range(Bound::Excluded(cv(1)), Bound::Included(cv(4)), 100).unwrap();
		assert_eq!(batch.items.iter().map(|c| c.version).collect::<Vec<_>>(), vec![cv(2), cv(3), cv(4)]);
		assert!(!batch.has_more);
	}

	#[test]
	fn read_range_falls_back_to_inner_when_below_cache_window() {
		// Capacity 2 keeps only versions {4,5}; a request starting at 1 is not covered, so the
		// decorator must fall through to the backend, which still has the full history.
		let inner = MemoryCdcStorage::new();
		let cached = CachedCdcStorage::new(inner, 2);
		for v in 1..=5 {
			cached.write(&cdc(v)).unwrap();
		}
		let batch = cached.read_range(Bound::Included(cv(1)), Bound::Included(cv(5)), 100).unwrap();
		assert_eq!(batch.items.len(), 5, "fallback must serve the full range from the backend");
	}
}
