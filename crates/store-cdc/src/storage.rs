// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{Bound, HashMap},
	sync,
};

use reifydb_catalog::metrics::storage::parser::parse_id;
use reifydb_core::{
	common::CommitVersion,
	event::metric::CdcEviction,
	interface::{
		catalog::metrics::MetricsId,
		cdc::{Cdc, CdcBatch, CdcChange},
	},
};
use reifydb_value::{byte_size::ByteSize, count::Count, value::datetime::DateTime};

use crate::error::CdcError;

pub type CdcStorageResult<T> = Result<T, CdcError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cutoff {
	Version(CommitVersion),
	Unbounded,
}

#[derive(Debug, Clone, Default)]
pub struct DropBeforeResult {
	pub count: Count,
	pub entries: Vec<CdcEviction>,
	pub more_remaining: bool,
}

#[inline]
pub fn normalize_range_inclusive(
	start: Bound<CommitVersion>,
	end: Bound<CommitVersion>,
) -> Option<(CommitVersion, CommitVersion)> {
	let lo_inc = match start {
		Bound::Included(v) => v,
		Bound::Excluded(v) => CommitVersion(v.0.checked_add(1)?),
		Bound::Unbounded => CommitVersion(0),
	};
	let hi_inc = match end {
		Bound::Included(v) => v,
		Bound::Excluded(v) => CommitVersion(v.0.checked_sub(1)?),
		Bound::Unbounded => CommitVersion(u64::MAX),
	};
	if lo_inc > hi_inc {
		None
	} else {
		Some((lo_inc, hi_inc))
	}
}

pub fn aggregate_evictions<'a, I>(cdc_changes: I) -> Vec<CdcEviction>
where
	I: IntoIterator<Item = &'a CdcChange>,
{
	let mut by_source: HashMap<MetricsId, CdcEviction> = HashMap::new();
	for change in cdc_changes {
		let key = change.key();
		let id = parse_id(key.as_ref());
		let entry = by_source.entry(id).or_insert_with(|| CdcEviction {
			id,
			key_bytes: ByteSize::ZERO,
			value_bytes: ByteSize::ZERO,
			count: Count::ZERO,
		});
		entry.key_bytes = entry.key_bytes.saturating_add(ByteSize::from_bytes(key.as_ref().len() as u64));
		entry.value_bytes = entry.value_bytes.saturating_add(ByteSize::from_bytes(change.value_bytes() as u64));
		entry.count = entry.count.saturating_add(Count::new(1));
	}
	by_source.into_values().collect()
}

pub fn total_evicted_count(evictions: &[CdcEviction]) -> Count {
	evictions.iter().fold(Count::ZERO, |acc, e| acc.saturating_add(e.count))
}

pub fn merge_evictions(evictions: Vec<CdcEviction>) -> Vec<CdcEviction> {
	let mut by_source: HashMap<MetricsId, CdcEviction> = HashMap::new();
	for e in evictions {
		let acc = by_source.entry(e.id).or_insert_with(|| CdcEviction {
			id: e.id,
			key_bytes: ByteSize::ZERO,
			value_bytes: ByteSize::ZERO,
			count: Count::ZERO,
		});
		acc.key_bytes = acc.key_bytes.saturating_add(e.key_bytes);
		acc.value_bytes = acc.value_bytes.saturating_add(e.value_bytes);
		acc.count = acc.count.saturating_add(e.count);
	}
	by_source.into_values().collect()
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

	fn drop_before(&self, cutoff: Cutoff, limit: usize) -> CdcStorageResult<DropBeforeResult>;

	fn truncated_before(&self) -> CdcStorageResult<CommitVersion>;

	fn find_ttl_cutoff(&self, cutoff: DateTime) -> CdcStorageResult<Option<Cutoff>>;

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

	fn drop_before(&self, cutoff: Cutoff, limit: usize) -> CdcStorageResult<DropBeforeResult> {
		(**self).drop_before(cutoff, limit)
	}

	fn truncated_before(&self) -> CdcStorageResult<CommitVersion> {
		(**self).truncated_before()
	}

	fn find_ttl_cutoff(&self, cutoff: DateTime) -> CdcStorageResult<Option<Cutoff>> {
		(**self).find_ttl_cutoff(cutoff)
	}
}
