// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, Bound},
	sync::Arc,
};

use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, CdcBatch},
};
use reifydb_runtime::sync::rwlock::RwLock;

use super::{
	CdcStorage, CdcStorageResult, DropBeforeResult, aggregate_evictions, normalize_range_inclusive,
	total_evicted_count,
};

#[derive(Clone)]
pub struct MemoryCdcStorage {
	inner: Arc<RwLock<BTreeMap<CommitVersion, Cdc>>>,
}

impl MemoryCdcStorage {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(RwLock::new(BTreeMap::new())),
		}
	}

	pub fn with_entries(entries: impl IntoIterator<Item = Cdc>) -> Self {
		let map: BTreeMap<CommitVersion, Cdc> = entries.into_iter().map(|cdc| (cdc.version, cdc)).collect();
		Self {
			inner: Arc::new(RwLock::new(map)),
		}
	}

	pub fn len(&self) -> usize {
		self.inner.read().len()
	}

	pub fn is_empty(&self) -> bool {
		self.inner.read().is_empty()
	}

	pub fn clear(&self) {
		self.inner.write().clear();
	}
}

impl Default for MemoryCdcStorage {
	fn default() -> Self {
		Self::new()
	}
}

impl CdcStorage for MemoryCdcStorage {
	fn write(&self, cdc: &Cdc) -> CdcStorageResult<()> {
		self.inner.write().insert(cdc.version, cdc.clone());
		Ok(())
	}

	fn read(&self, version: CommitVersion) -> CdcStorageResult<Option<Cdc>> {
		Ok(self.inner.read().get(&version).cloned())
	}

	fn read_range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> CdcStorageResult<CdcBatch> {
		let Some((lo_inc, hi_inc)) = normalize_range_inclusive(start, end) else {
			return Ok(CdcBatch {
				items: Vec::new(),
				has_more: false,
			});
		};
		let guard = self.inner.read();
		let (items, has_more) = collect_range_into(&guard, lo_inc, hi_inc, batch_size as usize);
		Ok(CdcBatch {
			items,
			has_more,
		})
	}

	fn count(&self, version: CommitVersion) -> CdcStorageResult<usize> {
		Ok(self.inner.read().get(&version).map(|cdc| cdc.system_changes.len()).unwrap_or(0))
	}

	fn min_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		Ok(self.inner.read().keys().next().copied())
	}

	fn max_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		Ok(self.inner.read().keys().next_back().copied())
	}

	fn drop_before(&self, version: CommitVersion, limit: usize) -> CdcStorageResult<DropBeforeResult> {
		let mut guard = self.inner.write();
		let keys_to_remove: Vec<_> = guard.range(..version).take(limit).map(|(k, _)| *k).collect();
		let more_remaining = keys_to_remove.len() == limit && guard.range(..version).nth(limit).is_some();
		let entries = aggregate_evictions(
			keys_to_remove.iter().filter_map(|k| guard.get(k)).flat_map(|cdc| cdc.system_changes.iter()),
		);
		let count = total_evicted_count(&entries);
		for key in &keys_to_remove {
			guard.remove(key);
		}
		Ok(DropBeforeResult {
			count,
			entries,
			more_remaining,
		})
	}
}

#[inline]
fn collect_range_into(
	guard: &BTreeMap<CommitVersion, Cdc>,
	lo_inc: CommitVersion,
	hi_inc: CommitVersion,
	batch_size: usize,
) -> (Vec<Cdc>, bool) {
	let mut items: Vec<Cdc> = Vec::with_capacity(batch_size.min(64));
	for (count, (_, cdc)) in guard.range(lo_inc..=hi_inc).enumerate() {
		if count >= batch_size {
			return (items, true);
		}
		items.push(cdc.clone());
	}
	(items, false)
}
