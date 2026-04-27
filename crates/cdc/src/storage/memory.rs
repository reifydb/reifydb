// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{BTreeMap, Bound},
	sync::Arc,
};

use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, CdcBatch},
};
use reifydb_runtime::sync::rwlock::RwLock;

use super::{CdcStorage, CdcStorageResult, DropBeforeResult, DroppedCdcEntry};

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
		let lo_inc: CommitVersion = match start {
			Bound::Included(v) => v,
			Bound::Excluded(v) => CommitVersion(v.0.saturating_add(1)),
			Bound::Unbounded => CommitVersion(0),
		};
		let hi_inc: CommitVersion = match end {
			Bound::Included(v) => v,
			Bound::Excluded(v) => CommitVersion(v.0.saturating_sub(1)),
			Bound::Unbounded => CommitVersion(u64::MAX),
		};
		if lo_inc > hi_inc {
			return Ok(CdcBatch {
				items: Vec::new(),
				has_more: false,
			});
		}

		let guard = self.inner.read();
		let batch_size = batch_size as usize;
		let range_iter = guard.range(lo_inc..=hi_inc);
		let mut items: Vec<Cdc> = Vec::with_capacity(batch_size.min(64));

		for (count, (_, cdc)) in range_iter.enumerate() {
			if count >= batch_size {
				// We've hit the batch limit, there are more items
				return Ok(CdcBatch {
					items,
					has_more: true,
				});
			}
			items.push(cdc.clone());
		}

		Ok(CdcBatch {
			items,
			has_more: false,
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

	fn drop_before(&self, version: CommitVersion) -> CdcStorageResult<DropBeforeResult> {
		let mut guard = self.inner.write();
		let keys_to_remove: Vec<_> = guard.range(..version).map(|(k, _)| *k).collect();
		let count = keys_to_remove.len();

		let mut entries = Vec::new();
		for key in &keys_to_remove {
			if let Some(cdc) = guard.get(key) {
				for sys_change in &cdc.system_changes {
					entries.push(DroppedCdcEntry {
						key: sys_change.key().clone(),
						value_bytes: sys_change.value_bytes() as u64,
					});
				}
			}
		}

		for key in keys_to_remove {
			guard.remove(&key);
		}

		Ok(DropBeforeResult {
			count,
			entries,
		})
	}
}
