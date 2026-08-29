// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	ops::AddAssign,
};

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::interface::{
	catalog::metrics::{
		MetricsId,
		parser::parse_id,
		storage::{MVCC_VERSION_SIZE, MultiStorageMetrics},
	},
	store::{SingleVersionStore, Tier},
};
use reifydb_value::{Result, util::cowvec::CowVec};

use crate::metrics::storage::encoding::{
	decode_storage_stats, decode_storage_stats_key, encode_storage_stats, encode_storage_stats_key,
	storage_stats_key_prefix,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TieredStorageMetrics {
	pub buffer: MultiStorageMetrics,
	pub persistent: MultiStorageMetrics,
}

impl TieredStorageMetrics {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn get(&self, tier: Tier) -> &MultiStorageMetrics {
		match tier {
			Tier::Buffer => &self.buffer,
			Tier::Persistent => &self.persistent,
		}
	}

	pub fn get_mut(&mut self, tier: Tier) -> &mut MultiStorageMetrics {
		match tier {
			Tier::Buffer => &mut self.buffer,
			Tier::Persistent => &mut self.persistent,
		}
	}

	pub fn estimated_total_bytes(&self) -> u64 {
		self.buffer.estimated_total_bytes() + self.persistent.estimated_total_bytes()
	}

	pub fn estimated_current_bytes(&self) -> u64 {
		self.buffer.estimated_current_bytes() + self.persistent.estimated_current_bytes()
	}

	pub fn estimated_historical_bytes(&self) -> u64 {
		self.buffer.estimated_historical_bytes() + self.persistent.estimated_historical_bytes()
	}
}

impl AddAssign for TieredStorageMetrics {
	fn add_assign(&mut self, rhs: Self) {
		self.buffer += rhs.buffer;
		self.persistent += rhs.persistent;
	}
}

pub struct StorageMetricsWriter<S> {
	storage: S,
	stats: HashMap<(Tier, MetricsId), MultiStorageMetrics>,
	dirty: HashSet<(Tier, MetricsId)>,
}

impl<S: SingleVersionStore> StorageMetricsWriter<S> {
	pub fn new(storage: S) -> Self {
		let mut stats = HashMap::new();

		if let Ok(batch) = storage.prefix(&storage_stats_key_prefix()) {
			for item in batch.items {
				if let Some((tier, id)) = decode_storage_stats_key(item.key.as_slice())
					&& let Some(s) = decode_storage_stats(item.bytes.as_slice())
				{
					stats.insert((tier, id), s);
				}
			}
		}
		Self {
			storage,
			stats,
			dirty: HashSet::new(),
		}
	}

	pub fn record_write(
		&mut self,
		tier: Tier,
		key: &[u8],
		value_bytes: u64,
		pre_value_bytes: Option<u64>,
	) -> Result<()> {
		let id = parse_id(key);

		let key_bytes = (key.len() + MVCC_VERSION_SIZE) as u64;

		self.update(tier, id, |stats| {
			if let Some(pre_val) = pre_value_bytes {
				stats.record_update(key_bytes, value_bytes, key_bytes, pre_val);
			} else {
				stats.record_insert(key_bytes, value_bytes);
			}
		})
	}

	pub fn record_delete(&mut self, tier: Tier, key: &[u8], pre_value_bytes: Option<u64>) -> Result<()> {
		let id = parse_id(key);

		let key_bytes = (key.len() + MVCC_VERSION_SIZE) as u64;

		self.update(tier, id, |stats| {
			if let Some(pre_val) = pre_value_bytes {
				stats.record_delete(key_bytes, key_bytes, pre_val);
			} else {
				stats.estimated_current_key_bytes += key_bytes;
				stats.estimated_current_count += 1;
			}
		})
	}

	pub fn record_eviction(&mut self, tier: Tier, key: &[u8], value_bytes: u64, current: bool) -> Result<()> {
		let id = parse_id(key);

		let key_bytes = (key.len() + MVCC_VERSION_SIZE) as u64;

		self.update(tier, id, |stats| {
			stats.record_eviction(key_bytes, value_bytes, current);
		})
	}

	fn update<F>(&mut self, tier: Tier, id: MetricsId, f: F) -> Result<()>
	where
		F: FnOnce(&mut MultiStorageMetrics),
	{
		f(self.stats.entry((tier, id)).or_default());
		self.dirty.insert((tier, id));
		Ok(())
	}

	pub fn flush(&mut self) -> Result<()> {
		if self.dirty.is_empty() {
			return Ok(());
		}
		let dirty: Vec<(Tier, MetricsId)> = self.dirty.drain().collect();
		for (tier, id) in dirty {
			if let Some(stats) = self.stats.get(&(tier, id)) {
				let storage_key = encode_storage_stats_key(tier, id);
				self.storage
					.set(&storage_key, EncodedBytes(CowVec::new(encode_storage_stats(stats))))?;
			}
		}
		Ok(())
	}
}

#[derive(Clone)]
pub struct StorageMetricsReader<S> {
	storage: S,
}

impl<S: SingleVersionStore> StorageMetricsReader<S> {
	pub fn new(storage: S) -> Self {
		Self {
			storage,
		}
	}

	pub fn get(&self, tier: Tier, id: MetricsId) -> Result<Option<MultiStorageMetrics>> {
		let key = encode_storage_stats_key(tier, id);
		Ok(self.storage.get(&key)?.and_then(|v| decode_storage_stats(v.bytes.as_slice())))
	}

	pub fn scan_all(&self) -> Result<Vec<((Tier, MetricsId), MultiStorageMetrics)>> {
		let prefix = storage_stats_key_prefix();
		let batch = self.storage.prefix(&prefix)?;

		let mut results = Vec::new();
		for item in batch.items {
			if let Some((tier, id)) = decode_storage_stats_key(item.key.as_slice())
				&& let Some(stats) = decode_storage_stats(item.bytes.as_slice())
			{
				results.push(((tier, id), stats));
			}
		}

		Ok(results)
	}

	pub fn scan_tier(&self, tier: Tier) -> Result<Vec<(MetricsId, MultiStorageMetrics)>> {
		self.scan_all().map(|all| {
			all.into_iter()
				.filter_map(|((t, obj), stats)| {
					if t == tier {
						Some((obj, stats))
					} else {
						None
					}
				})
				.collect()
		})
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_tier_stats() {
		let mut tier_stats = TieredStorageMetrics::new();
		tier_stats.get_mut(Tier::Buffer).record_insert(10, 100);
		tier_stats.get_mut(Tier::Persistent).record_insert(20, 200);

		assert_eq!(tier_stats.buffer.estimated_total_bytes(), 110);
		assert_eq!(tier_stats.persistent.estimated_total_bytes(), 220);
		assert_eq!(tier_stats.estimated_total_bytes(), 330);
	}
}
