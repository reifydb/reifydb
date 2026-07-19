// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

const MVCC_VERSION_SIZE: usize = 10;

use std::{
	collections::{HashMap, HashSet},
	ops::AddAssign,
};

use reifydb_codec::encoded::row::EncodedRow;
use reifydb_core::interface::store::{SingleVersionStore, Tier};
use reifydb_value::{Result, util::cowvec::CowVec};

use crate::{
	MetricsId,
	storage::{
		encoding::{
			decode_storage_stats, decode_storage_stats_key, encode_storage_stats, encode_storage_stats_key,
			storage_stats_key_prefix,
		},
		parser::parse_id,
	},
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MultiStorageMetrics {
	pub current_key_bytes: u64,

	pub current_value_bytes: u64,

	pub historical_key_bytes: u64,

	pub historical_value_bytes: u64,

	pub current_count: u64,

	pub historical_count: u64,
}

impl MultiStorageMetrics {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn total_bytes(&self) -> u64 {
		self.current_key_bytes
			+ self.current_value_bytes
			+ self.historical_key_bytes
			+ self.historical_value_bytes
	}

	pub fn current_bytes(&self) -> u64 {
		self.current_key_bytes + self.current_value_bytes
	}

	pub fn historical_bytes(&self) -> u64 {
		self.historical_key_bytes + self.historical_value_bytes
	}

	pub fn total_count(&self) -> u64 {
		self.current_count + self.historical_count
	}

	pub fn record_insert(&mut self, key_bytes: u64, value_bytes: u64) {
		self.current_key_bytes += key_bytes;
		self.current_value_bytes += value_bytes;
		self.current_count += 1;
	}

	pub fn record_update(
		&mut self,
		post_key_bytes: u64,
		post_value_bytes: u64,
		pre_key_bytes: u64,
		pre_value_bytes: u64,
	) {
		self.current_key_bytes = self.current_key_bytes.saturating_sub(pre_key_bytes);
		self.current_value_bytes = self.current_value_bytes.saturating_sub(pre_value_bytes);
		self.current_count = self.current_count.saturating_sub(1);

		self.historical_key_bytes += pre_key_bytes;
		self.historical_value_bytes += pre_value_bytes;
		self.historical_count += 1;

		self.current_key_bytes += post_key_bytes;
		self.current_value_bytes += post_value_bytes;
		self.current_count += 1;
	}

	pub fn record_delete(&mut self, tombstone_key_bytes: u64, pre_key_bytes: u64, pre_value_bytes: u64) {
		self.current_key_bytes = self.current_key_bytes.saturating_sub(pre_key_bytes);
		self.current_value_bytes = self.current_value_bytes.saturating_sub(pre_value_bytes);
		self.current_count = self.current_count.saturating_sub(1);

		self.historical_key_bytes += pre_key_bytes;
		self.historical_value_bytes += pre_value_bytes;
		self.historical_count += 1;

		self.historical_key_bytes += tombstone_key_bytes;
		self.historical_count += 1;
	}

	pub fn record_drop(&mut self, key_bytes: u64, value_bytes: u64) {
		self.historical_key_bytes = self.historical_key_bytes.saturating_sub(key_bytes);
		self.historical_value_bytes = self.historical_value_bytes.saturating_sub(value_bytes);
		self.historical_count = self.historical_count.saturating_sub(1);
	}
}

impl AddAssign for MultiStorageMetrics {
	fn add_assign(&mut self, rhs: Self) {
		self.current_key_bytes += rhs.current_key_bytes;
		self.current_value_bytes += rhs.current_value_bytes;
		self.historical_key_bytes += rhs.historical_key_bytes;
		self.historical_value_bytes += rhs.historical_value_bytes;
		self.current_count += rhs.current_count;
		self.historical_count += rhs.historical_count;
	}
}

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

	pub fn total_bytes(&self) -> u64 {
		self.buffer.total_bytes() + self.persistent.total_bytes()
	}

	pub fn current_bytes(&self) -> u64 {
		self.buffer.current_bytes() + self.persistent.current_bytes()
	}

	pub fn historical_bytes(&self) -> u64 {
		self.buffer.historical_bytes() + self.persistent.historical_bytes()
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
					&& let Some(s) = decode_storage_stats(item.row.as_slice())
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
				stats.historical_key_bytes += key_bytes;
				stats.historical_count += 1;
			}
		})
	}

	pub fn record_drop(&mut self, tier: Tier, key: &[u8], value_bytes: u64) -> Result<()> {
		let id = parse_id(key);

		let key_bytes = (key.len() + MVCC_VERSION_SIZE) as u64;

		self.update(tier, id, |stats| {
			stats.record_drop(key_bytes, value_bytes);
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
				self.storage.set(&storage_key, EncodedRow(CowVec::new(encode_storage_stats(stats))))?;
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
		Ok(self.storage.get(&key)?.and_then(|v| decode_storage_stats(v.row.as_slice())))
	}

	pub fn scan_all(&self) -> Result<Vec<((Tier, MetricsId), MultiStorageMetrics)>> {
		let prefix = storage_stats_key_prefix();
		let batch = self.storage.prefix(&prefix)?;

		let mut results = Vec::new();
		for item in batch.items {
			if let Some((tier, id)) = decode_storage_stats_key(item.key.as_slice())
				&& let Some(stats) = decode_storage_stats(item.row.as_slice())
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
	fn test_storage_stats_insert() {
		let mut stats = MultiStorageMetrics::new();
		stats.record_insert(10, 100);

		assert_eq!(stats.current_key_bytes, 10);
		assert_eq!(stats.current_value_bytes, 100);
		assert_eq!(stats.current_count, 1);
		assert_eq!(stats.historical_key_bytes, 0);
		assert_eq!(stats.historical_count, 0);
		assert_eq!(stats.total_bytes(), 110);
	}

	#[test]
	fn test_storage_stats_update() {
		let mut stats = MultiStorageMetrics::new();
		stats.record_insert(10, 100);
		stats.record_update(10, 150, 10, 100);

		// Current should have new value
		assert_eq!(stats.current_key_bytes, 10);
		assert_eq!(stats.current_value_bytes, 150);
		assert_eq!(stats.current_count, 1);

		// Historical should have old value
		assert_eq!(stats.historical_key_bytes, 10);
		assert_eq!(stats.historical_value_bytes, 100);
		assert_eq!(stats.historical_count, 1);

		assert_eq!(stats.total_bytes(), 270); // 10+150 + 10+100
	}

	#[test]
	fn test_storage_stats_delete() {
		let mut stats = MultiStorageMetrics::new();
		stats.record_insert(10, 100);
		stats.record_delete(10, 10, 100);

		// Current should be empty
		assert_eq!(stats.current_key_bytes, 0);
		assert_eq!(stats.current_value_bytes, 0);
		assert_eq!(stats.current_count, 0);

		// Historical should have old value + tombstone key
		assert_eq!(stats.historical_key_bytes, 20); // old key + tombstone key
		assert_eq!(stats.historical_value_bytes, 100);
		assert_eq!(stats.historical_count, 2); // old entry + tombstone
	}

	#[test]
	fn test_tier_stats() {
		let mut tier_stats = TieredStorageMetrics::new();
		tier_stats.get_mut(Tier::Buffer).record_insert(10, 100);
		tier_stats.get_mut(Tier::Persistent).record_insert(20, 200);

		assert_eq!(tier_stats.buffer.total_bytes(), 110);
		assert_eq!(tier_stats.persistent.total_bytes(), 220);
		assert_eq!(tier_stats.total_bytes(), 330);
	}
}
